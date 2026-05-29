//! Procedural macros for defining RPC modules and generating transport-facing glue.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    AngleBracketedGenericArguments, Attribute, FnArg, GenericArgument, Ident, ItemTrait, LitStr,
    PatType, PathArguments, ReturnType, Token, TraitItem, TraitItemFn, Type, TypeImplTrait,
    TypeParamBound, parse_macro_input, punctuated::Punctuated,
};

/// Expands an RPC trait into the client, responder, and permission helpers used by this workspace.
///
/// The annotated trait remains the source of truth for request/response types and access-control
/// metadata. The macro generates:
///
/// - a transport-backed client trait,
/// - an `execution::MethodResponder` adapter for server dispatch, and
/// - a permissions helper with the module name and role metadata.
#[proc_macro_attribute]
pub fn rpc_module(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let trait_item = parse_macro_input!(item as ItemTrait);
    expand_rpc_module(trait_item)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand_rpc_module(mut trait_item: ItemTrait) -> syn::Result<proc_macro2::TokenStream> {
    let trait_ident = trait_item.ident.clone();
    let client_ident = format_ident!("{}Client", trait_ident);
    let responder_ident = format_ident!("{}MethodResponder", trait_ident);
    let response_stream_ident = format_ident!("{}ResponseStream", trait_ident);
    let permissions_ident = format_ident!("{}MethodPermissions", trait_ident);
    let decode_helper_ident = format_ident!(
        "__{}_decode_one_shot_outcome",
        module_name_from_trait(&trait_ident)
    );
    let encode_helper_ident =
        format_ident!("__{}_encode_payload", module_name_from_trait(&trait_ident));
    let module_name = module_name_from_trait(&trait_ident);
    let client_trait_doc =
        format!("Transport-backed client interface generated from [`{trait_ident}`].");
    let responder_doc = format!("Server-side responder adapter generated from [`{trait_ident}`].");
    let responder_new_doc =
        format!("Wraps an implementation of [`{trait_ident}`] for server-side dispatch.");
    let response_stream_doc = format!(
        "Pinned stream of [`transport::CallResponse`] values produced by `{trait_ident}` streaming methods."
    );
    let execute_streaming_doc =
        format!("Dispatches a streaming `{trait_ident}` method call after applying role checks.");
    let permissions_doc =
        format!("Role metadata generated from the methods declared on [`{trait_ident}`].");
    let module_name_doc = format!("Canonical module name used on the wire for [`{trait_ident}`].");
    let allowed_roles_doc = format!(
        "Returns the roles allowed to call a `{trait_ident}` method, or `None` for unknown methods."
    );
    let is_public_doc =
        format!("Returns whether a `{trait_ident}` method is marked with `#[public]`.");
    let is_role_allowed_doc =
        format!("Returns whether the supplied role may call a `{trait_ident}` method.");

    let mut methods = Vec::new();
    for item in &mut trait_item.items {
        if let TraitItem::Fn(method) = item {
            let method_info = MethodInfo::from_trait_method(method)?;
            strip_rpc_attrs(method);
            methods.push(method_info);
        }
    }

    let client_trait_methods = methods.iter().map(MethodInfo::client_trait_method);
    let client_impl_methods = methods
        .iter()
        .map(|method| method.client_impl_method(&module_name, &decode_helper_ident));

    let unary_bounds = methods
        .iter()
        .filter_map(|method| match &method.kind {
            MethodKind::Unary { .. } => {
                let request_ty = &method.request_ty;
                Some(quote! { ::transport::ClientSideOneShotTransport<C, #request_ty> })
            }
            MethodKind::Streaming { .. } => None,
        })
        .collect::<Vec<_>>();
    let streaming_bounds = methods
        .iter()
        .filter_map(|method| match &method.kind {
            MethodKind::Streaming { .. } => {
                let request_ty = &method.request_ty;
                Some(quote! { ::transport::ClientSideChunkedTransport<C, #request_ty> })
            }
            MethodKind::Unary { .. } => None,
        })
        .collect::<Vec<_>>();
    let transport_bounds = unary_bounds
        .into_iter()
        .chain(streaming_bounds)
        .collect::<Vec<_>>();

    let execute_streaming_arms = methods
        .iter()
        .filter_map(|m| m.execute_streaming_arm(&encode_helper_ident, &permissions_ident));
    let execute_arms = methods.iter().filter_map(MethodInfo::execute_arm);
    let permission_arms = methods.iter().map(MethodInfo::permission_arm);
    let is_public_arms = methods.iter().map(MethodInfo::is_public_arm);

    Ok(quote! {
        #trait_item

        #[doc = #client_trait_doc]
        pub trait #client_ident<C: ::transport::UnaryCodec> {
            #(#client_trait_methods)*
        }

        impl<T, C> #client_ident<C> for T
        where
            T: Sync + #(#transport_bounds +)*,
            C: ::transport::UnaryCodec + Send + Sync + 'static,
        {
            #(#client_impl_methods)*
        }

        #[doc = #responder_doc]
        pub struct #responder_ident<I, C> {
            inner: I,
            _codec: ::std::marker::PhantomData<C>,
        }

        impl<I, C> #responder_ident<I, C> {
            #[doc = #responder_new_doc]
            pub fn new(inner: I) -> Self {
                Self {
                    inner,
                    _codec: ::std::marker::PhantomData,
                }
            }
        }

        #[doc = #response_stream_doc]
        pub type #response_stream_ident =
            ::std::pin::Pin<Box<dyn ::futures::Stream<Item = ::transport::CallResponse> + Send>>;

        impl<I, C> #responder_ident<I, C>
        where
            I: #trait_ident + Send + Sync,
            C: ::transport::UnaryCodec + Send + Sync + 'static,
        {
            #[doc = #execute_streaming_doc]
            pub fn execute_streaming(
                &self,
                method: &str,
                payload: ::transport::Payload,
                caller_role: ::std::option::Option<&str>,
            ) -> Option<#response_stream_ident> {
                if let ::std::option::Option::Some(role) = caller_role {
                    if !#permissions_ident::is_role_allowed(method, role) {
                        return Some(Box::pin(::futures::stream::once(::std::future::ready(
                            ::transport::CallResponse::Error(
                                ::transport::Error::AccessForbidden.surely_encode::<C>(),
                            ),
                        ))));
                    }
                }
                match method {
                    #(#execute_streaming_arms)*
                    _ => None,
                }
            }
        }

        #[::async_trait::async_trait]
        impl<I, C> ::execution::MethodResponder for #responder_ident<I, C>
        where
            I: #trait_ident + Send + Sync,
            C: ::transport::UnaryCodec + Send + Sync,
        {
            fn get_name(&self) -> &str {
                #module_name
            }

            async fn execute(
                &self,
                method: &str,
                payload: ::transport::Payload,
                caller_role: ::std::option::Option<&str>,
            ) -> ::execution::Response {
                if let ::std::option::Option::Some(role) = caller_role {
                    if !#permissions_ident::is_role_allowed(method, role) {
                        return ::execution::Response::Error(
                            ::transport::Error::AccessForbidden.surely_encode::<C>(),
                        );
                    }
                }
                match method {
                    #(#execute_arms)*
                    _ => ::execution::Response::Error(
                        ::transport::Error::ModuleNotFound.surely_encode::<C>(),
                    ),
                }
            }
        }

        impl<I, C> ::execution::MethodResponderWithCodec for #responder_ident<I, C>
        where
            I: #trait_ident + Send + Sync,
            C: ::transport::UnaryCodec + Send + Sync,
        {
            type Codec = C;
        }

        #[doc = #permissions_doc]
        pub struct #permissions_ident;

        impl #permissions_ident {
            #[doc = #module_name_doc]
            pub const MODULE_NAME: &'static str = #module_name;

            #[doc = #allowed_roles_doc]
            pub fn allowed_roles(method: &str) -> Option<&'static [&'static str]> {
                match method {
                    #(#permission_arms)*
                    _ => None,
                }
            }

            #[doc = #is_public_doc]
            pub fn is_public(method: &str) -> bool {
                match method {
                    #(#is_public_arms)*
                    _ => false,
                }
            }

            #[doc = #is_role_allowed_doc]
            pub fn is_role_allowed(method: &str, role: &str) -> bool {
                Self::is_public(method)
                    || Self::allowed_roles(method)
                        .is_some_and(|roles| roles.iter().any(|allowed| *allowed == role))
            }
        }

        fn #decode_helper_ident<C, T, E>(
            result: ::transport::Result<::transport::Payload>,
        ) -> ::transport::ProgressOutcome<T, E, ::transport::Error>
        where
            C: ::transport::UnaryCodec,
            T: ::serde::de::DeserializeOwned,
            E: ::serde::de::DeserializeOwned,
        {
            match result {
                Ok(payload) => match payload.decode::<C, T>() {
                    Ok(value) => ::transport::ProgressOutcome::Success(value),
                    Err(error) => ::transport::ProgressOutcome::Transport(
                        ::transport::TransportIssue::Error(error),
                    ),
                },
                Err(::transport::Error::App(payload)) => match payload.decode::<C, E>() {
                    Ok(error) => ::transport::ProgressOutcome::Error(error),
                    Err(decode_error) => ::transport::ProgressOutcome::Transport(
                        ::transport::TransportIssue::Error(decode_error),
                    ),
                },
                Err(error) => ::transport::ProgressOutcome::Transport(
                    ::transport::TransportIssue::Error(error),
                ),
            }
        }

        fn #encode_helper_ident<C, T>(value: &T) -> ::transport::Payload
        where
            C: ::transport::UnaryCodec,
            T: ::serde::Serialize,
        {
            match ::transport::Encodable::encode::<C>(value) {
                Ok(payload) => payload,
                Err(error) => error.surely_encode::<C>(),
            }
        }
    })
}

enum RolesSpec {
    Public,
    Roles(Vec<LitStr>),
}

struct MethodInfo {
    ident: Ident,
    request_ty: Type,
    kind: MethodKind,
    roles: RolesSpec,
    doc_attrs: Vec<Attribute>,
}

enum MethodKind {
    Unary {
        success_ty: Type,
        error_ty: Type,
        is_async: bool,
    },
    Streaming {
        progress_ty: Type,
        success_ty: Type,
        error_ty: Type,
    },
}

impl MethodInfo {
    fn from_trait_method(method: &TraitItemFn) -> syn::Result<Self> {
        let request_ty = extract_request_type(method)?;
        let roles = parse_roles_spec(&method.attrs)?;
        let kind = extract_method_kind(method)?;

        Ok(Self {
            ident: method.sig.ident.clone(),
            request_ty,
            kind,
            roles,
            doc_attrs: method
                .attrs
                .iter()
                .filter(|attr| attr.path().is_ident("doc"))
                .cloned()
                .collect(),
        })
    }

    fn client_trait_method(&self) -> proc_macro2::TokenStream {
        let method_ident = &self.ident;
        let request_ty = &self.request_ty;
        let doc_attrs = &self.doc_attrs;

        match &self.kind {
            MethodKind::Unary {
                success_ty,
                error_ty,
                ..
            } => quote! {
                #(#doc_attrs)*
                fn #method_ident(
                    &self,
                    req: #request_ty,
                ) -> impl ::std::future::Future<
                    Output = ::transport::ProgressOutcome<#success_ty, #error_ty, ::transport::Error>
                > + Send;
            },
            MethodKind::Streaming {
                progress_ty,
                success_ty,
                error_ty,
            } => quote! {
                #(#doc_attrs)*
                fn #method_ident(
                    &self,
                    req: #request_ty,
                ) -> impl ::std::future::Future<
                    Output = ::transport::Result<
                        ::progressable_future::ProgressableFuture<
                            #progress_ty,
                            #success_ty,
                            #error_ty,
                            ::transport::Error,
                        >,
                    >
                > + Send;
            },
        }
    }

    fn client_impl_method(
        &self,
        module_name: &str,
        decode_helper_ident: &Ident,
    ) -> proc_macro2::TokenStream {
        let method_ident = &self.ident;
        let request_ty = &self.request_ty;
        let method_name = self.ident.to_string();

        match &self.kind {
            MethodKind::Unary {
                success_ty,
                error_ty,
                ..
            } => quote! {
                async fn #method_ident(
                    &self,
                    req: #request_ty,
                ) -> ::transport::ProgressOutcome<#success_ty, #error_ty, ::transport::Error> {
                    #decode_helper_ident::<C, #success_ty, #error_ty>(
                        <T as ::transport::ClientSideOneShotTransport<C, #request_ty>>::execute_module_method_call(
                            self,
                            #module_name,
                            #method_name,
                            req,
                        )
                        .await,
                    )
                }
            },
            MethodKind::Streaming {
                progress_ty,
                success_ty,
                error_ty,
            } => quote! {
                async fn #method_ident(
                    &self,
                    req: #request_ty,
                ) -> ::transport::Result<
                    ::progressable_future::ProgressableFuture<
                        #progress_ty,
                        #success_ty,
                        #error_ty,
                        ::transport::Error,
                    >,
                > {
                    let response_stream =
                        <T as ::transport::ClientSideChunkedTransport<C, #request_ty>>::execute_module_method_call_stream(
                            self,
                            #module_name,
                            #method_name,
                            req,
                        )
                        .await?;

                    Ok(::progressable_future::ProgressableFuture::new_with_transport(
                        move |sender| async move {
                            let mut response_stream = response_stream;

                            while let Some(frame) = ::futures::StreamExt::next(&mut response_stream).await {
                                match frame {
                                    Err(error) => {
                                        return ::transport::ProgressOutcome::Transport(
                                            ::transport::TransportIssue::Error(error),
                                        );
                                    }
                                    Ok(::transport::CallResponse::Progress(payload)) => {
                                        let progress = match payload.decode::<C, #progress_ty>() {
                                            Ok(progress) => progress,
                                            Err(error) => {
                                                return ::transport::ProgressOutcome::Transport(
                                                    ::transport::TransportIssue::Error(error),
                                                );
                                            }
                                        };
                                        sender.progress(progress);
                                    }
                                    Ok(::transport::CallResponse::Success(payload)) => {
                                        return match payload.decode::<C, #success_ty>() {
                                            Ok(result) => ::transport::ProgressOutcome::Success(result),
                                            Err(error) => ::transport::ProgressOutcome::Transport(
                                                ::transport::TransportIssue::Error(error),
                                            ),
                                        };
                                    }
                                    Ok(::transport::CallResponse::Error(payload)) => {
                                        return match payload.decode::<C, #error_ty>() {
                                            Ok(error) => ::transport::ProgressOutcome::Error(error),
                                            Err(error) => ::transport::ProgressOutcome::Transport(
                                                ::transport::TransportIssue::Error(error),
                                            ),
                                        };
                                    }
                                }
                            }

                            ::transport::ProgressOutcome::Transport(
                                ::transport::TransportIssue::Interrupted,
                            )
                        },
                    ))
                }
            },
        }
    }

    fn execute_streaming_arm(
        &self,
        encode_helper_ident: &Ident,
        _permissions_ident: &Ident,
    ) -> Option<proc_macro2::TokenStream> {
        let MethodKind::Streaming {
            progress_ty: _,
            success_ty: _,
            error_ty: _,
        } = &self.kind
        else {
            return None;
        };

        let method_ident = &self.ident;
        let request_ty = &self.request_ty;
        let method_name = self.ident.to_string();

        Some(quote! {
            #method_name => {
                let req = match payload.decode::<C, #request_ty>() {
                    Ok(req) => req,
                    Err(error) => {
                        return Some(Box::pin(::futures::stream::once(async move {
                            ::transport::CallResponse::Error(error.surely_encode::<C>())
                        })));
                    }
                };

                Some(Box::pin(
                    ::futures::StreamExt::map(self.inner.#method_ident(req).stream(), |step| match step {
                        ::progressable_future::Step::Progress(progress) => {
                            ::transport::CallResponse::Progress(#encode_helper_ident::<C, _>(&progress))
                        }
                        ::progressable_future::Step::Success(result) => {
                            ::transport::CallResponse::Success(#encode_helper_ident::<C, _>(&result))
                        }
                        ::progressable_future::Step::Error(error) => {
                            ::transport::CallResponse::Error(#encode_helper_ident::<C, _>(&error))
                        }
                        ::progressable_future::Step::Transport(_) => {
                            ::transport::CallResponse::Error(
                                ::transport::Error::ReceiveResponse("stream interrupted".to_string())
                                    .surely_encode::<C>(),
                            )
                        }
                    }),
                ))
            }
        })
    }

    fn execute_arm(&self) -> Option<proc_macro2::TokenStream> {
        let MethodKind::Unary {
            success_ty: _,
            error_ty: _,
            is_async,
        } = &self.kind
        else {
            return None;
        };

        let method_ident = &self.ident;
        let request_ty = &self.request_ty;
        let method_name = self.ident.to_string();
        let call = if *is_async {
            quote! { self.inner.#method_ident(req).await }
        } else {
            quote! { self.inner.#method_ident(req) }
        };

        Some(quote! {
            #method_name => {
                let req = match payload.decode::<C, #request_ty>() {
                    Ok(req) => req,
                    Err(error) => return ::execution::Response::Error(error.surely_encode::<C>()),
                };

                match #call {
                    Ok(result) => match ::transport::Encodable::encode::<C>(&result) {
                        Ok(encoded) => ::execution::Response::Success(encoded),
                        Err(error) => ::execution::Response::Error(error.surely_encode::<C>()),
                    },
                    Err(error) => match ::transport::Encodable::encode::<C>(&error) {
                        Ok(encoded) => ::execution::Response::Error(encoded),
                        Err(encode_error) => {
                            ::execution::Response::Error(encode_error.surely_encode::<C>())
                        }
                    },
                }
            }
        })
    }

    fn permission_arm(&self) -> proc_macro2::TokenStream {
        let method_name = self.ident.to_string();

        match &self.roles {
            RolesSpec::Public => quote! {
                #method_name => None,
            },
            RolesSpec::Roles(roles) => quote! {
                #method_name => Some(&[#(#roles),*]),
            },
        }
    }

    fn is_public_arm(&self) -> proc_macro2::TokenStream {
        let method_name = self.ident.to_string();
        let is_pub = matches!(self.roles, RolesSpec::Public);
        quote! {
            #method_name => #is_pub,
        }
    }
}

fn strip_rpc_attrs(method: &mut TraitItemFn) {
    method
        .attrs
        .retain(|attr| !attr.path().is_ident("allowed_roles") && !attr.path().is_ident("public"));
}

fn parse_roles_spec(attrs: &[Attribute]) -> syn::Result<RolesSpec> {
    let is_public = attrs.iter().any(|a| a.path().is_ident("public"));
    if is_public {
        return Ok(RolesSpec::Public);
    }

    let mut roles = Vec::new();
    for attr in attrs {
        if attr.path().is_ident("allowed_roles") {
            let parsed = attr.parse_args_with(Punctuated::<LitStr, Token![,]>::parse_terminated)?;
            roles.extend(parsed.into_iter());
        }
    }

    Ok(RolesSpec::Roles(roles))
}

fn extract_request_type(method: &TraitItemFn) -> syn::Result<Type> {
    let mut typed_inputs = method.sig.inputs.iter().filter_map(|input| match input {
        FnArg::Typed(PatType { ty, .. }) => Some((**ty).clone()),
        FnArg::Receiver(_) => None,
    });

    let request_ty = typed_inputs.next().ok_or_else(|| {
        syn::Error::new_spanned(
            &method.sig,
            "rpc_module methods must take exactly one request argument after &self",
        )
    })?;

    if typed_inputs.next().is_some() {
        return Err(syn::Error::new_spanned(
            &method.sig,
            "rpc_module methods must take exactly one request argument after &self",
        ));
    }

    Ok(request_ty)
}

fn extract_method_kind(method: &TraitItemFn) -> syn::Result<MethodKind> {
    let return_ty = match &method.sig.output {
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &method.sig,
                "rpc_module methods must declare a return type",
            ));
        }
        ReturnType::Type(_, ty) => ty.as_ref(),
    };

    if method.sig.asyncness.is_some() {
        let (success_ty, error_ty) = extract_result_types(return_ty)?;
        return Ok(MethodKind::Unary {
            success_ty,
            error_ty,
            is_async: true,
        });
    }

    if let Some((progress_ty, success_ty, error_ty)) = extract_progressable_future_types(return_ty)?
    {
        return Ok(MethodKind::Streaming {
            progress_ty,
            success_ty,
            error_ty,
        });
    }

    if let Some((success_ty, error_ty)) = extract_future_result_types(return_ty)? {
        return Ok(MethodKind::Unary {
            success_ty,
            error_ty,
            is_async: true,
        });
    }

    let (success_ty, error_ty) = extract_result_types(return_ty)?;
    Ok(MethodKind::Unary {
        success_ty,
        error_ty,
        is_async: false,
    })
}

fn extract_progressable_future_types(ty: &Type) -> syn::Result<Option<(Type, Type, Type)>> {
    let Type::Path(type_path) = ty else {
        return Ok(None);
    };

    let Some(args) =
        angle_args_for_last_segment(type_path.path.segments.last(), "ProgressableFuture")
    else {
        return Ok(None);
    };

    let generic_types = args
        .args
        .iter()
        .filter_map(|arg| match arg {
            GenericArgument::Type(ty) => Some(ty.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    if generic_types.len() != 3 {
        return Err(syn::Error::new_spanned(
            ty,
            "ProgressableFuture return types must have exactly three type parameters",
        ));
    }

    Ok(Some((
        generic_types[0].clone(),
        generic_types[1].clone(),
        generic_types[2].clone(),
    )))
}

fn extract_future_result_types(ty: &Type) -> syn::Result<Option<(Type, Type)>> {
    let Type::ImplTrait(TypeImplTrait { bounds, .. }) = ty else {
        return Ok(None);
    };

    for bound in bounds {
        let TypeParamBound::Trait(trait_bound) = bound else {
            continue;
        };

        let Some(args) = angle_args_for_last_segment(trait_bound.path.segments.last(), "Future")
        else {
            continue;
        };

        for arg in &args.args {
            if let GenericArgument::AssocType(assoc) = arg {
                if assoc.ident == "Output" {
                    let (success_ty, error_ty) = extract_result_types(&assoc.ty)?;
                    return Ok(Some((success_ty, error_ty)));
                }
            }
        }
    }

    Ok(None)
}

fn extract_result_types(ty: &Type) -> syn::Result<(Type, Type)> {
    let Type::Path(type_path) = ty else {
        return Err(syn::Error::new_spanned(
            ty,
            "rpc_module unary methods must return Result<T, E>",
        ));
    };

    let args =
        angle_args_for_last_segment(type_path.path.segments.last(), "Result").ok_or_else(|| {
            syn::Error::new_spanned(ty, "rpc_module unary methods must return Result<T, E>")
        })?;

    let generic_types = args
        .args
        .iter()
        .filter_map(|arg| match arg {
            GenericArgument::Type(ty) => Some(ty.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    if generic_types.len() != 2 {
        return Err(syn::Error::new_spanned(
            ty,
            "Result return types must have exactly two type parameters",
        ));
    }

    Ok((generic_types[0].clone(), generic_types[1].clone()))
}

fn angle_args_for_last_segment<'a>(
    segment: Option<&'a syn::PathSegment>,
    expected_ident: &str,
) -> Option<&'a AngleBracketedGenericArguments> {
    let segment = segment?;
    if segment.ident != expected_ident {
        return None;
    }

    match &segment.arguments {
        PathArguments::AngleBracketed(args) => Some(args),
        PathArguments::None | PathArguments::Parenthesized(_) => None,
    }
}

fn module_name_from_trait(ident: &Ident) -> String {
    let input = ident.to_string();
    let mut output = String::new();
    let chars = input.chars().collect::<Vec<_>>();

    for (index, ch) in chars.iter().enumerate() {
        if ch.is_ascii_uppercase() {
            if index > 0 {
                let prev = chars[index - 1];
                if prev.is_ascii_lowercase()
                    || prev.is_ascii_digit()
                    || (prev.is_ascii_uppercase()
                        && chars
                            .get(index + 1)
                            .is_some_and(|next| next.is_ascii_lowercase()))
                {
                    output.push('_');
                }
            }
            output.push(ch.to_ascii_lowercase());
        } else if ch.is_ascii_digit() {
            if index > 0 && chars[index - 1].is_ascii_alphabetic() {
                output.push('_');
            }
            output.push(*ch);
        } else {
            output.push(*ch);
        }
    }

    output
}
