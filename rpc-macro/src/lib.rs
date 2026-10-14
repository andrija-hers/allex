//! The `#[rpc_module]` attribute — **write one trait, get the whole RPC surface**.
//!
//! # The idea: the trait is the entire contract
//!
//! You declare a service as a plain Rust trait and annotate it with `#[rpc_module]`. From that one
//! trait — its method signatures and its `#[public]` / `#[authenticated]` / `#[allowed_roles(..)]`
//! attributes — the macro derives *everything* on both sides of the wire. **You never hand-write a
//! client, a request router, a role check, or (de)serialization glue.** The trait itself is the source of truth for
//! request/response types *and* access control; the macro is the only thing that reads it.
//!
//! The annotated trait is re-emitted essentially verbatim (the access-tier attributes are stripped
//! from it), so the server side is just `impl FooV1 for MyService { .. }` —
//! a normal trait impl with no framework types in the signatures. Doc comments on trait methods are
//! copied onto the generated client methods, so your API docs follow the contract to callers.
//!
//! # Method *shape* selects unary vs streaming — by return type, not an attribute
//!
//! This is the thing people miss: whether a method is a one-shot call or a progress stream is
//! decided by **what it returns**, which the macro pattern-matches. There is no `#[streaming]`
//! flag.
//!
//! ```ignore
//! #[rpc_module]
//! pub trait ArithmeticV1 {
//!     // unary, sync:   -> Result<T, E>
//!     #[public]
//!     fn add(&self, req: AddRequest) -> Result<AddResult, ArithmeticError>;
//!
//!     // unary, async:  -> impl Future<Output = Result<T, E>> + Send
//!     #[allowed_roles("user")]
//!     fn mul(&self, req: MulRequest) -> impl Future<Output = Result<MulResult, ArithmeticError>> + Send;
//!
//!     // STREAMING:     -> ProgressableFuture<Progress, Success, Error>
//!     #[allowed_roles("admin")]
//!     fn count(&self, req: CountRequest) -> ProgressableFuture<CountStep, StepsMade, ArithmeticError>;
//! }
//! ```
//!
//! Return `Result<S, E>` (sync or behind `impl Future`) for a **unary** method; return a
//! `ProgressableFuture<P, S, E>` for a **streaming** one. Every method takes
//! **exactly one request argument after `&self`** — bundle multiple parameters into a struct.
//! Violating either rule is a compile error from this macro, not a mysterious downstream one.
//!
//! # What it generates (names derived from the trait, e.g. `FooV1`)
//!
//! - **`FooV1Client<C: UnaryCodec>`** — a client *trait with a blanket impl* for any transport that
//!   satisfies the needed `transport::ClientSide*Transport` bounds. So a working client exists for
//!   free on every transport (reqwest / websocket / signalr / axum-test): you just call
//!   `client.add(req).await`. Unary calls return a `transport::ProgressOutcome<S, E,
//!   transport::Error>` (success / app-error / transport-fault — the three outcomes, kept
//!   distinct), streaming calls return a `ProgressableFuture`.
//! - **`FooV1MethodResponder<I, C>`** — the server adapter. Wrap your `impl FooV1` in
//!   `FooV1MethodResponder::<_, JsonCodec>::new(svc)` and register it in an `execution::Registry`;
//!   it implements both `execution::MethodResponder` (unary) and `StreamingMethodExecutor`.
//! - **`FooV1MethodPermissions`** — the role table: `MODULE_NAME`, `allowed_roles(method)`,
//!   `is_public(method)`, `requires_authentication(method)`, `is_authorized(method, caller_role)`.
//! - `FooV1ResponseStream` (the pinned stream alias) and two private codec helpers.
//!
//! # Gotcha: the wire module name splits the version — `FooV1` → `foo_v_1`
//!
//! The on-wire module id is the snake_case of the trait name, and the derivation inserts an
//! underscore *before a trailing version digit*. So `ArithmeticV1` is **`arithmetic_v_1`** on the
//! wire (not `arithmetic_v1`), `IdentityV1` is `identity_v_1`. You almost never type this — the
//! generated client and responder agree on it — but if you hand-build a `ModuleMethodCall`, match a
//! module string, or read `MODULE_NAME`, expect the `_v_1` form.
//!
//! # Authorization is enforced *in the generated dispatch, before the body is decoded*
//!
//! The responder calls `FooV1MethodPermissions::is_authorized(method, caller_role)` as the first
//! thing in `execute` / `execute_streaming`; a failure returns `transport::Error::AccessForbidden`
//! (→ HTTP 403) **without decoding the request**. `caller_role` arrives in the encoded wire form
//! (`handle\x1erole`); the check extracts the role segment itself. Anonymous callers (`None`) pass
//! only `#[public]` methods. This is *where* the "auth is attributes on the trait" rule in the
//! workspace `AGENTS.md` is actually implemented — you never write the check.
//!
//! # The three access tiers, and why a method must declare one
//!
//! ```ignore
//! #[public]                        // anyone, including callers with no identity
//! #[authenticated]                 // any caller whose token validated, whatever role
//! #[allowed_roles("admin", "moderator")]   // exactly these roles
//! ```
//!
//! **Declaring a tier is mandatory: a method with none is a compile error.** It used to mean
//! "reachable by nobody" — `allowed_roles` returned an empty list, so every role failed the check
//! and anonymous callers failed `is_public` — which is a safe default and an invisible one. It
//! shipped a dead method in this very workspace: `IdentityV1::logout` carried no attribute and
//! answered 403 to every caller, including the authenticated ones it was written for, because the
//! tier it needed (`#[authenticated]`) did not exist yet. A dead endpoint should not be spellable by
//! omission, so it now fails the build with a message naming the three choices.
//!
//! Declaring *two* tiers is also an error. The previous behaviour silently preferred `#[public]`,
//! i.e. resolved an ambiguous authorization declaration the most permissive way.
//!
//! ## The living example
//!
//! The `examples/arithmetic_base` crate declares the trait above; the `arithmetic_client` /
//! `arithmetic_server` / `arithmetic_tests` crates exercise the generated client and responder over
//! every transport. Read those before writing a new module.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    AngleBracketedGenericArguments, Attribute, FnArg, GenericArgument, Ident, ItemTrait, LitStr,
    PatType, PathArguments, ReturnType, Token, TraitItem, TraitItemFn, Type, TypeImplTrait,
    TypeParamBound, parse_macro_input, punctuated::Punctuated, spanned::Spanned,
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
    let requires_authentication_doc = format!(
        "Returns whether a `{trait_ident}` method is marked with `#[authenticated]` — \
         callable by any caller whose token validated, whatever role it carries."
    );
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

    let execute_streaming_arms = methods.iter().filter_map(|m| {
        m.execute_streaming_arm(&module_name, &encode_helper_ident, &permissions_ident)
    });
    let execute_arms = methods.iter().filter_map(|m| m.execute_arm(&module_name));
    let permission_arms = methods.iter().map(MethodInfo::permission_arm);
    let is_public_arms = methods.iter().map(MethodInfo::is_public_arm);
    let requires_authentication_arms = methods.iter().map(MethodInfo::requires_authentication_arm);

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
                match method {
                    #(#execute_streaming_arms)*
                    _ => None,
                }
            }
        }

        // Trait object form of the inherent `execute_streaming`, so the registry
        // can route streaming generically (see `allex-serve`).
        impl<I, C> ::execution::StreamingMethodExecutor for #responder_ident<I, C>
        where
            I: #trait_ident + Send + Sync,
            C: ::transport::UnaryCodec + Send + Sync + 'static,
        {
            fn get_name(&self) -> &str {
                #module_name
            }

            fn execute_streaming(
                &self,
                method: &str,
                payload: ::transport::Payload,
                caller_role: ::std::option::Option<&str>,
            ) -> ::std::option::Option<::execution::ResponseStream> {
                #responder_ident::<I, C>::execute_streaming(self, method, payload, caller_role)
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
                if !#permissions_ident::is_authorized(method, caller_role) {
                    ::transport::tracing::warn!(
                        rpc_module = #module_name,
                        rpc_method = method,
                        rpc_side = "server",
                        outcome = "access_forbidden",
                        caller_role = caller_role.unwrap_or("anonymous"),
                        "rpc server authorization denied"
                    );
                    return ::execution::Response::TransportError(
                        ::transport::Error::AccessForbidden,
                    );
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

            #[doc = #requires_authentication_doc]
            pub fn requires_authentication(method: &str) -> bool {
                match method {
                    #(#requires_authentication_arms)*
                    _ => false,
                }
            }

            #[doc = #is_role_allowed_doc]
            pub fn is_role_allowed(method: &str, role: &str) -> bool {
                Self::is_public(method)
                    // `#[authenticated]` accepts every role: having a validated
                    // identity IS the requirement, so the role is not consulted.
                    || Self::requires_authentication(method)
                    || Self::allowed_roles(method)
                        .is_some_and(|roles| roles.iter().any(|allowed| *allowed == role))
            }

            /// Returns `true` when `caller_role` is permitted to call `method`.
            ///
            /// Anonymous callers (`None`) are only permitted on `#[public]`
            /// methods — an `#[authenticated]` method rejects them, which is the
            /// whole point of the tier.
            pub fn is_authorized(method: &str, caller_role: ::std::option::Option<&str>) -> bool {
                match caller_role {
                    ::std::option::Option::Some(encoded) => {
                        // caller_role is encoded as 'handle RS role'; extract the role part.
                        let role = encoded.split('').nth(1).unwrap_or(encoded);
                        Self::is_role_allowed(method, role)
                    }
                    ::std::option::Option::None => Self::is_public(method),
                }
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

/// Who may call a method, as declared on the trait.
///
/// The three tiers are exhaustive on purpose: every authorization question a
/// service can ask about a caller is "anyone", "anyone we know", or "these
/// roles". `Authenticated` was missing until 2026-07, and its absence is why
/// `IdentityV1::logout` shipped with no attribute at all — the tier it needed
/// could not be spelled — which the permission table then read as *nobody*.
enum RolesSpec {
    /// `#[public]` — anyone, including callers with no identity at all.
    Public,
    /// `#[authenticated]` — any caller whose bearer token validated, whatever
    /// role it carries. The tier for "this is about you": logging out, reading
    /// your own profile, deleting your own account.
    Authenticated,
    /// `#[allowed_roles("a", "b")]` — exactly these roles. An empty list is
    /// reachable by nobody, which is what an unannotated method used to be.
    Roles(Vec<LitStr>),
}

struct MethodInfo {
    ident: Ident,
    request_ty: Type,
    kind: MethodKind,
    roles: RolesSpec,
    doc_attrs: Vec<Attribute>,
}

// The `Unary`/`Streaming` variants legitimately differ in size; this is a small compile-time-only
// enum in the proc-macro, so boxing to equalise them isn't worth it.
#[allow(clippy::large_enum_variant)]
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
        let roles = parse_roles_spec(&method.attrs).map_err(|mut e| {
            // Point a missing tier at the method signature rather than at the
            // attribute list that isn't there.
            if e.to_string().starts_with(NO_TIER_ERROR_PREFIX) {
                e = syn::Error::new_spanned(&method.sig, e.to_string());
            }
            e
        })?;
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
                    let __allex_span = ::transport::tracing::debug_span!(
                        "allex.rpc.client",
                        rpc_module = #module_name,
                        rpc_method = #method_name,
                        rpc_kind = "unary",
                        rpc_side = "client",
                    );
                    let __allex_result = {
                        let _guard = __allex_span.enter();
                        #decode_helper_ident::<C, #success_ty, #error_ty>(
                            <T as ::transport::ClientSideOneShotTransport<C, #request_ty>>::execute_module_method_call(
                                self,
                                #module_name,
                                #method_name,
                                req,
                            )
                            .await,
                        )
                    };
                    match &__allex_result {
                        ::transport::ProgressOutcome::Success(_) => {
                            ::transport::tracing::debug!(parent: &__allex_span, outcome = "success", "rpc unary client call finished");
                        }
                        ::transport::ProgressOutcome::Error(_) => {
                            ::transport::tracing::debug!(parent: &__allex_span, outcome = "app_error", "rpc unary client call finished");
                        }
                        ::transport::ProgressOutcome::Transport(::transport::TransportIssue::Interrupted) => {
                            ::transport::tracing::warn!(parent: &__allex_span, outcome = "transport_interrupted", "rpc unary client call interrupted");
                        }
                        ::transport::ProgressOutcome::Transport(::transport::TransportIssue::Error(error)) => {
                            ::transport::tracing::warn!(parent: &__allex_span, outcome = "transport_error", transport_error = ?error, "rpc unary client call failed");
                        }
                    }
                    __allex_result
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
                    let __allex_span = ::transport::tracing::debug_span!(
                        "allex.rpc.client",
                        rpc_module = #module_name,
                        rpc_method = #method_name,
                        rpc_kind = "streaming",
                        rpc_side = "client",
                    );
                    let response_stream = {
                        let _guard = __allex_span.enter();
                        <T as ::transport::ClientSideChunkedTransport<C, #request_ty>>::execute_module_method_call_stream(
                            self,
                            #module_name,
                            #method_name,
                            req,
                        )
                        .await?
                    };
                    ::transport::tracing::debug!(parent: &__allex_span, "rpc streaming client call started");

                    Ok(::progressable_future::ProgressableFuture::new_with_transport(
                        move |sender| {
                            let __allex_span = __allex_span.clone();
                            async move {
                                let _guard = __allex_span.enter();
                                let mut response_stream = response_stream;

                                // This runs in a task of its own. When whoever holds the
                                // future drops it, stop reading: otherwise the task, and
                                // the connection it reads, live as long as the server keeps
                                // the stream open, which for a watch is for ever.
                                loop {
                                    let frame = {
                                        let next = ::futures::StreamExt::next(&mut response_stream);
                                        let closed = sender.closed();
                                        ::futures::pin_mut!(next, closed);
                                        match ::futures::future::select(next, closed).await {
                                            ::futures::future::Either::Left((Some(frame), _)) => frame,
                                            ::futures::future::Either::Left((None, _)) => break,
                                            ::futures::future::Either::Right(_) => {
                                                ::transport::tracing::debug!(parent: &__allex_span, outcome = "abandoned", "rpc streaming client call abandoned by its caller");
                                                return ::transport::ProgressOutcome::Transport(
                                                    ::transport::TransportIssue::Interrupted,
                                                );
                                            }
                                        }
                                    };
                                    match frame {
                                        Err(error) => {
                                            ::transport::tracing::warn!(parent: &__allex_span, outcome = "transport_error", transport_error = ?error, "rpc streaming client call failed");
                                            return ::transport::ProgressOutcome::Transport(
                                                ::transport::TransportIssue::Error(error),
                                            );
                                        }
                                        Ok(::transport::CallResponse::Progress(payload)) => {
                                            let progress = match payload.decode::<C, #progress_ty>() {
                                                Ok(progress) => progress,
                                                Err(error) => {
                                                    ::transport::tracing::warn!(parent: &__allex_span, outcome = "transport_error", transport_error = ?error, "rpc streaming client progress decode failed");
                                                    return ::transport::ProgressOutcome::Transport(
                                                        ::transport::TransportIssue::Error(error),
                                                    );
                                                }
                                            };
                                            ::transport::tracing::trace!(parent: &__allex_span, outcome = "progress", "rpc streaming client progress");
                                            sender.progress(progress);
                                        }
                                        Ok(::transport::CallResponse::Success(payload)) => {
                                            return match payload.decode::<C, #success_ty>() {
                                                Ok(result) => {
                                                    ::transport::tracing::debug!(parent: &__allex_span, outcome = "success", "rpc streaming client call finished");
                                                    ::transport::ProgressOutcome::Success(result)
                                                }
                                                Err(error) => {
                                                    ::transport::tracing::warn!(parent: &__allex_span, outcome = "transport_error", transport_error = ?error, "rpc streaming client success decode failed");
                                                    ::transport::ProgressOutcome::Transport(
                                                        ::transport::TransportIssue::Error(error),
                                                    )
                                                }
                                            };
                                        }
                                        Ok(::transport::CallResponse::Error(payload)) => {
                                            return match payload.decode::<C, #error_ty>() {
                                                Ok(error) => {
                                                    ::transport::tracing::debug!(parent: &__allex_span, outcome = "app_error", "rpc streaming client call finished");
                                                    ::transport::ProgressOutcome::Error(error)
                                                }
                                                Err(error) => {
                                                    ::transport::tracing::warn!(parent: &__allex_span, outcome = "transport_error", transport_error = ?error, "rpc streaming client error decode failed");
                                                    ::transport::ProgressOutcome::Transport(
                                                        ::transport::TransportIssue::Error(error),
                                                    )
                                                }
                                            };
                                        }
                                    }
                                }

                                ::transport::tracing::warn!(parent: &__allex_span, outcome = "transport_interrupted", "rpc streaming client call interrupted");
                                ::transport::ProgressOutcome::Transport(
                                    ::transport::TransportIssue::Interrupted,
                                )
                            }
                        },
                    ))
                }
            },
        }
    }

    fn execute_streaming_arm(
        &self,
        module_name: &str,
        encode_helper_ident: &Ident,
        permissions_ident: &Ident,
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
                if !#permissions_ident::is_authorized(#method_name, caller_role) {
                    ::transport::tracing::warn!(
                        rpc_module = #module_name,
                        rpc_method = #method_name,
                        rpc_kind = "streaming",
                        rpc_side = "server",
                        outcome = "access_forbidden",
                        caller_role = caller_role.unwrap_or("anonymous"),
                        "rpc streaming server authorization denied"
                    );
                    return Some(Box::pin(::futures::stream::once(::std::future::ready(
                        ::transport::CallResponse::Error(
                            ::transport::Error::AccessForbidden.surely_encode::<C>(),
                        ),
                    ))));
                }
                let req = match payload.decode::<C, #request_ty>() {
                    Ok(req) => req,
                    Err(error) => {
                        ::transport::tracing::warn!(
                            rpc_module = #module_name,
                            rpc_method = #method_name,
                            rpc_kind = "streaming",
                            rpc_side = "server",
                            outcome = "decode_error",
                            transport_error = ?error,
                            "rpc streaming server request decode failed"
                        );
                        return Some(Box::pin(::futures::stream::once(async move {
                            ::transport::CallResponse::Error(error.surely_encode::<C>())
                        })));
                    }
                };
                let __allex_span = ::transport::tracing::debug_span!(
                    "allex.rpc.server",
                    rpc_module = #module_name,
                    rpc_method = #method_name,
                    rpc_kind = "streaming",
                    rpc_side = "server",
                );
                ::transport::tracing::debug!(parent: &__allex_span, "rpc streaming server call started");

                Some(Box::pin(
                    ::futures::StreamExt::map(self.inner.#method_ident(req).stream(), {
                        let __allex_span = __allex_span.clone();
                        move |step| match step {
                            ::progressable_future::Step::Progress(progress) => {
                                ::transport::tracing::trace!(parent: &__allex_span, outcome = "progress", "rpc streaming server progress");
                                ::transport::CallResponse::Progress(#encode_helper_ident::<C, _>(&progress))
                            }
                            ::progressable_future::Step::Success(result) => {
                                ::transport::tracing::debug!(parent: &__allex_span, outcome = "success", "rpc streaming server call finished");
                                ::transport::CallResponse::Success(#encode_helper_ident::<C, _>(&result))
                            }
                            ::progressable_future::Step::Error(error) => {
                                ::transport::tracing::debug!(parent: &__allex_span, outcome = "app_error", "rpc streaming server call finished");
                                ::transport::CallResponse::Error(#encode_helper_ident::<C, _>(&error))
                            }
                            ::progressable_future::Step::Transport(_) => {
                                ::transport::tracing::warn!(parent: &__allex_span, outcome = "transport_interrupted", "rpc streaming server stream interrupted");
                                ::transport::CallResponse::Error(
                                    ::transport::Error::ReceiveResponse("stream interrupted".to_string())
                                        .surely_encode::<C>(),
                                )
                            }
                        }
                    }),
                ))
            }
        })
    }

    fn execute_arm(&self, module_name: &str) -> Option<proc_macro2::TokenStream> {
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
                    Err(error) => {
                        ::transport::tracing::warn!(
                            rpc_module = #module_name,
                            rpc_method = #method_name,
                            rpc_kind = "unary",
                            rpc_side = "server",
                            outcome = "decode_error",
                            transport_error = ?error,
                            "rpc unary server request decode failed"
                        );
                        return ::execution::Response::Error(error.surely_encode::<C>());
                    }
                };
                let __allex_span = ::transport::tracing::debug_span!(
                    "allex.rpc.server",
                    rpc_module = #module_name,
                    rpc_method = #method_name,
                    rpc_kind = "unary",
                    rpc_side = "server",
                );
                let _guard = __allex_span.enter();
                ::transport::tracing::debug!("rpc unary server call started");

                match #call {
                    Ok(result) => match ::transport::Encodable::encode::<C>(&result) {
                        Ok(encoded) => {
                            ::transport::tracing::debug!(outcome = "success", "rpc unary server call finished");
                            ::execution::Response::Success(encoded)
                        }
                        Err(error) => {
                            ::transport::tracing::warn!(outcome = "encode_error", transport_error = ?error, "rpc unary server success encode failed");
                            ::execution::Response::Error(error.surely_encode::<C>())
                        }
                    },
                    Err(error) => match ::transport::Encodable::encode::<C>(&error) {
                        Ok(encoded) => {
                            ::transport::tracing::debug!(outcome = "app_error", "rpc unary server call finished");
                            ::execution::Response::Error(encoded)
                        }
                        Err(encode_error) => {
                            ::transport::tracing::warn!(outcome = "encode_error", transport_error = ?encode_error, "rpc unary server error encode failed");
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
            // Neither tier restricts by role, so neither has a role list.
            // `requires_authentication` is what tells them apart.
            RolesSpec::Public | RolesSpec::Authenticated => quote! {
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

    fn requires_authentication_arm(&self) -> proc_macro2::TokenStream {
        let method_name = self.ident.to_string();
        let needs_auth = matches!(self.roles, RolesSpec::Authenticated);
        quote! {
            #method_name => #needs_auth,
        }
    }
}

fn strip_rpc_attrs(method: &mut TraitItemFn) {
    method.attrs.retain(|attr| {
        !attr.path().is_ident("allowed_roles")
            && !attr.path().is_ident("public")
            && !attr.path().is_ident("authenticated")
    });
}

fn parse_roles_spec(attrs: &[Attribute]) -> syn::Result<RolesSpec> {
    let is_public = attrs.iter().any(|a| a.path().is_ident("public"));
    let is_authenticated = attrs.iter().any(|a| a.path().is_ident("authenticated"));
    let role_attrs: Vec<&Attribute> = attrs
        .iter()
        .filter(|a| a.path().is_ident("allowed_roles"))
        .collect();

    // Two tiers on one method is a contradiction, and the old code silently
    // preferred `#[public]` — the most permissive reading of an ambiguous
    // declaration, which is the wrong way to resolve an authorization question.
    let declared = [is_public, is_authenticated, !role_attrs.is_empty()]
        .iter()
        .filter(|present| **present)
        .count();
    if declared > 1 {
        let span = attrs
            .iter()
            .find(|a| {
                a.path().is_ident("public")
                    || a.path().is_ident("authenticated")
                    || a.path().is_ident("allowed_roles")
            })
            .map(|a| a.span())
            .unwrap_or_else(proc_macro2::Span::call_site);
        return Err(syn::Error::new(
            span,
            "an rpc_module method declares exactly one access tier: `#[public]`, \
             `#[authenticated]`, or `#[allowed_roles(..)]`",
        ));
    }

    if is_public {
        return Ok(RolesSpec::Public);
    }
    if is_authenticated {
        return Ok(RolesSpec::Authenticated);
    }

    // Declaring a tier is mandatory. Omitting one used to compile into "nobody
    // may call this", which is a safe default and a silent one: the method is
    // generated, the client can call it, and every call — authenticated or not —
    // comes back 403. `IdentityV1::logout` shipped that way and stayed broken for
    // months, because nothing about it looks wrong at the call site or in a test
    // that exercises the store directly.
    if role_attrs.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "{NO_TIER_ERROR_PREFIX}, so the generated permission table would \
                 answer 403 to every caller, authenticated ones included. Declare \
                 exactly one of: `#[public]` (anyone, including anonymous), \
                 `#[authenticated]` (any caller whose token validated, whatever \
                 role), or `#[allowed_roles(\"admin\", \"user\")]` (those roles). \
                 To say \"nobody\" on purpose, write `#[allowed_roles()]`."
            ),
        ));
    }

    let mut roles = Vec::new();
    for attr in role_attrs {
        let parsed = attr.parse_args_with(Punctuated::<LitStr, Token![,]>::parse_terminated)?;
        roles.extend(parsed);
    }

    Ok(RolesSpec::Roles(roles))
}

/// Start of the missing-tier message, used to re-span it onto the method.
const NO_TIER_ERROR_PREFIX: &str = "this rpc_module method declares no access tier";

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
            if let GenericArgument::AssocType(assoc) = arg
                && assoc.ident == "Output"
            {
                let (success_ty, error_ty) = extract_result_types(&assoc.ty)?;
                return Ok(Some((success_ty, error_ty)));
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

#[cfg(test)]
mod access_tier_tests {
    use super::{RolesSpec, expand_rpc_module, parse_roles_spec};
    use syn::{ItemTrait, TraitItem, parse_quote};

    /// Runs the real macro body over a trait, minus the `proc_macro` shim.
    fn expand(item: ItemTrait) -> Result<(), String> {
        expand_rpc_module(item)
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    fn tier_of(method: TraitItem) -> Result<RolesSpec, String> {
        match method {
            TraitItem::Fn(f) => parse_roles_spec(&f.attrs).map_err(|e| e.to_string()),
            other => panic!("expected a method, got {other:?}"),
        }
    }

    #[test]
    fn a_method_with_no_tier_is_rejected() {
        // The whole point of the hard error: this used to compile into an
        // endpoint that answered 403 to everybody, which is how
        // IdentityV1::logout stayed broken without anyone noticing.
        let err = expand(parse_quote! {
            pub trait ThingV1 {
                fn touch(&self, req: Req) -> Result<Res, Err>;
            }
        })
        .expect_err("a method with no access tier must not compile");
        assert!(
            err.contains("declares no access tier"),
            "the error should say what is missing, got: {err}"
        );
        for tier in ["#[public]", "#[authenticated]", "#[allowed_roles"] {
            assert!(
                err.contains(tier),
                "the error should name {tier} as a choice, got: {err}"
            );
        }
    }

    #[test]
    fn each_single_tier_is_accepted() {
        for attr in [
            quote::quote!(#[public]),
            quote::quote!(#[authenticated]),
            quote::quote!(#[allowed_roles("admin")]),
            // An explicitly empty list is how you say "nobody" on purpose — the
            // escape hatch the error message points at.
            quote::quote!(#[allowed_roles()]),
        ] {
            let item: ItemTrait = syn::parse_quote! {
                pub trait ThingV1 {
                    #attr
                    fn touch(&self, req: Req) -> Result<Res, Err>;
                }
            };
            expand(item).unwrap_or_else(|e| panic!("{attr} should be accepted: {e}"));
        }
    }

    #[test]
    fn two_tiers_on_one_method_are_rejected() {
        // Previously the macro silently preferred #[public] — resolving an
        // ambiguous authorization declaration the most permissive way.
        let err = expand(parse_quote! {
            pub trait ThingV1 {
                #[public]
                #[allowed_roles("admin")]
                fn touch(&self, req: Req) -> Result<Res, Err>;
            }
        })
        .expect_err("two tiers on one method must not compile");
        assert!(
            err.contains("exactly one access tier"),
            "the error should say only one tier is allowed, got: {err}"
        );
    }

    #[test]
    fn authenticated_is_neither_public_nor_role_restricted() {
        // What the generated table must say: no role list (it accepts every
        // role) and not public (anonymous callers are refused).
        let tier = tier_of(parse_quote! {
            #[authenticated]
            fn touch(&self, req: Req) -> Result<Res, Err>;
        })
        .expect("parses");
        assert!(matches!(tier, RolesSpec::Authenticated));
    }
}
