//! Procedural macro for deriving path-addressed state update types.
//!
//! # Two entry points
//!
//! ## `#[derive(State)]` — backward-compatible, no field hiding
//!
//! ```ignore
//! #[derive(State, Clone, Debug, PartialEq)]
//! struct Leaf {
//!     #[state]          // atomic — requires PartialEq + Clone
//!     value: i32,
//!     ignored: bool,    // not #[state] — never emits an update
//! }
//! ```
//!
//! Fields keep their names; struct literals still work.
//!
//! ## `#[state_type]` — field hiding enforced
//!
//! ```ignore
//! #[state_type]
//! #[derive(Clone, Debug, PartialEq)]
//! struct Leaf {
//!     #[state]
//!     value: i32,
//!     #[state(nested)]
//!     batch: BatchState,      // BatchState: Default
//!     #[state(vec)]
//!     items: Vec<ItemState>,
//!     ignored: bool,          // plain — required param in new()
//! }
//! ```
//!
//! State fields are renamed to `__state_<name>` (private to the module).
//! The macro generates:
//! - `Leaf::new(ignored: bool, value: i32) -> Leaf` — plain + atomic params;
//!   nested fields default via `Default::default()`; vec fields start empty.
//! - `fn value(&self) -> &i32`, `fn batch(&self) -> &BatchState`,
//!   `fn items(&self) -> &[ItemState]` — read-only getters.
//! - `LeafUpdate` enum, `impl HasUpdate for Leaf`, setter/mutator methods.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Field, Fields, Ident, Meta, Type, parse_macro_input, punctuated::Punctuated,
    token::Comma,
};

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

#[proc_macro_derive(State, attributes(state))]
pub fn derive_state(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_state(input, false)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_attribute]
pub fn state_type(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    expand_state_type(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

// ---------------------------------------------------------------------------
// Field classification
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
enum FieldKind {
    /// `#[state]` — atomic value, requires `PartialEq + Clone`
    Atomic,
    /// `#[state(nested)]` — child struct that also derives State; requires `Default`
    Nested,
    /// `#[state(vec)]` — `Vec<T>` where `T` derives State
    Vec,
}

struct StateField<'a> {
    /// Logical name as written by the user (e.g. `batches`).
    ident: &'a Ident,
    /// Name of the actual struct field.
    /// Equals `ident` for `#[derive(State)]`; `__state_batches` for `#[state_type]`.
    storage_ident: Ident,
    ty: &'a Type,
    kind: FieldKind,
}

/// All fields in declaration order, state or plain.
struct FieldInfo<'a> {
    field: &'a Field,
    kind: Option<FieldKind>, // None = plain (non-state)
}

fn inspect_fields<'a>(named: &'a Punctuated<Field, Comma>) -> syn::Result<Vec<FieldInfo<'a>>> {
    named
        .iter()
        .map(|f| {
            Ok(FieldInfo {
                field: f,
                kind: state_attr_kind(f)?,
            })
        })
        .collect()
}

fn extract_state_fields<'a>(infos: &'a [FieldInfo<'a>], use_hidden: bool) -> Vec<StateField<'a>> {
    infos
        .iter()
        .filter_map(|fi| {
            fi.kind.map(|kind| {
                let ident = fi.field.ident.as_ref().expect("named field");
                let storage_ident = if use_hidden {
                    format_ident!("__state_{}", ident)
                } else {
                    ident.clone()
                };
                StateField {
                    ident,
                    storage_ident,
                    ty: &fi.field.ty,
                    kind,
                }
            })
        })
        .collect()
}

fn state_attr_kind(field: &Field) -> syn::Result<Option<FieldKind>> {
    for attr in &field.attrs {
        if !attr.path().is_ident("state") {
            continue;
        }
        match &attr.meta {
            Meta::Path(_) => return Ok(Some(FieldKind::Atomic)),
            Meta::List(list) => {
                let ident: Ident = list.parse_args()?;
                if ident == "nested" {
                    return Ok(Some(FieldKind::Nested));
                } else if ident == "vec" {
                    return Ok(Some(FieldKind::Vec));
                } else {
                    return Err(syn::Error::new_spanned(
                        &ident,
                        "expected `nested` or `vec`",
                    ));
                }
            }
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "expected `#[state]`, `#[state(nested)]`, or `#[state(vec)]`",
                ));
            }
        }
    }
    Ok(None)
}

fn get_named_fields<'a>(
    data: &'a Data,
    ident: &'a Ident,
) -> syn::Result<&'a Punctuated<Field, Comma>> {
    match data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => Ok(&f.named),
            _ => Err(syn::Error::new_spanned(
                ident,
                "#[derive(State)] / #[state_type] only supports structs with named fields",
            )),
        },
        _ => Err(syn::Error::new_spanned(
            ident,
            "#[derive(State)] / #[state_type] only supports structs",
        )),
    }
}

// ---------------------------------------------------------------------------
// `#[derive(State)]` expansion — no field hiding
// ---------------------------------------------------------------------------

fn expand_state(input: DeriveInput, use_hidden: bool) -> syn::Result<TokenStream2> {
    let struct_ident = &input.ident;
    let update_ident = format_ident!("{}Update", struct_ident);

    let named = get_named_fields(&input.data, struct_ident)?;
    let infos = inspect_fields(named)?;
    let state_fields = extract_state_fields(&infos, use_hidden);

    let update_enum = gen_update_enum(&update_ident, &state_fields);
    let has_update_impl = gen_has_update_impl(struct_ident, &update_ident, &state_fields);
    let mutators = gen_mutators(struct_ident, &update_ident, &state_fields);

    Ok(quote! {
        #update_enum
        #has_update_impl
        #mutators
    })
}

// ---------------------------------------------------------------------------
// `#[state_type]` expansion — field hiding + new() + getters
// ---------------------------------------------------------------------------

fn expand_state_type(input: DeriveInput) -> syn::Result<TokenStream2> {
    let struct_ident = &input.ident;
    let vis = &input.vis;
    let update_ident = format_ident!("{}Update", struct_ident);
    let generics = &input.generics;

    // Collect struct-level attrs (except #[state_type] itself, which is
    // consumed and not re-emitted — it's already gone by the time we run).
    let struct_attrs = &input.attrs;

    let named = get_named_fields(&input.data, struct_ident)?;
    let infos = inspect_fields(named)?;
    let state_fields = extract_state_fields(&infos, true);

    // Rewritten struct fields
    let rewritten_fields = rewrite_struct_fields(&infos)?;

    // new() constructor
    let constructor = gen_constructor(struct_ident, vis, &infos)?;

    // Read-only getters
    let getters = gen_getters(struct_ident, &state_fields);

    // Update enum, HasUpdate impl, mutators (using hidden storage idents)
    let update_enum = gen_update_enum(&update_ident, &state_fields);
    let has_update_impl = gen_has_update_impl(struct_ident, &update_ident, &state_fields);
    let mutators = gen_mutators(struct_ident, &update_ident, &state_fields);

    Ok(quote! {
        #(#struct_attrs)*
        #vis struct #struct_ident #generics {
            #rewritten_fields
        }

        #constructor
        #getters
        #update_enum
        #has_update_impl
        #mutators
    })
}

/// Rewrite the struct's field list: rename `#[state]` fields and make them private.
fn rewrite_struct_fields(infos: &[FieldInfo<'_>]) -> syn::Result<TokenStream2> {
    let fields: Vec<TokenStream2> = infos
        .iter()
        .map(|fi| {
            let field = fi.field;
            match fi.kind {
                None => {
                    // Plain field — emit as-is
                    quote! { #field }
                }
                Some(_) => {
                    // State field — rename, make private, strip #[state] attr
                    let ident = field.ident.as_ref().expect("named field");
                    let storage = format_ident!("__state_{}", ident);
                    let ty = &field.ty;
                    let other_attrs: Vec<_> = field
                        .attrs
                        .iter()
                        .filter(|a| !a.path().is_ident("state"))
                        .collect();
                    // No visibility modifier = private to the module
                    quote! { #(#other_attrs)* #storage: #ty }
                }
            }
        })
        .collect();
    Ok(quote! { #(#fields),* })
}

/// Generate a `pub fn new(...)` constructor.
///
/// Parameters: plain fields + atomic state fields, in declaration order.
/// Nested state fields → `Default::default()`.
/// Vec state fields    → `Vec::new()`.
fn gen_constructor(
    struct_ident: &Ident,
    vis: &syn::Visibility,
    infos: &[FieldInfo<'_>],
) -> syn::Result<TokenStream2> {
    let mut params: Vec<TokenStream2> = Vec::new();
    let mut inits: Vec<TokenStream2> = Vec::new();

    for fi in infos {
        let field = fi.field;
        let ident = field.ident.as_ref().expect("named field");
        let ty = &field.ty;

        match fi.kind {
            None => {
                // Plain field: required param, same name
                params.push(quote! { #ident: #ty });
                inits.push(quote! { #ident });
            }
            Some(FieldKind::Atomic) => {
                // Atomic: required param, stored under hidden name
                let storage = format_ident!("__state_{}", ident);
                params.push(quote! { #ident: #ty });
                inits.push(quote! { #storage: #ident });
            }
            Some(FieldKind::Nested) => {
                // Nested: defaulted
                let storage = format_ident!("__state_{}", ident);
                inits.push(quote! { #storage: ::std::default::Default::default() });
            }
            Some(FieldKind::Vec) => {
                // Vec: always empty
                let storage = format_ident!("__state_{}", ident);
                inits.push(quote! { #storage: ::std::vec::Vec::new() });
            }
        }
    }

    Ok(quote! {
        impl #struct_ident {
            /// Constructs a new instance.
            ///
            /// Plain fields and atomic state fields are required parameters.
            /// Nested state fields are initialised via `Default::default()`.
            /// Vec state fields start empty.
            #vis fn new(#(#params),*) -> Self {
                Self { #(#inits),* }
            }
        }
    })
}

/// Generate read-only getters for all state fields.
fn gen_getters(struct_ident: &Ident, state_fields: &[StateField<'_>]) -> TokenStream2 {
    let methods: Vec<TokenStream2> = state_fields
        .iter()
        .map(|f| {
            let name = f.ident; // getter name = logical field name
            let storage = &f.storage_ident;
            let ty = f.ty;
            match f.kind {
                FieldKind::Atomic | FieldKind::Nested => quote! {
                    pub fn #name(&self) -> &#ty {
                        &self.#storage
                    }
                },
                FieldKind::Vec => {
                    let item_ty = vec_item_ty(ty);
                    quote! {
                        pub fn #name(&self) -> &[#item_ty] {
                            &self.#storage
                        }
                    }
                }
            }
        })
        .collect();

    quote! {
        impl #struct_ident {
            #(#methods)*
        }
    }
}

// ---------------------------------------------------------------------------
// Shared code generation: Update enum, HasUpdate, mutators
// ---------------------------------------------------------------------------

fn gen_update_enum(update_ident: &Ident, state_fields: &[StateField<'_>]) -> TokenStream2 {
    let variants: Vec<TokenStream2> = state_fields.iter().map(update_variant).collect();
    quote! {
        #[derive(Debug, Clone)]
        pub enum #update_ident {
            #(#variants),*
        }
    }
}

fn gen_has_update_impl(
    struct_ident: &Ident,
    update_ident: &Ident,
    state_fields: &[StateField<'_>],
) -> TokenStream2 {
    let arms: Vec<TokenStream2> = state_fields.iter().map(apply_arm).collect();
    quote! {
        impl ::allex_state::HasUpdate for #struct_ident {
            type Update = #update_ident;

            fn apply_update(&mut self, update: Self::Update) {
                match update {
                    #(#arms),*
                }
            }
        }
    }
}

fn gen_mutators(
    struct_ident: &Ident,
    update_ident: &Ident,
    state_fields: &[StateField<'_>],
) -> TokenStream2 {
    let methods: Vec<TokenStream2> = state_fields
        .iter()
        .map(|f| mutator_methods(f, update_ident))
        .collect();
    quote! {
        impl #struct_ident {
            #(#methods)*
        }
    }
}

// ---------------------------------------------------------------------------
// Per-field code generation (shared between derive and attribute)
// ---------------------------------------------------------------------------

fn update_variant(f: &StateField<'_>) -> TokenStream2 {
    let ty = f.ty;
    match f.kind {
        FieldKind::Atomic => {
            let variant = field_to_variant_changed(f.ident);
            quote! { #variant(#ty) }
        }
        FieldKind::Nested => {
            let variant = field_to_variant_nested(f.ident);
            let update_ty = nested_update_ty(ty);
            quote! { #variant(#update_ty) }
        }
        FieldKind::Vec => {
            let item_ty = vec_item_ty(ty);
            let item_update_ty = nested_update_ty(item_ty);
            let pushed = field_to_variant_pushed(f.ident);
            let removed = field_to_variant_removed(f.ident);
            let item_v = field_to_variant_item(f.ident);
            quote! {
                #pushed(#item_ty),
                #removed(usize),
                #item_v(usize, #item_update_ty)
            }
        }
    }
}

/// Uses `f.storage_ident` so it works for both derive (same name) and attribute (hidden name).
fn apply_arm(f: &StateField<'_>) -> TokenStream2 {
    let storage = &f.storage_ident;
    let update_path = quote! { Self::Update };
    match f.kind {
        FieldKind::Atomic => {
            let variant = field_to_variant_changed(f.ident);
            quote! {
                #update_path::#variant(v) => { self.#storage = v; }
            }
        }
        FieldKind::Nested => {
            let variant = field_to_variant_nested(f.ident);
            quote! {
                #update_path::#variant(child_update) => {
                    ::allex_state::HasUpdate::apply_update(&mut self.#storage, child_update);
                }
            }
        }
        FieldKind::Vec => {
            let pushed = field_to_variant_pushed(f.ident);
            let removed = field_to_variant_removed(f.ident);
            let item_v = field_to_variant_item(f.ident);
            quote! {
                #update_path::#pushed(item) => {
                    self.#storage.push(item);
                }
                #update_path::#removed(index) => {
                    if index < self.#storage.len() { self.#storage.remove(index); }
                }
                #update_path::#item_v(index, child_update) => {
                    if let Some(item) = self.#storage.get_mut(index) {
                        ::allex_state::HasUpdate::apply_update(item, child_update);
                    }
                }
            }
        }
    }
}

/// Uses `f.storage_ident` for field access in setter / mutator bodies.
fn mutator_methods(f: &StateField<'_>, update_enum: &Ident) -> TokenStream2 {
    let logical = f.ident; // method name suffix (always logical)
    let storage = &f.storage_ident; // actual struct field
    let ty = f.ty;

    match f.kind {
        FieldKind::Atomic => {
            let method = format_ident!("set_{}", logical);
            let variant = field_to_variant_changed(logical);
            quote! {
                pub fn #method(&mut self, value: #ty) -> ::std::option::Option<#update_enum> {
                    if self.#storage == value {
                        return ::std::option::Option::None;
                    }
                    self.#storage = value.clone();
                    ::std::option::Option::Some(#update_enum::#variant(value))
                }
            }
        }
        FieldKind::Nested => {
            let method = format_ident!("mutate_{}", logical);
            let variant = field_to_variant_nested(logical);
            let update_ty = nested_update_ty(ty);
            quote! {
                pub fn #method<F>(&mut self, f: F) -> ::std::option::Option<#update_enum>
                where
                    F: ::std::ops::FnOnce(&mut #ty) -> ::std::option::Option<#update_ty>,
                {
                    f(&mut self.#storage).map(#update_enum::#variant)
                }
            }
        }
        FieldKind::Vec => {
            let item_ty = vec_item_ty(ty);
            let item_update_ty = nested_update_ty(item_ty);
            let push_m = format_ident!("push_{}", logical);
            let remove_m = format_ident!("remove_{}", logical);
            let mutate_m = format_ident!("mutate_{}", logical);
            let pushed = field_to_variant_pushed(logical);
            let removed = field_to_variant_removed(logical);
            let item_v = field_to_variant_item(logical);
            quote! {
                pub fn #push_m(&mut self, item: #item_ty) -> #update_enum {
                    self.#storage.push(item.clone());
                    #update_enum::#pushed(item)
                }

                pub fn #remove_m(&mut self, index: usize) -> ::std::option::Option<#update_enum> {
                    if index < self.#storage.len() {
                        self.#storage.remove(index);
                        ::std::option::Option::Some(#update_enum::#removed(index))
                    } else {
                        ::std::option::Option::None
                    }
                }

                pub fn #mutate_m<F>(
                    &mut self,
                    index: usize,
                    f: F,
                ) -> ::std::option::Option<#update_enum>
                where
                    F: ::std::ops::FnOnce(&mut #item_ty) -> ::std::option::Option<#item_update_ty>,
                {
                    self.#storage.get_mut(index)
                        .and_then(|item| f(item))
                        .map(|child| #update_enum::#item_v(index, child))
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Naming helpers
// ---------------------------------------------------------------------------

fn field_to_variant_changed(ident: &Ident) -> Ident {
    format_ident!("{}Changed", capitalize(ident.to_string()))
}
fn field_to_variant_nested(ident: &Ident) -> Ident {
    format_ident!("{}", capitalize(ident.to_string()))
}
fn field_to_variant_pushed(ident: &Ident) -> Ident {
    format_ident!("{}Pushed", capitalize(ident.to_string()))
}
fn field_to_variant_removed(ident: &Ident) -> Ident {
    format_ident!("{}Removed", capitalize(ident.to_string()))
}
fn field_to_variant_item(ident: &Ident) -> Ident {
    format_ident!("{}Item", capitalize(ident.to_string()))
}

fn capitalize(s: String) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + c.as_str(),
    }
}

fn nested_update_ty(ty: &Type) -> TokenStream2 {
    quote! { <#ty as ::allex_state::HasUpdate>::Update }
}

fn vec_item_ty(ty: &Type) -> &Type {
    if let Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            if seg.ident == "Vec" {
                if let syn::PathArguments::AngleBracketed(ref args) = seg.arguments {
                    if let Some(syn::GenericArgument::Type(inner)) = args.args.first() {
                        return inner;
                    }
                }
            }
        }
    }
    panic!("#[state(vec)] field must be Vec<T>")
}
