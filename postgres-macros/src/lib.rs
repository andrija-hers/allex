//! The `FromRow` derive — map a `tokio_postgres::Row` onto a named-field struct.
//!
//! `#[derive(FromRow)]` generates `TryFrom<tokio_postgres::Row>` for a struct with named fields,
//! pulling each field by column name via `row.try_get(..)`. Column names default to the field name;
//! override one with `#[row(rename = "col")]`. The generated code refers to `tokio_postgres`
//! **through `allex_postgres`'s re-export**, so a downstream crate only needs to depend on
//! `allex-postgres`, never on `tokio-postgres` directly. Use this via that re-export
//! (`allex_postgres::FromRow`) rather than depending on this macro crate on its own.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Lit, Meta, Token, parse_macro_input, punctuated::Punctuated};

fn column_name_for_field(field: &syn::Field) -> String {
    for attr in &field.attrs {
        if attr.path().is_ident("row") {
            let args = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated);
            if let Ok(metas) = args {
                for meta in metas {
                    if let Meta::NameValue(nv) = meta
                        && nv.path.is_ident("rename")
                        && let syn::Expr::Lit(syn::ExprLit {
                            lit: Lit::Str(s), ..
                        }) = &nv.value
                    {
                        return s.value();
                    }
                }
            }
        }
    }
    field.ident.as_ref().unwrap().to_string()
}

/// Derives `TryFrom<allex_postgres::tokio_postgres::Row>` for a named-field struct.
///
/// Field names are mapped to column names by default. Use `#[row(rename = "col")]`
/// to override for a specific field.
#[proc_macro_derive(FromRow, attributes(row))]
pub fn derive_from_row(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    let name = &input.ident;

    let fields = match input.data {
        syn::Data::Struct(syn::DataStruct {
            fields: syn::Fields::Named(ref fields),
            ..
        }) => &fields.named,
        _ => {
            return syn::Error::new_spanned(
                input,
                "FromRow only works on structs with named fields",
            )
            .to_compile_error()
            .into();
        }
    };

    let field_gets: Vec<TokenStream2> = fields
        .iter()
        .map(|f| {
            let ident = &f.ident;
            let col = column_name_for_field(f);
            quote! { #ident: row.try_get(#col)? }
        })
        .collect();

    let expanded = quote! {
        impl ::std::convert::TryFrom<::allex_postgres::tokio_postgres::Row> for #name {
            type Error = ::allex_postgres::tokio_postgres::Error;

            fn try_from(row: ::allex_postgres::tokio_postgres::Row) -> ::std::result::Result<Self, Self::Error> {
                Ok(Self {
                    #(#field_gets),*
                })
            }
        }
    };

    TokenStream::from(expanded)
}
