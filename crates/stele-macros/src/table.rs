//! Parsing and code generation for [`crate::OnchainTable`].

use std::collections::HashSet;

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Error, Fields, GenericArgument, LitStr, Meta, Path,
    PathArguments, Result, Type,
};

/// Options accepted by `#[table(...)]` on the struct.
#[derive(Default)]
struct TableOptions {
    name: Option<LitStr>,
    schema: Option<LitStr>,
}

/// Options accepted by the field attributes.
#[derive(Default)]
struct FieldOptions {
    primary_key: bool,
    column: Option<LitStr>,
}

pub(super) fn expand(input: DeriveInput) -> Result<TokenStream> {
    let struct_name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "OnchainTable does not support generic structs",
        ));
    }

    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(_) | Fields::Unit => {
                return Err(Error::new_spanned(
                    struct_name,
                    "OnchainTable requires a struct with named fields",
                ));
            }
        },
        Data::Enum(_) | Data::Union(_) => {
            return Err(Error::new_spanned(
                struct_name,
                "OnchainTable can only be derived for structs",
            ));
        }
    };

    let options = parse_table_options(&input.attrs)?;
    let default_name = to_snake_case(&struct_name.to_string());
    let (table_name, table_span) = match options.name {
        Some(literal) => (literal.value(), literal.span()),
        None => (default_name, struct_name.span()),
    };
    let (schema, schema_span) = match options.schema {
        Some(literal) => (literal.value(), literal.span()),
        None => (String::from("public"), struct_name.span()),
    };
    check_identifier(&table_name, table_span, "table name")?;
    check_identifier(&schema, schema_span, "schema name")?;

    let mut columns = Vec::with_capacity(fields.len());
    let mut column_names = HashSet::with_capacity(fields.len());
    let mut primary_key: Option<String> = None;

    for field in fields {
        let Some(ident) = &field.ident else {
            return Err(Error::new_spanned(
                field,
                "OnchainTable requires named fields",
            ));
        };
        let field_options = parse_field_options(&field.attrs)?;
        let (scalar, nullable) = map_type(&field.ty)?;
        let column_name = match &field_options.column {
            Some(literal) => literal.value(),
            None => to_snake_case(&ident.to_string()),
        };
        check_identifier(&column_name, ident.span(), "column name")?;
        if !column_names.insert(column_name.clone()) {
            return Err(Error::new(
                ident.span(),
                format!("duplicate column name {column_name:?}"),
            ));
        }
        if field_options.primary_key {
            if nullable {
                return Err(Error::new(
                    ident.span(),
                    "#[primary_key] cannot be applied to an Option<T> field",
                ));
            }
            if primary_key.is_some() {
                return Err(Error::new(
                    ident.span(),
                    "exactly one #[primary_key] field is allowed",
                ));
            }
            primary_key = Some(column_name.clone());
        }

        let name = LitStr::new(&column_name, ident.span());
        columns.push(quote! {
            ::stele_schema::ColumnDef::new(
                ::std::string::String::from(#name),
                #scalar,
                #nullable,
            )
        });
    }

    let Some(primary_key) = primary_key else {
        return Err(Error::new_spanned(
            struct_name,
            "OnchainTable requires exactly one #[primary_key] field",
        ));
    };

    let schema = LitStr::new(&schema, schema_span);
    let table_name = LitStr::new(&table_name, table_span);
    let primary_key = LitStr::new(&primary_key, struct_name.span());

    Ok(quote! {
        impl #struct_name {
            /// Build the PostgreSQL table definition described by this struct.
            pub fn table_def() -> ::stele_schema::TableDef {
                ::stele_schema::TableDef {
                    schema: ::std::string::String::from(#schema),
                    name: ::std::string::String::from(#table_name),
                    columns: ::std::vec![#(#columns),*],
                    primary_key: ::std::vec![::std::string::String::from(#primary_key)],
                    indexes: ::std::vec::Vec::new(),
                }
            }

            /// Alias for [`Self::table_def`].
            pub fn table() -> ::stele_schema::TableDef {
                Self::table_def()
            }
        }
    })
}

fn parse_table_options(attrs: &[Attribute]) -> Result<TableOptions> {
    let mut options = TableOptions::default();
    for attr in attrs {
        if !attr.path().is_ident("table") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let target = if meta.path.is_ident("name") {
                &mut options.name
            } else if meta.path.is_ident("schema") {
                &mut options.schema
            } else {
                return Err(meta.error("unsupported #[table] option; expected `name` or `schema`"));
            };
            let value: LitStr = meta.value()?.parse()?;
            if target.replace(value).is_some() {
                return Err(meta.error("duplicate #[table] option"));
            }
            Ok(())
        })?;
    }
    Ok(options)
}

fn parse_field_options(attrs: &[Attribute]) -> Result<FieldOptions> {
    let mut options = FieldOptions::default();
    for attr in attrs {
        if attr.path().is_ident("primary_key") {
            if !matches!(attr.meta, Meta::Path(_)) {
                return Err(Error::new_spanned(
                    attr,
                    "#[primary_key] does not take arguments",
                ));
            }
            if std::mem::replace(&mut options.primary_key, true) {
                return Err(Error::new_spanned(attr, "duplicate #[primary_key]"));
            }
        } else if attr.path().is_ident("column") {
            attr.parse_nested_meta(|meta| {
                if !meta.path.is_ident("name") {
                    return Err(meta.error("unsupported #[column] option; expected `name`"));
                }
                let value: LitStr = meta.value()?.parse()?;
                if options.column.replace(value).is_some() {
                    return Err(meta.error("duplicate #[column] option"));
                }
                Ok(())
            })?;
        }
    }
    Ok(options)
}

/// Map a field type to a `stele_schema::ScalarType` expression and its
/// nullability. `Option<T>` is transparent apart from setting nullability.
fn map_type(ty: &Type) -> Result<(TokenStream, bool)> {
    let Type::Path(type_path) = ty else {
        return Err(unsupported_type(ty));
    };
    if type_path.qself.is_some() {
        return Err(unsupported_type(ty));
    }
    let path = &type_path.path;
    let Some(segment) = path.segments.last() else {
        return Err(unsupported_type(ty));
    };

    if segment.ident == "Option" {
        let inner = single_type_argument(&segment.arguments)
            .ok_or_else(|| Error::new_spanned(ty, "Option must contain exactly one type"))?;
        let (scalar, _) = map_type(inner)?;
        return Ok((scalar, true));
    }

    let scalar = map_scalar(path, ty)?;
    Ok((scalar, false))
}

fn map_scalar(path: &Path, ty: &Type) -> Result<TokenStream> {
    let Some(segment) = path.segments.last() else {
        return Err(unsupported_type(ty));
    };
    let name = segment.ident.to_string();
    let scalar = match name.as_str() {
        "bool" => quote!(::stele_schema::ScalarType::Boolean),
        "i8" | "i16" | "i32" => quote!(::stele_schema::ScalarType::Integer),
        "i64" => quote!(::stele_schema::ScalarType::BigInt),
        "f32" => quote!(::stele_schema::ScalarType::Real),
        "f64" => quote!(::stele_schema::ScalarType::DoublePrecision),
        "String" => quote!(::stele_schema::ScalarType::Text),
        "Address" => quote!(::stele_schema::ScalarType::Text),
        "Vec" => {
            let inner = single_type_argument(&segment.arguments).ok_or_else(|| {
                Error::new_spanned(ty, "Vec must contain exactly one type, e.g. Vec<u8>")
            })?;
            if last_ident(inner).as_deref() != Some("u8") {
                return Err(Error::new_spanned(
                    ty,
                    "only Vec<u8> is supported; it maps to BYTEA",
                ));
            }
            quote!(::stele_schema::ScalarType::Bytes)
        }
        "Value"
            if path.segments.len() == 1
                || (path.segments.len() == 2 && path.segments[0].ident == "serde_json") =>
        {
            quote!(::stele_schema::ScalarType::Jsonb)
        }
        _ => return Err(unsupported_type(ty)),
    };
    Ok(scalar)
}

fn single_type_argument(arguments: &PathArguments) -> Option<&Type> {
    let PathArguments::AngleBracketed(arguments) = arguments else {
        return None;
    };
    if arguments.args.len() != 1 {
        return None;
    }
    match arguments.args.first()? {
        GenericArgument::Type(ty) => Some(ty),
        _ => None,
    }
}

fn last_ident(ty: &Type) -> Option<String> {
    match ty {
        Type::Path(path) if path.qself.is_none() => path
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string()),
        _ => None,
    }
}

fn unsupported_type(ty: &Type) -> Error {
    Error::new_spanned(
        ty,
        "unsupported OnchainTable field type; expected Address, bool, i8, i16, i32, i64, f32, \
         f64, String, Vec<u8>, serde_json::Value, or Option<T> of one of these",
    )
}

fn check_identifier(value: &str, span: Span, what: &str) -> Result<()> {
    if value.is_empty() || value.len() > 63 || value.contains('\0') {
        return Err(Error::new(
            span,
            format!(
                "{what} must be a non-empty PostgreSQL identifier of at most 63 bytes without NUL"
            ),
        ));
    }
    Ok(())
}

/// Convert a Rust identifier to snake_case for the default SQL identifier.
fn to_snake_case(value: &str) -> String {
    let characters: Vec<char> = value.chars().collect();
    let mut result = String::with_capacity(value.len() + 4);
    for (index, character) in characters.iter().enumerate() {
        if !character.is_uppercase() {
            result.push(*character);
            continue;
        }
        let previous = index.checked_sub(1).and_then(|index| characters.get(index));
        let next = characters.get(index + 1);
        let boundary = previous.is_some_and(|previous| {
            previous.is_lowercase() || previous.is_numeric() || previous == &'_'
        }) || (previous.is_some_and(|previous| previous.is_uppercase())
            && next.is_some_and(|next| next.is_lowercase()));
        if boundary {
            result.push('_');
        }
        result.extend(character.to_lowercase());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    fn render(input: DeriveInput) -> Result<String> {
        expand(input).map(|tokens| tokens.to_string())
    }

    fn compact(tokens: &str) -> String {
        tokens
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect()
    }

    #[test]
    fn converts_rust_names_to_snake_case() {
        assert_eq!(to_snake_case("UserTable"), "user_table");
        assert_eq!(to_snake_case("HTTPResponse"), "http_response");
        assert_eq!(to_snake_case("tokenAddress"), "token_address");
        assert_eq!(to_snake_case("already_snake"), "already_snake");
        assert_eq!(to_snake_case("A"), "a");
    }

    #[test]
    fn expands_all_supported_types_and_column_overrides() {
        let input: DeriveInput = parse_quote! {
            #[table(name = "holder", schema = "app")]
            struct Holder {
                #[primary_key]
                address: stele_types::Address,
                #[column(name = "token_address")]
                token: Address,
                balance: i64,
                count: i32,
                small: i16,
                tiny: i8,
                ratio: f64,
                approximate: f32,
                active: bool,
                label: String,
                data: Vec<u8>,
                metadata: Option<serde_json::Value>,
            }
        };
        let tokens = compact(&render(input).unwrap());
        for expected in [
            "String::from(\"app\")",
            "String::from(\"holder\")",
            "String::from(\"address\")",
            "String::from(\"token_address\")",
            "ScalarType::Text,false",
            "ScalarType::BigInt,false",
            "ScalarType::Integer,false",
            "ScalarType::Real,false",
            "ScalarType::DoublePrecision,false",
            "ScalarType::Boolean,false",
            "ScalarType::Bytes,false",
            "ScalarType::Jsonb,true",
            "pubfntable()",
        ] {
            assert!(
                tokens.contains(expected),
                "missing {expected:?} in {tokens}"
            );
        }
    }

    #[test]
    fn defaults_to_snake_case_name_and_public_schema() {
        let input: DeriveInput = parse_quote! {
            struct UserTable {
                #[primary_key]
                id: i64,
            }
        };
        let tokens = compact(&render(input).unwrap());
        assert!(tokens.contains("String::from(\"public\")"), "{tokens}");
        assert!(tokens.contains("String::from(\"user_table\")"), "{tokens}");
        assert!(tokens.contains("String::from(\"id\")"), "{tokens}");
        assert!(
            tokens.contains("primary_key:::std::vec![::std::string::String::from(\"id\")]"),
            "{tokens}"
        );
    }

    #[test]
    fn rejects_unknown_types() {
        let input: DeriveInput = parse_quote! {
            struct Bad {
                #[primary_key]
                id: i64,
                other: u64,
            }
        };
        let error = expand(input).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unsupported OnchainTable field type"),
            "{error}"
        );
    }

    #[test]
    fn rejects_non_byte_vectors() {
        let input: DeriveInput = parse_quote! {
            struct Bad {
                #[primary_key]
                id: i64,
                values: Vec<i32>,
            }
        };
        let error = expand(input).unwrap_err();
        assert!(error.to_string().contains("only Vec<u8>"), "{error}");
    }

    #[test]
    fn requires_exactly_one_primary_key() {
        let missing: DeriveInput = parse_quote! {
            struct NoKey {
                id: i64,
            }
        };
        assert!(
            expand(missing)
                .unwrap_err()
                .to_string()
                .contains("exactly one #[primary_key]")
        );

        let repeated: DeriveInput = parse_quote! {
            struct TwoKeys {
                #[primary_key]
                a: i64,
                #[primary_key]
                b: i64,
            }
        };
        assert!(
            expand(repeated)
                .unwrap_err()
                .to_string()
                .contains("exactly one #[primary_key]")
        );

        let optional: DeriveInput = parse_quote! {
            struct OptionalKey {
                #[primary_key]
                id: Option<i64>,
            }
        };
        assert!(
            expand(optional)
                .unwrap_err()
                .to_string()
                .contains("cannot be applied to an Option")
        );
    }

    #[test]
    fn rejects_unsupported_table_options() {
        let input: DeriveInput = parse_quote! {
            #[table(index = "nope")]
            struct Bad {
                #[primary_key]
                id: i64,
            }
        };
        let error = expand(input).unwrap_err();
        assert!(
            error.to_string().contains("unsupported #[table] option"),
            "{error}"
        );
    }

    #[test]
    fn rejects_generic_and_tuple_structs() {
        let generic: DeriveInput = parse_quote! {
            struct Generic<T> {
                #[primary_key]
                id: T,
            }
        };
        assert!(
            expand(generic)
                .unwrap_err()
                .to_string()
                .contains("generic structs")
        );

        let tuple: DeriveInput = parse_quote! {
            struct Tuple(#[primary_key] i64);
        };
        assert!(
            expand(tuple)
                .unwrap_err()
                .to_string()
                .contains("named fields")
        );
    }

    #[test]
    fn rejects_invalid_identifiers() {
        let input: DeriveInput = parse_quote! {
            #[table(name = "")]
            struct Bad {
                #[primary_key]
                id: i64,
            }
        };
        assert!(
            expand(input)
                .unwrap_err()
                .to_string()
                .contains("non-empty PostgreSQL identifier")
        );
    }
}
