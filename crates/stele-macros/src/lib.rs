//! Derive macros for stele.
//!
//! [`OnchainTable`] turns a struct with named fields into a
//! `stele_schema::TableDef`. The generated SQL identifier is derived from the
//! field names, so the struct keeps Rust naming conventions while the database
//! gets snake_case columns.

use proc_macro::TokenStream;

mod table;

/// Derive a PostgreSQL table definition from a struct.
///
/// # Attributes
///
/// - `#[table(name = "...", schema = "...")]` on the struct. `name` defaults to
///   the snake_case struct name and `schema` defaults to `public`.
/// - `#[primary_key]` on exactly one field.
/// - `#[column(name = "...")]` on a field to override its column name. The
///   default is the snake_case field name.
///
/// # Type mapping
///
/// | Rust type | Column type |
/// |---|---|
/// | `Address` | `TEXT` |
/// | `bool` | `BOOLEAN` |
/// | `i8`, `i16`, `i32` | `INTEGER` |
/// | `i64` | `BIGINT` |
/// | `f32` | `REAL` |
/// | `f64` | `DOUBLE PRECISION` |
/// | `String` | `TEXT` |
/// | `Vec<u8>` | `BYTEA` |
/// | `serde_json::Value` | `JSONB` |
/// | `Option<T>` | the mapping of `T`, nullable |
///
/// Other types are rejected at compile time.
///
/// # Generated API
///
/// ```ignore
/// impl UserTable {
///     pub fn table_def() -> stele_schema::TableDef { /* ... */ }
///     pub fn table() -> stele_schema::TableDef { Self::table_def() }
/// }
/// ```
#[proc_macro_derive(OnchainTable, attributes(table, primary_key, column))]
pub fn onchain_table(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    table::expand(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
