//! Integration coverage for the `OnchainTable` derive.
//!
//! The derive emits paths rooted at `stele_schema`, so this test crate depends
//! on both crates as dev-dependencies.

use stele_macros::OnchainTable;
use stele_schema::{ColumnDef, ScalarType};
use stele_types::Address;

#[derive(OnchainTable)]
#[table(name = "holder", schema = "app")]
#[allow(dead_code)]
struct Holder {
    #[primary_key]
    address: Address,
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

#[derive(OnchainTable)]
#[allow(dead_code)]
struct UserTable {
    #[primary_key]
    id: i64,
}

#[test]
fn derives_the_explicit_table_definition() {
    let def = Holder::table_def();
    assert_eq!(def.schema, "app");
    assert_eq!(def.name, "holder");
    assert_eq!(def.primary_key, vec![String::from("address")]);
    assert_eq!(
        def.columns,
        vec![
            ColumnDef::new("address", ScalarType::Text, false),
            ColumnDef::new("token_address", ScalarType::Text, false),
            ColumnDef::new("balance", ScalarType::BigInt, false),
            ColumnDef::new("count", ScalarType::Integer, false),
            ColumnDef::new("small", ScalarType::Integer, false),
            ColumnDef::new("tiny", ScalarType::Integer, false),
            ColumnDef::new("ratio", ScalarType::DoublePrecision, false),
            ColumnDef::new("approximate", ScalarType::Real, false),
            ColumnDef::new("active", ScalarType::Boolean, false),
            ColumnDef::new("label", ScalarType::Text, false),
            ColumnDef::new("data", ScalarType::Bytes, false),
            ColumnDef::new("metadata", ScalarType::Jsonb, true),
        ]
    );
    assert!(def.indexes.is_empty());
    def.validate().expect("generated definition is valid");
    assert_eq!(Holder::table(), def);
}

#[test]
fn defaults_the_table_name_and_schema() {
    let def = UserTable::table_def();
    assert_eq!(def.schema, "public");
    assert_eq!(def.name, "user_table");
    assert_eq!(def.primary_key, vec![String::from("id")]);
    assert_eq!(
        def.columns,
        vec![ColumnDef::new("id", ScalarType::BigInt, false)]
    );
    assert_eq!(UserTable::table(), def);
}
