use stele_schema::{
    ColumnDef, ColumnType, EnumDef, IndexDef, ScalarType, SchemaDef, SchemaError, TableDef,
    ViewDef, quote_identifier,
};

fn table() -> TableDef {
    TableDef {
        schema: "public".into(),
        name: "holder".into(),
        columns: vec![
            ColumnDef::new("address", ScalarType::Text, false),
            ColumnDef::new("balance", ScalarType::EVM_INTEGER, false),
        ],
        primary_key: vec!["address".into()],
        ..Default::default()
    }
}

#[test]
fn renders_all_supported_types_and_composite_key() {
    let table = TableDef {
        schema: "app".into(),
        name: "values".into(),
        columns: vec![
            ColumnDef::new("text", ScalarType::Text, false),
            ColumnDef::new("int", ScalarType::Integer, false),
            ColumnDef::new("big", ScalarType::BigInt, true),
            ColumnDef::new("bool", ScalarType::Boolean, true),
            ColumnDef::new("float", ScalarType::DoublePrecision, true),
            ColumnDef::new("json", ScalarType::Jsonb, true),
            ColumnDef::new("amount", ScalarType::EVM_INTEGER, true),
            ColumnDef::new(
                "ratio",
                ScalarType::Numeric {
                    precision: 38,
                    scale: 18,
                },
                true,
            ),
            ColumnDef::new("list", ColumnType::Array(ScalarType::Text), true),
        ],
        primary_key: vec!["text".into(), "int".into()],
        ..Default::default()
    };
    assert_eq!(
        table.create_table_sql().unwrap(),
        "CREATE TABLE \"app\".\"values\" (\n  \"text\" TEXT NOT NULL,\n  \"int\" INTEGER NOT NULL,\n  \"big\" BIGINT,\n  \"bool\" BOOLEAN,\n  \"float\" DOUBLE PRECISION,\n  \"json\" JSONB,\n  \"amount\" NUMERIC(78,0),\n  \"ratio\" NUMERIC(38,18),\n  \"list\" TEXT[],\n  PRIMARY KEY (\"text\", \"int\")\n);"
    );
}

#[test]
fn quotes_injection_payloads_as_identifiers_everywhere() {
    let mut table = table();
    table.schema = "s\"; DROP SCHEMA public;--".into();
    table.name = "t\"; DROP TABLE users;--".into();
    table.columns[0].name = "c\"); DROP TABLE users;--".into();
    table.primary_key[0] = table.columns[0].name.clone();
    assert_eq!(
        table.create_table_sql().unwrap(),
        "CREATE TABLE \"s\"\"; DROP SCHEMA public;--\".\"t\"\"; DROP TABLE users;--\" (\n  \"c\"\"); DROP TABLE users;--\" TEXT NOT NULL,\n  \"balance\" NUMERIC(78,0) NOT NULL,\n  PRIMARY KEY (\"c\"\"); DROP TABLE users;--\")\n);"
    );
    assert_eq!(quote_identifier("select").unwrap(), "\"select\"");
    assert_eq!(quote_identifier("a.b").unwrap(), "\"a.b\"");
}

#[test]
fn identifiers_enforce_byte_limits_and_reject_nul() {
    assert!(quote_identifier(&"a".repeat(63)).is_ok());
    assert!(quote_identifier(&"a".repeat(64)).is_err());
    assert!(quote_identifier(&format!("{}a", "é".repeat(31))).is_ok());
    assert!(quote_identifier(&"é".repeat(32)).is_err());
    for bad in ["", "a\0b"] {
        assert_eq!(
            quote_identifier(bad),
            Err(SchemaError::InvalidIdentifier(bad.into()))
        );
    }
}

#[test]
fn numeric_precision_and_scale_boundaries_are_validated_including_arrays() {
    for (precision, scale) in [(1, 0), (1, 1), (78, 0), (1000, 0), (1000, 1000)] {
        let mut table = table();
        table.columns[1].column_type = ColumnType::Array(ScalarType::Numeric { precision, scale });
        assert!(
            table
                .create_table_sql()
                .unwrap()
                .contains(&format!("NUMERIC({precision},{scale})[]"))
        );
    }
    for (precision, scale) in [(0, 0), (1, 2), (78, 79), (1001, 0), (u16::MAX, u16::MAX)] {
        let mut table = table();
        table.columns[1].column_type = ScalarType::Numeric { precision, scale }.into();
        assert_eq!(
            table.create_table_sql(),
            Err(SchemaError::InvalidNumeric { precision, scale })
        );
    }
}

#[test]
fn evm_integer_storage_has_enough_decimal_digits_for_256_bit_extremes() {
    let max_u256 = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let max_i256 = "57896044618658097711785492504343953926634992332820282019728792003956564819967";
    let min_i256_magnitude =
        "57896044618658097711785492504343953926634992332820282019728792003956564819968";
    assert_eq!(max_u256.len(), 78);
    assert_eq!(max_i256.len(), 77);
    assert_eq!(min_i256_magnitude.len(), 77);
    assert!(
        table()
            .create_table_sql()
            .unwrap()
            .contains("NUMERIC(78,0)")
    );
}

#[test]
fn rejects_invalid_tables_and_primary_keys() {
    let mut t = table();
    t.columns.clear();
    assert_eq!(t.validate(), Err(SchemaError::NoColumns));
    let mut t = table();
    t.primary_key.clear();
    assert_eq!(t.validate(), Err(SchemaError::NoPrimaryKey));
    let mut t = table();
    t.columns.push(t.columns[0].clone());
    assert_eq!(
        t.validate(),
        Err(SchemaError::DuplicateColumn("address".into()))
    );
    let mut t = table();
    t.primary_key.push("address".into());
    assert_eq!(
        t.validate(),
        Err(SchemaError::DuplicatePrimaryKeyColumn("address".into()))
    );
    let mut t = table();
    t.primary_key = vec!["missing".into()];
    assert_eq!(
        t.validate(),
        Err(SchemaError::UnknownPrimaryKeyColumn("missing".into()))
    );
    let mut t = table();
    t.columns[0].nullable = true;
    assert_eq!(
        t.validate(),
        Err(SchemaError::NullablePrimaryKeyColumn("address".into()))
    );
}

#[test]
fn collection_validates_all_tables_and_qualified_names() {
    let a = table();
    let mut b = a.clone();
    b.schema = "other".into();
    let mut schema = SchemaDef {
        tables: vec![a.clone(), b],
        ..Default::default()
    };
    assert_eq!(schema.create_tables_sql().unwrap().len(), 2);
    schema.tables.push(a);
    assert_eq!(
        schema.create_tables_sql(),
        Err(SchemaError::DuplicateTable {
            schema: "public".into(),
            name: "holder".into()
        })
    );
    schema.tables.pop();
    schema.tables[1].primary_key.clear();
    assert_eq!(schema.create_tables_sql(), Err(SchemaError::NoPrimaryKey));
}

#[test]
fn renders_extended_scalar_types() {
    let table = TableDef {
        schema: "app".into(),
        name: "types".into(),
        columns: vec![
            ColumnDef::new("small", ScalarType::SmallInt, false),
            ColumnDef::new("ratio", ScalarType::Real, false),
            ColumnDef::new("data", ScalarType::Bytes, false),
            ColumnDef::new("seen_at", ScalarType::Timestamp, false),
        ],
        primary_key: vec!["small".into()],
        ..Default::default()
    };
    assert_eq!(
        table.create_table_sql().unwrap(),
        "CREATE TABLE \"app\".\"types\" (\n  \"small\" SMALLINT NOT NULL,\n  \"ratio\" REAL NOT NULL,\n  \"data\" BYTEA NOT NULL,\n  \"seen_at\" TIMESTAMPTZ NOT NULL,\n  PRIMARY KEY (\"small\")\n);"
    );
}

#[test]
fn renders_enum_index_and_view_ddl_in_dependency_order() {
    let mut holder = table();
    holder.indexes = vec![
        IndexDef::new("holder_balance_idx", vec!["balance".into()], false),
        IndexDef::new("holder_address_unique", vec!["address".into()], true),
    ];
    let schema = SchemaDef {
        tables: vec![holder],
        enums: vec![EnumDef::new(
            "mood",
            vec!["happy".into(), "it's fine".into()],
        )],
        views: vec![ViewDef::new(
            "holder_view",
            "SELECT address FROM public.holder;\n",
        )],
    };
    assert_eq!(
        schema.create_statements().unwrap(),
        vec![
            "CREATE TYPE \"mood\" AS ENUM ('happy', 'it''s fine');".to_string(),
            "CREATE TABLE \"public\".\"holder\" (\n  \"address\" TEXT NOT NULL,\n  \"balance\" NUMERIC(78,0) NOT NULL,\n  PRIMARY KEY (\"address\")\n);".to_string(),
            "CREATE INDEX \"holder_balance_idx\" ON \"public\".\"holder\" (\"balance\");"
                .to_string(),
            "CREATE UNIQUE INDEX \"holder_address_unique\" ON \"public\".\"holder\" (\"address\");"
                .to_string(),
            "CREATE VIEW \"holder_view\" AS SELECT address FROM public.holder;"
                .to_string(),
        ]
    );
}

#[test]
fn diff_renders_added_and_removed_objects() {
    let legacy = {
        let mut table = table();
        table.name = "legacy".into();
        table
    };
    let previous = SchemaDef {
        tables: vec![table(), legacy],
        views: vec![ViewDef::new("legacy_view", "SELECT 1")],
        ..Default::default()
    };

    let mut holder = table();
    holder
        .columns
        .push(ColumnDef::new("updated_at", ScalarType::Timestamp, false));
    holder.indexes = vec![IndexDef::new(
        "holder_balance_idx",
        vec!["balance".into()],
        false,
    )];
    let mut pool = table();
    pool.name = "pool".into();
    pool.columns = vec![ColumnDef::new("id", ScalarType::BigInt, false)];
    pool.primary_key = vec!["id".into()];
    pool.indexes = vec![IndexDef::new("pool_id_idx", vec!["id".into()], true)];
    let current = SchemaDef {
        tables: vec![holder, pool],
        enums: vec![EnumDef::new("mood", vec!["happy".into()])],
        views: vec![ViewDef::new(
            "holder_view",
            "SELECT address FROM public.holder",
        )],
    };

    assert_eq!(
        current.diff(&previous).unwrap(),
        vec![
            "DROP VIEW \"legacy_view\";".to_string(),
            "CREATE TYPE \"mood\" AS ENUM ('happy');".to_string(),
            "CREATE TABLE \"public\".\"pool\" (\n  \"id\" BIGINT NOT NULL,\n  PRIMARY KEY (\"id\")\n);"
                .to_string(),
            "CREATE UNIQUE INDEX \"pool_id_idx\" ON \"public\".\"pool\" (\"id\");"
                .to_string(),
            "ALTER TABLE \"public\".\"holder\" ADD COLUMN \"updated_at\" TIMESTAMPTZ NOT NULL;"
                .to_string(),
            "CREATE INDEX \"holder_balance_idx\" ON \"public\".\"holder\" (\"balance\");"
                .to_string(),
            "DROP TABLE \"public\".\"legacy\";".to_string(),
            "CREATE VIEW \"holder_view\" AS SELECT address FROM public.holder;"
                .to_string(),
        ]
    );
}

#[test]
fn diff_renders_type_and_nullability_changes() {
    let previous = SchemaDef {
        tables: vec![table()],
        ..Default::default()
    };

    let mut retyped = table();
    retyped.columns[1].column_type = ScalarType::BigInt.into();
    retyped.columns[1].nullable = true;
    let current = SchemaDef {
        tables: vec![retyped],
        ..Default::default()
    };
    assert_eq!(
        current.diff(&previous).unwrap(),
        vec![
            "ALTER TABLE \"public\".\"holder\" ALTER COLUMN \"balance\" TYPE BIGINT;".to_string(),
            "ALTER TABLE \"public\".\"holder\" ALTER COLUMN \"balance\" DROP NOT NULL;".to_string(),
        ]
    );

    let mut optional = table();
    optional.columns[1].nullable = true;
    let loose = SchemaDef {
        tables: vec![optional],
        ..Default::default()
    };
    assert_eq!(
        loose.diff(&previous).unwrap(),
        vec![
            "ALTER TABLE \"public\".\"holder\" ALTER COLUMN \"balance\" DROP NOT NULL;".to_string(),
        ]
    );
    assert_eq!(
        previous.diff(&loose).unwrap(),
        vec![
            "ALTER TABLE \"public\".\"holder\" ALTER COLUMN \"balance\" SET NOT NULL;".to_string(),
        ]
    );
}

#[test]
fn diff_rejects_primary_key_and_enum_changes() {
    let previous = SchemaDef {
        tables: vec![table()],
        enums: vec![EnumDef::new("mood", vec!["happy".into()])],
        ..Default::default()
    };

    let mut rekeyed = table();
    rekeyed.primary_key = vec!["address".into(), "balance".into()];
    let current = SchemaDef {
        tables: vec![rekeyed],
        enums: vec![EnumDef::new("mood", vec!["happy".into()])],
        ..Default::default()
    };
    match current.diff(&previous) {
        Err(SchemaError::UnsupportedChange(message)) => {
            assert!(message.contains("primary key"), "{message}");
        }
        other => panic!("expected an unsupported primary key change, got {other:?}"),
    }

    let changed_enum = SchemaDef {
        tables: vec![table()],
        enums: vec![EnumDef::new("mood", vec!["happy".into(), "sad".into()])],
        ..Default::default()
    };
    match changed_enum.diff(&previous) {
        Err(SchemaError::UnsupportedChange(message)) => {
            assert!(message.contains("enum"), "{message}");
        }
        other => panic!("expected an unsupported enum change, got {other:?}"),
    }
}

#[test]
fn quotes_injection_payloads_in_new_object_names_and_enum_labels() {
    let payload = "x\"; DROP TABLE users;--";
    let column = "c\"; DROP TABLE users;--";

    let mut holder = table();
    holder
        .columns
        .push(ColumnDef::new(column, ScalarType::Text, true));
    holder.indexes = vec![IndexDef::new(payload, vec![column.into()], true)];
    let schema = SchemaDef {
        tables: vec![holder],
        enums: vec![EnumDef::new(payload, vec!["' ); DROP TYPE mood;--".into()])],
        views: vec![ViewDef::new(payload, "SELECT 1")],
    };

    let statements = schema.create_statements().unwrap();
    assert_eq!(
        statements[0],
        "CREATE TYPE \"x\"\"; DROP TABLE users;--\" AS ENUM (''' ); DROP TYPE mood;--');"
    );
    assert_eq!(
        statements[2],
        "CREATE UNIQUE INDEX \"x\"\"; DROP TABLE users;--\" ON \"public\".\"holder\" (\"c\"\"; DROP TABLE users;--\");"
    );
    assert_eq!(
        statements[3],
        "CREATE VIEW \"x\"\"; DROP TABLE users;--\" AS SELECT 1;"
    );
}

#[test]
fn validates_indexes_views_and_enums() {
    let mut empty_index = table();
    empty_index.indexes = vec![IndexDef::new("idx", Vec::new(), false)];
    assert_eq!(
        empty_index.validate(),
        Err(SchemaError::NoIndexColumns("idx".into()))
    );

    let mut unknown_column = table();
    unknown_column.indexes = vec![IndexDef::new("idx", vec!["missing".into()], false)];
    assert_eq!(
        unknown_column.validate(),
        Err(SchemaError::UnknownIndexColumn {
            index: "idx".into(),
            column: "missing".into(),
        })
    );

    let mut repeated_index = table();
    repeated_index.indexes = vec![
        IndexDef::new("idx", vec!["address".into()], false),
        IndexDef::new("idx", vec!["balance".into()], false),
    ];
    assert_eq!(
        repeated_index.validate(),
        Err(SchemaError::DuplicateIndex {
            schema: "public".into(),
            name: "idx".into(),
        })
    );

    let mut first = table();
    first.indexes = vec![IndexDef::new("shared_idx", vec!["address".into()], false)];
    let mut second = table();
    second.name = "other".into();
    second.indexes = vec![IndexDef::new("shared_idx", vec!["address".into()], false)];
    let shared_index = SchemaDef {
        tables: vec![first, second],
        ..Default::default()
    };
    assert_eq!(
        shared_index.validate(),
        Err(SchemaError::DuplicateIndex {
            schema: "public".into(),
            name: "shared_idx".into(),
        })
    );

    let repeated_view = SchemaDef {
        views: vec![ViewDef::new("v", "SELECT 1"), ViewDef::new("v", "SELECT 2")],
        ..Default::default()
    };
    assert_eq!(
        repeated_view.validate(),
        Err(SchemaError::DuplicateView("v".into()))
    );

    let repeated_enum = SchemaDef {
        enums: vec![
            EnumDef::new("e", vec!["a".into()]),
            EnumDef::new("e", vec!["b".into()]),
        ],
        ..Default::default()
    };
    assert_eq!(
        repeated_enum.validate(),
        Err(SchemaError::DuplicateEnum("e".into()))
    );

    let empty_enum = SchemaDef {
        enums: vec![EnumDef::new("e", Vec::new())],
        ..Default::default()
    };
    assert_eq!(
        empty_enum.validate(),
        Err(SchemaError::NoEnumValues("e".into()))
    );

    let repeated_label = SchemaDef {
        enums: vec![EnumDef::new("e", vec!["a".into(), "a".into()])],
        ..Default::default()
    };
    assert_eq!(
        repeated_label.validate(),
        Err(SchemaError::DuplicateEnumValue {
            enum_name: "e".into(),
            value: "a".into(),
        })
    );

    for sql in ["", "   ", ";"] {
        let empty_view = SchemaDef {
            views: vec![ViewDef::new("v", sql)],
            ..Default::default()
        };
        assert_eq!(
            empty_view.validate(),
            Err(SchemaError::InvalidViewSql("v".into()))
        );
    }

    let long = "a".repeat(64);
    let mut long_index = table();
    long_index.indexes = vec![IndexDef::new(&long, vec!["address".into()], false)];
    assert_eq!(
        long_index.validate(),
        Err(SchemaError::InvalidIdentifier(long.clone()))
    );
    assert_eq!(
        SchemaDef {
            views: vec![ViewDef::new(&long, "SELECT 1")],
            ..Default::default()
        }
        .validate(),
        Err(SchemaError::InvalidIdentifier(long.clone()))
    );
    assert_eq!(
        SchemaDef {
            enums: vec![EnumDef::new(&long, vec!["a".into()])],
            ..Default::default()
        }
        .validate(),
        Err(SchemaError::InvalidIdentifier(long))
    );
}
