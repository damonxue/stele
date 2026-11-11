//! Minimal ERC-20 indexing application.
//!
//! The example is intentionally a complete program: it loads a TOML config,
//! declares a table with `OnchainTable`, registers a handler and runs the
//! engine. It requires a reachable Postgres database and RPC endpoint to
//! execute, so it is compiled but not run by the test suite.

use std::error::Error;

use stele::{
    Address, CellValue, Checkpoint, DecodedEvent, EventContext, HandlerError, OnchainTable, Row,
    SchemaDef, SteleBuilder, TableRef,
};

#[derive(OnchainTable)]
#[table(name = "transfer", schema = "public")]
#[allow(dead_code)]
struct TransferRow {
    #[primary_key]
    id: String,
    #[column(name = "token_address")]
    token: Address,
    #[column(name = "from_address")]
    from: Address,
    #[column(name = "to_address")]
    to: Address,
    #[column(name = "transfer_value")]
    value: String,
    #[column(name = "block_number")]
    block_number: i64,
}

const CONFIG: &str = r#"
ordering = "multichain"

[database]
url = "postgres://localhost/stele_example"

[chains.mainnet]
id = 1
rpc = ["http://127.0.0.1:8545"]

[contracts.Token]
abi = "examples/abis/erc20.json"
chain = "mainnet"
address = ["0x0000000000000000000000000000000000000001"]
start_block = 0
events = ["Transfer"]
"#;

fn transfer_id(event: &DecodedEvent) -> String {
    format!(
        "{}:{}",
        event.block.number,
        event.checkpoint.encode().unwrap_or_default()
    )
}

fn row_from_event(event: &DecodedEvent) -> Result<Row, HandlerError> {
    let args = event
        .args
        .as_object()
        .ok_or_else(|| HandlerError::User("Transfer args are not an object".to_owned()))?;
    let from = args
        .get("from")
        .and_then(|value| value.as_str())
        .ok_or_else(|| HandlerError::User("Transfer.from missing".to_owned()))?
        .parse::<Address>()
        .map_err(|error| HandlerError::User(error.to_string()))?;
    let to = args
        .get("to")
        .and_then(|value| value.as_str())
        .ok_or_else(|| HandlerError::User("Transfer.to missing".to_owned()))?
        .parse::<Address>()
        .map_err(|error| HandlerError::User(error.to_string()))?;
    let value = args
        .get("value")
        .and_then(|value| value.as_str())
        .ok_or_else(|| HandlerError::User("Transfer.value missing".to_owned()))?;
    let token = event
        .log
        .as_ref()
        .map(|log| log.log.address)
        .ok_or_else(|| HandlerError::User("Transfer has no log".to_owned()))?;

    let mut row = Row::new();
    row.insert("id".into(), CellValue::Text(transfer_id(event)));
    row.insert("token_address".into(), CellValue::Text(token.to_hex()));
    row.insert("from_address".into(), CellValue::Text(from.to_hex()));
    row.insert("to_address".into(), CellValue::Text(to.to_hex()));
    row.insert("transfer_value".into(), CellValue::Text(value.to_owned()));
    row.insert(
        "block_number".into(),
        CellValue::Int(event.block.number as i64),
    );
    Ok(row)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = stele::Config::from_toml_str(CONFIG)?;
    let schema = SchemaDef {
        tables: vec![TransferRow::table()],
        ..SchemaDef::default()
    };

    SteleBuilder::new()
        .config(config)
        .schema(schema)
        .on(
            "Token:Transfer",
            |context: &EventContext<'_>, event: &DecodedEvent| {
                Box::pin(async move {
                    let row = row_from_event(event)?;
                    let key = Row::from([("id".to_owned(), row["id"].clone())]);
                    let table = TableRef {
                        schema: "public".to_owned(),
                        name: "transfer".to_owned(),
                    };
                    context
                        .store
                        .upsert(&table, &key, &row)
                        .await
                        .map_err(|error| HandlerError::Transient(error.to_string()))?;
                    tracing::info!(
                        checkpoint = Checkpoint::block_end(
                            event.block.timestamp,
                            event.chain_id,
                            event.block.number
                        )
                        .encode()
                        .unwrap_or_default(),
                        "indexed transfer"
                    );
                    Ok(())
                })
            },
        )
        .run()
        .await?;

    Ok(())
}
