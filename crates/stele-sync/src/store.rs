//! Postgres cache store for synced block data and interval bookkeeping.
//!
//! Every table lives in the caller-supplied schema. DDL is idempotent so
//! `initialize` can run on every process start, and every write uses an
//! `ON CONFLICT` clause so a retried or replayed sync range is harmless.
//!
//! Interval storage uses `nummultirange`, so the server must be PostgreSQL 14
//! or newer.

use std::collections::BTreeMap;

use sqlx::postgres::{PgPool, PgPoolOptions, Postgres};
use sqlx::QueryBuilder;

use stele_types::{Address, BlockData, CallType, TraceOutcome};

use crate::error::SyncError;
use crate::interval::{BlockRange, BlockRangeSet};

/// Rows per multi-row insert. Bounds the parameter count per statement.
const INSERT_BATCH_ROWS: usize = 500;

/// Cached chain data and interval bookkeeping.
pub struct SyncStore {
    pool: PgPool,
    schema: String,
}

impl SyncStore {
    /// Connects to `url` and scopes all tables to `schema`.
    ///
    /// The schema is created by [`SyncStore::initialize`], not by `connect`, so
    /// a connection can be established before the database is prepared.
    pub async fn connect(url: &str, schema: &str) -> Result<Self, SyncError> {
        let schema = quote_identifier(schema)?;
        let pool = PgPoolOptions::new().max_connections(5).connect(url).await?;
        Ok(Self { pool, schema })
    }

    /// Creates the schema and every cache table if they do not exist.
    pub async fn initialize(&self) -> Result<(), SyncError> {
        for statement in statements(&self.schema) {
            sqlx::query(&statement).execute(&self.pool).await?;
        }
        Ok(())
    }

    /// Writes all rows of one block. Safe to call again with the same block.
    pub async fn insert_block_data(
        &self,
        chain_id: u64,
        data: &BlockData,
    ) -> Result<(), SyncError> {
        let chain_id = as_i64(chain_id, "chain id")?;
        let blocks = match data.block {
            Some(block) => vec![BlockRow {
                number: as_i64(block.number, "block number")?,
                hash: block.hash.to_hex(),
                parent_hash: block.parent_hash.to_hex(),
                timestamp: as_i64(block.timestamp, "block timestamp")?,
            }],
            None => Vec::new(),
        };

        let mut transactions = Vec::with_capacity(data.transactions.len());
        for transaction in &data.transactions {
            transactions.push(TransactionRow {
                block_number: as_i64(transaction.block_number, "block number")?,
                transaction_index: as_i32(transaction.transaction_index, "transaction index")?,
                hash: transaction.hash.to_hex(),
                from: transaction.from.to_hex(),
                to: transaction.to.map(Address::to_hex),
            });
        }

        let mut receipts = Vec::with_capacity(data.receipts.len());
        for receipt in &data.receipts {
            receipts.push(ReceiptRow {
                block_number: as_i64(receipt.block_number, "block number")?,
                transaction_index: as_i32(receipt.transaction_index, "transaction index")?,
                from: receipt.from.to_hex(),
                to: receipt.to.map(Address::to_hex),
                success: receipt.success,
            });
        }

        let mut traces = Vec::with_capacity(data.traces.len());
        for trace in &data.traces {
            let mut trace_address = Vec::with_capacity(trace.trace_address.len());
            for index in &trace.trace_address {
                trace_address.push(i32::try_from(*index).map_err(|_| {
                    SyncError::InvalidResponse(format!(
                        "trace address component {index} exceeds the database integer range"
                    ))
                })?);
            }
            traces.push(TraceRow {
                block_number: as_i64(trace.block_number, "block number")?,
                transaction_index: as_i32(trace.transaction_index, "transaction index")?,
                trace_address,
                from: trace.trace.from.to_hex(),
                to: trace.trace.to.map(Address::to_hex),
                call_type: call_type_name(trace.trace.call_type).to_string(),
                input: trace.trace.input.clone(),
                value: trace.trace.value.map(|value| value.to_vec()),
                output: trace.trace.output.clone(),
                outcome: outcome_name(trace.trace.outcome).to_string(),
            });
        }

        let mut logs = Vec::with_capacity(data.logs.len());
        for log in &data.logs {
            logs.push(LogRow {
                block_number: as_i64(log.log.block_number, "block number")?,
                transaction_index: as_i32(log.transaction_index, "transaction index")?,
                log_index: as_i32(log.log_index, "log index")?,
                address: log.log.address.to_hex(),
                topics: log.log.topics.iter().map(|topic| topic.to_hex()).collect(),
                data: log.log.data.clone(),
            });
        }

        let schema = &self.schema;
        let mut transaction = self.pool.begin().await?;

        for block in &blocks {
            sqlx::query(&format!(
                r#"INSERT INTO {schema}."blocks" (chain_id, number, hash, parent_hash, timestamp)
                   VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING"#
            ))
            .bind(chain_id)
            .bind(block.number)
            .bind(&block.hash)
            .bind(&block.parent_hash)
            .bind(block.timestamp)
            .execute(&mut *transaction)
            .await?;
        }

        for chunk in transactions.chunks(INSERT_BATCH_ROWS) {
            let mut query = QueryBuilder::<Postgres>::new(format!(
                r#"INSERT INTO {schema}."transactions"
                   (chain_id, block_number, transaction_index, hash, "from", "to") "#
            ));
            query.push_values(chunk, |mut row, transaction| {
                row.push_bind(chain_id)
                    .push_bind(transaction.block_number)
                    .push_bind(transaction.transaction_index)
                    .push_bind(&transaction.hash)
                    .push_bind(&transaction.from)
                    .push_bind(&transaction.to);
            });
            query.push(" ON CONFLICT DO NOTHING");
            query.build().execute(&mut *transaction).await?;
        }

        for chunk in receipts.chunks(INSERT_BATCH_ROWS) {
            let mut query = QueryBuilder::<Postgres>::new(format!(
                r#"INSERT INTO {schema}."transaction_receipts"
                   (chain_id, block_number, transaction_index, "from", "to", success) "#
            ));
            query.push_values(chunk, |mut row, receipt| {
                row.push_bind(chain_id)
                    .push_bind(receipt.block_number)
                    .push_bind(receipt.transaction_index)
                    .push_bind(&receipt.from)
                    .push_bind(&receipt.to)
                    .push_bind(receipt.success);
            });
            query.push(" ON CONFLICT DO NOTHING");
            query.build().execute(&mut *transaction).await?;
        }

        for chunk in traces.chunks(INSERT_BATCH_ROWS) {
            let mut query = QueryBuilder::<Postgres>::new(format!(
                r#"INSERT INTO {schema}."traces"
                   (chain_id, block_number, transaction_index, trace_address, "from", "to",
                    call_type, input, value, output, outcome) "#
            ));
            query.push_values(chunk, |mut row, trace| {
                row.push_bind(chain_id)
                    .push_bind(trace.block_number)
                    .push_bind(trace.transaction_index)
                    .push_bind(&trace.trace_address)
                    .push_bind(&trace.from)
                    .push_bind(&trace.to)
                    .push_bind(&trace.call_type)
                    .push_bind(&trace.input)
                    .push_bind(&trace.value)
                    .push_bind(&trace.output)
                    .push_bind(&trace.outcome);
            });
            query.push(" ON CONFLICT DO NOTHING");
            query.build().execute(&mut *transaction).await?;
        }

        for chunk in logs.chunks(INSERT_BATCH_ROWS) {
            let mut query = QueryBuilder::<Postgres>::new(format!(
                r#"INSERT INTO {schema}."logs"
                   (chain_id, block_number, transaction_index, log_index, address, topics, data) "#
            ));
            query.push_values(chunk, |mut row, log| {
                row.push_bind(chain_id)
                    .push_bind(log.block_number)
                    .push_bind(log.transaction_index)
                    .push_bind(log.log_index)
                    .push_bind(&log.address)
                    .push_bind(&log.topics)
                    .push_bind(&log.data);
            });
            query.push(" ON CONFLICT DO NOTHING");
            query.build().execute(&mut *transaction).await?;
        }

        transaction.commit().await?;
        Ok(())
    }

    /// Unions `ranges` into the interval set stored for `(chain_id,
    /// fragment_id)`.
    ///
    /// Overlapping and adjacent ranges collapse into one `nummultirange` entry,
    /// so replaying a range is idempotent.
    pub async fn insert_intervals(
        &self,
        chain_id: u64,
        fragment_id: &str,
        ranges: &BlockRangeSet,
    ) -> Result<(), SyncError> {
        if ranges.is_empty() {
            return Ok(());
        }
        let chain_id = as_i64(chain_id, "chain id")?;
        sqlx::query(&format!(
            r#"INSERT INTO {schema}."intervals" AS existing (chain_id, fragment_id, blocks)
               VALUES ($1, $2, $3::nummultirange)
               ON CONFLICT (chain_id, fragment_id)
               DO UPDATE SET blocks = existing.blocks + EXCLUDED.blocks"#,
            schema = self.schema
        ))
        .bind(chain_id)
        .bind(fragment_id)
        .bind(encode_multirange(ranges))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Reads back the merged interval set for `(chain_id, fragment_id)`.
    pub async fn get_intervals(
        &self,
        chain_id: u64,
        fragment_id: &str,
    ) -> Result<BlockRangeSet, SyncError> {
        let chain_id = as_i64(chain_id, "chain id")?;
        let stored: Option<String> = sqlx::query_scalar(&format!(
            r#"SELECT blocks::text FROM {schema}."intervals"
               WHERE chain_id = $1 AND fragment_id = $2"#,
            schema = self.schema
        ))
        .bind(chain_id)
        .bind(fragment_id)
        .fetch_optional(&self.pool)
        .await?;

        match stored {
            Some(value) => parse_multirange(&value),
            None => Ok(BlockRangeSet::default()),
        }
    }

    /// Records factory child addresses. Each address keeps its earliest
    /// creation block across repeated calls.
    pub async fn append_factory_addresses(
        &self,
        chain_id: u64,
        factory_id: &str,
        values: &[(Address, u64)],
    ) -> Result<(), SyncError> {
        let chain_id = as_i64(chain_id, "chain id")?;
        let (factory,) = sqlx::query_as::<_, (i32,)>(&format!(
            r#"INSERT INTO {schema}."factories" (fragment_id) VALUES ($1)
               ON CONFLICT (fragment_id) DO UPDATE SET fragment_id = EXCLUDED.fragment_id
               RETURNING id"#,
            schema = self.schema
        ))
        .bind(factory_id)
        .fetch_one(&self.pool)
        .await?;

        let mut earliest: BTreeMap<Address, i64> = BTreeMap::new();
        for (address, block_number) in values {
            let block_number = as_i64(*block_number, "factory address block number")?;
            earliest
                .entry(*address)
                .and_modify(|current| *current = (*current).min(block_number))
                .or_insert(block_number);
        }
        if earliest.is_empty() {
            return Ok(());
        }

        let rows: Vec<(String, i64)> = earliest
            .into_iter()
            .map(|(address, block_number)| (address.to_hex(), block_number))
            .collect();

        for chunk in rows.chunks(INSERT_BATCH_ROWS) {
            let mut query = QueryBuilder::<Postgres>::new(format!(
                r#"INSERT INTO {schema}."factory_addresses" AS existing
                   (factory_id, chain_id, block_number, address) "#,
                schema = self.schema
            ));
            query.push_values(chunk, |mut row, (address, block_number)| {
                row.push_bind(factory)
                    .push_bind(chain_id)
                    .push_bind(*block_number)
                    .push_bind(address);
            });
            query.push(
                " ON CONFLICT (factory_id, address) DO UPDATE
                  SET block_number = LEAST(existing.block_number, EXCLUDED.block_number)",
            );
            query.build().execute(&self.pool).await?;
        }
        Ok(())
    }

    /// Returns `(address, creation block)` pairs ordered by creation block.
    pub async fn factory_addresses(
        &self,
        chain_id: u64,
        factory_id: &str,
    ) -> Result<Vec<(Address, u64)>, SyncError> {
        let chain_id = as_i64(chain_id, "chain id")?;
        let rows: Vec<(String, i64)> = sqlx::query_as(&format!(
            r#"SELECT address.address, address.block_number
               FROM {schema}."factory_addresses" AS address
               JOIN {schema}."factories" AS factory ON factory.id = address.factory_id
               WHERE factory.fragment_id = $1 AND address.chain_id = $2
               ORDER BY address.block_number, address.address"#,
            schema = self.schema
        ))
        .bind(factory_id)
        .bind(chain_id)
        .fetch_all(&self.pool)
        .await?;

        let mut values = Vec::with_capacity(rows.len());
        for (address, block_number) in rows {
            let address = Address::from_hex(&address).map_err(|error| {
                SyncError::InvalidResponse(format!(
                    "stored factory address {address:?} is invalid: {error}"
                ))
            })?;
            let block_number = u64::try_from(block_number).map_err(|_| {
                SyncError::InvalidResponse(format!(
                    "stored factory block number {block_number} is negative"
                ))
            })?;
            values.push((address, block_number));
        }
        Ok(values)
    }
}

struct BlockRow {
    number: i64,
    hash: String,
    parent_hash: String,
    timestamp: i64,
}

struct TransactionRow {
    block_number: i64,
    transaction_index: i32,
    hash: String,
    from: String,
    to: Option<String>,
}

struct ReceiptRow {
    block_number: i64,
    transaction_index: i32,
    from: String,
    to: Option<String>,
    success: bool,
}

struct TraceRow {
    block_number: i64,
    transaction_index: i32,
    trace_address: Vec<i32>,
    from: String,
    to: Option<String>,
    call_type: String,
    input: Vec<u8>,
    value: Option<Vec<u8>>,
    output: Option<Vec<u8>>,
    outcome: String,
}

struct LogRow {
    block_number: i64,
    transaction_index: i32,
    log_index: i32,
    address: String,
    topics: Vec<String>,
    data: Vec<u8>,
}

fn call_type_name(call_type: CallType) -> &'static str {
    match call_type {
        CallType::Call => "CALL",
        CallType::CallCode => "CALLCODE",
        CallType::DelegateCall => "DELEGATECALL",
        CallType::StaticCall => "STATICCALL",
        CallType::Create => "CREATE",
        CallType::Create2 => "CREATE2",
        CallType::SelfDestruct => "SELFDESTRUCT",
    }
}

fn outcome_name(outcome: TraceOutcome) -> &'static str {
    match outcome {
        TraceOutcome::Successful => "success",
        TraceOutcome::Reverted => "revert",
        TraceOutcome::Unknown => "unknown",
    }
}

fn as_i64(value: u64, field: &str) -> Result<i64, SyncError> {
    i64::try_from(value).map_err(|_| {
        SyncError::InvalidResponse(format!("{field} {value} exceeds the database bigint range"))
    })
}

fn as_i32(value: u64, field: &str) -> Result<i32, SyncError> {
    i32::try_from(value).map_err(|_| {
        SyncError::InvalidResponse(format!(
            "{field} {value} exceeds the database integer range"
        ))
    })
}

/// Restricts a schema name to `[A-Za-z0-9_]+` and returns it quoted.
///
/// Quoting alone prevents injection; the character restriction keeps the
/// identifier portable and catches configuration mistakes early.
fn quote_identifier(schema: &str) -> Result<String, SyncError> {
    let valid = !schema.is_empty()
        && schema.len() <= 63
        && schema
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    if !valid {
        return Err(SyncError::InvalidSchema(schema.to_string()));
    }
    Ok(format!("\"{schema}\""))
}

/// Encodes ranges as a `nummultirange` literal with exclusive upper bounds.
///
/// `[a, b]` becomes `[a, b + 1)` so Postgres merges adjacent ranges, which
/// makes stored intervals canonical.
fn encode_multirange(ranges: &BlockRangeSet) -> String {
    if ranges.is_empty() {
        return "{}".to_string();
    }
    let inner = ranges
        .ranges()
        .iter()
        .map(|range| format!("[{},{})", range.start(), u128::from(range.end()) + 1))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{inner}}}")
}

/// Inverse of [`encode_multirange`], tolerant of Postgres' canonical output.
fn parse_multirange(value: &str) -> Result<BlockRangeSet, SyncError> {
    let trimmed = value.trim();
    let body = trimmed
        .strip_prefix('{')
        .and_then(|rest| rest.strip_suffix('}'))
        .ok_or_else(|| invalid_interval(value))?;

    let mut ranges = Vec::new();
    let mut index = 0;
    let bytes = body.as_bytes();
    while index < bytes.len() {
        match bytes[index] {
            b'[' | b'(' => {
                let lower_inclusive = bytes[index] == b'[';
                let rest = &body[index + 1..];
                let close = rest
                    .find([')', ']'])
                    .ok_or_else(|| invalid_interval(value))?;
                let upper_inclusive = rest.as_bytes()[close] == b']';
                let inner = &rest[..close];
                let (lower, upper) = inner
                    .split_once(',')
                    .ok_or_else(|| invalid_interval(value))?;

                let start = match parse_bound(lower, value)? {
                    None => 0,
                    Some(bound) if lower_inclusive => bound,
                    Some(bound) => bound
                        .checked_add(1)
                        .ok_or_else(|| invalid_interval(value))?,
                };
                let end = match parse_bound(upper, value)? {
                    None => u128::from(u64::MAX),
                    Some(bound) if upper_inclusive => bound,
                    Some(bound) => bound
                        .checked_sub(1)
                        .ok_or_else(|| invalid_interval(value))?,
                };
                if start <= end {
                    let start = u64::try_from(start).map_err(|_| invalid_interval(value))?;
                    let end = u64::try_from(end).map_err(|_| invalid_interval(value))?;
                    let range =
                        BlockRange::new(start, end).ok_or_else(|| invalid_interval(value))?;
                    ranges.push(range);
                }
                index += 1 + close + 1;
            }
            b',' | b' ' | b'\t' | b'\r' | b'\n' => index += 1,
            _ => return Err(invalid_interval(value)),
        }
    }
    Ok(BlockRangeSet::new(ranges))
}

fn parse_bound(value: &str, whole: &str) -> Result<Option<u128>, SyncError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse::<u128>()
        .map(Some)
        .map_err(|_| invalid_interval(whole))
}

fn invalid_interval(value: &str) -> SyncError {
    SyncError::InvalidResponse(format!("invalid nummultirange {value:?}"))
}

fn statements(schema: &str) -> Vec<String> {
    vec![
        format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}."blocks" (
                chain_id bigint NOT NULL,
                number bigint NOT NULL,
                hash varchar(66) NOT NULL,
                parent_hash varchar(66) NOT NULL,
                timestamp bigint NOT NULL,
                PRIMARY KEY (chain_id, number)
            )"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}."transactions" (
                chain_id bigint NOT NULL,
                block_number bigint NOT NULL,
                transaction_index integer NOT NULL,
                hash varchar(66) NOT NULL,
                "from" varchar(42) NOT NULL,
                "to" varchar(42),
                PRIMARY KEY (chain_id, block_number, transaction_index)
            )"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}."transaction_receipts" (
                chain_id bigint NOT NULL,
                block_number bigint NOT NULL,
                transaction_index integer NOT NULL,
                "from" varchar(42) NOT NULL,
                "to" varchar(42),
                success boolean NOT NULL,
                PRIMARY KEY (chain_id, block_number, transaction_index)
            )"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}."traces" (
                chain_id bigint NOT NULL,
                block_number bigint NOT NULL,
                transaction_index integer NOT NULL,
                trace_address integer[] NOT NULL,
                "from" varchar(42) NOT NULL,
                "to" varchar(42),
                call_type text NOT NULL,
                input bytea NOT NULL,
                value bytea,
                output bytea,
                outcome text NOT NULL,
                PRIMARY KEY (chain_id, block_number, transaction_index, trace_address)
            )"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}."logs" (
                chain_id bigint NOT NULL,
                block_number bigint NOT NULL,
                transaction_index integer NOT NULL,
                log_index integer NOT NULL,
                address varchar(42) NOT NULL,
                topics text[] NOT NULL,
                data bytea NOT NULL,
                PRIMARY KEY (chain_id, block_number, log_index)
            )"#
        ),
        format!(
            r#"CREATE INDEX IF NOT EXISTS "logs_address_idx"
               ON {schema}."logs" (chain_id, address)"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}."intervals" (
                chain_id bigint NOT NULL,
                fragment_id text NOT NULL,
                blocks nummultirange NOT NULL,
                PRIMARY KEY (chain_id, fragment_id)
            )"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}."factories" (
                id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                fragment_id text NOT NULL UNIQUE
            )"#
        ),
        format!(
            r#"CREATE TABLE IF NOT EXISTS {schema}."factory_addresses" (
                factory_id integer NOT NULL,
                chain_id bigint NOT NULL,
                block_number bigint NOT NULL,
                address varchar(42) NOT NULL,
                PRIMARY KEY (factory_id, address)
            )"#
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(start: u64, end: u64) -> BlockRange {
        BlockRange::new(start, end).unwrap()
    }

    fn set(ranges: &[(u64, u64)]) -> BlockRangeSet {
        BlockRangeSet::new(
            ranges
                .iter()
                .map(|&(start, end)| range(start, end))
                .collect(),
        )
    }

    #[test]
    fn multirange_round_trips_through_postgres_text() {
        let value = set(&[(0, 10), (20, 20), (40, 50)]);
        let encoded = encode_multirange(&value);
        assert_eq!(encoded, "{[0,11),[20,21),[40,51)}");
        assert_eq!(parse_multirange(&encoded).unwrap(), value);
    }

    #[test]
    fn empty_sets_and_adjacent_ranges_are_canonical() {
        assert_eq!(encode_multirange(&BlockRangeSet::default()), "{}");
        assert!(parse_multirange("{}").unwrap().is_empty());
        assert!(parse_multirange(" { } ").unwrap().is_empty());
        assert_eq!(
            set(&[(0, 10), (11, 20)]).ranges(),
            &[range(0, 20)],
            "adjacent ranges merge before encoding"
        );
        assert_eq!(
            parse_multirange("{[0,21)}").unwrap(),
            set(&[(0, 20)]),
            "canonical output from Postgres is readable"
        );
    }

    #[test]
    fn unbounded_bounds_decode_to_the_full_domain() {
        assert_eq!(parse_multirange("{(,11)}").unwrap(), set(&[(0, 10)]));
        assert_eq!(parse_multirange("{[5,)}").unwrap(), set(&[(5, u64::MAX)]));
        assert_eq!(
            parse_multirange("{[0,11),[20,21)}").unwrap(),
            set(&[(0, 10), (20, 20)])
        );
    }

    #[test]
    fn malformed_multiranges_are_rejected() {
        for value in ["", "{", "{[0,11)", "{[a,b)}", "{[0,11);[20,21)}", "{}x"] {
            assert!(
                matches!(parse_multirange(value), Err(SyncError::InvalidResponse(_))),
                "{value:?} should be rejected"
            );
        }
    }

    #[test]
    fn schema_names_are_validated_and_quoted() {
        assert_eq!(
            quote_identifier("stele_cache_1").unwrap(),
            "\"stele_cache_1\""
        );
        for schema in ["", "public; DROP TABLE blocks", "with space", "quote\"d"] {
            assert!(matches!(
                quote_identifier(schema),
                Err(SyncError::InvalidSchema(_))
            ));
        }
    }

    #[test]
    fn identifiers_beyond_postgres_limits_are_rejected() {
        assert!(quote_identifier(&"a".repeat(64)).is_err());
        assert!(quote_identifier(&"a".repeat(63)).is_ok());
    }
}
