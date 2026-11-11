//! Async JSON-RPC retrieval with endpoint failover and retries.
//!
//! [`RpcClient`] speaks HTTP through [`HttpTransport`] by default. The
//! [`RpcTransport`] trait is a seam for in-memory transports in tests; it lives
//! at the HTTP-body level so envelope handling, error mapping and retry policy
//! are exercised by the same code path in tests and production.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures_util::stream::{self, StreamExt};
use serde_json::{json, Map, Value};

use stele_types::{
    Address, BlockData, BlockRef, CallType, Log, LogRef, ReceiptRef, Topic, Trace, TraceOutcome,
    TraceRef, TransactionRef, B256,
};

use crate::error::SyncError;
use crate::hex;
use crate::interval::BlockRange;
use crate::rpc::{quantity, ADDRESS_CHUNK_SIZE};

/// Block span of one `eth_getLogs` request. A conservative initial estimate;
/// callers that learn a provider limit should issue smaller ranges instead.
pub const LOG_RANGE_CHUNK: u64 = 500;

/// Maximum number of `eth_getTransactionReceipt` calls in flight.
const RECEIPT_CONCURRENCY: usize = 10;

/// One HTTP round trip at the JSON-RPC body level.
#[async_trait]
pub trait RpcTransport: Send + Sync {
    /// Posts `body` to `url` and returns the parsed JSON response.
    ///
    /// Implementations must return [`SyncError::Transport`] for network or HTTP
    /// status failures and must not interpret JSON-RPC envelopes; that is the
    /// client's responsibility.
    async fn post(&self, url: &str, body: &str) -> Result<Value, SyncError>;
}

/// Retry and backoff configuration for one client.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    /// Additional rounds after the first attempt.
    pub max_retries: u32,
    /// Delay before the first retry; doubled each round.
    pub base_delay: Duration,
    /// Upper bound for the doubling delay.
    pub max_delay: Duration,
}

impl RetryPolicy {
    /// Three retries with exponential backoff, as required by the API contract.
    pub const fn default_policy() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(2),
        }
    }

    /// No backoff. Intended for tests; retries still happen.
    pub const fn immediate() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    fn delay(&self, round: u32) -> Duration {
        if self.base_delay.is_zero() {
            return Duration::ZERO;
        }
        let factor = 1u32 << round.min(16);
        self.base_delay.saturating_mul(factor).min(self.max_delay)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

/// `reqwest`-backed transport with a request timeout.
pub struct HttpTransport {
    client: reqwest::Client,
}

impl HttpTransport {
    /// Builds a transport with a 30 second request timeout.
    pub fn new() -> Result<Self, SyncError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .map_err(|error| SyncError::Transport(error.to_string()))?;
        Ok(Self { client })
    }
}

#[async_trait]
impl RpcTransport for HttpTransport {
    async fn post(&self, url: &str, body: &str) -> Result<Value, SyncError> {
        let response = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_owned())
            .send()
            .await
            .map_err(|error| SyncError::Transport(format!("{url}: {error}")))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|error| SyncError::Transport(format!("{url}: {error}")))?;
        if !status.is_success() {
            return Err(SyncError::Transport(format!(
                "{url} returned HTTP {status}: {text}"
            )));
        }
        serde_json::from_str(&text).map_err(|error| {
            SyncError::InvalidResponse(format!("{url} returned invalid JSON: {error}"))
        })
    }
}

/// JSON-RPC client for one chain with multiple interchangeable endpoints.
///
/// Requests rotate across endpoints; a failed endpoint is retried on the next
/// one, and the whole set is retried with exponential backoff. The endpoint
/// that last succeeded is preferred for the next request.
pub struct RpcClient {
    urls: Vec<String>,
    transport: Arc<dyn RpcTransport>,
    retry: RetryPolicy,
    /// Index of the endpoint that last succeeded.
    cursor: AtomicUsize,
    next_id: AtomicU64,
}

impl RpcClient {
    /// Creates a client for `rpc_urls` using the built-in HTTP transport.
    ///
    /// Returns [`SyncError::InvalidUrl`] when the list is empty or contains a
    /// URL that is not `http://` or `https://`.
    pub fn new(rpc_urls: Vec<String>) -> Result<Self, SyncError> {
        let transport = Arc::new(HttpTransport::new()?);
        Self::with_transport(rpc_urls, transport)
    }

    /// Creates a client with a caller-supplied transport.
    ///
    /// Useful for in-memory transports in tests and for providers that need
    /// custom headers or authentication.
    pub fn with_transport(
        rpc_urls: Vec<String>,
        transport: Arc<dyn RpcTransport>,
    ) -> Result<Self, SyncError> {
        if rpc_urls.is_empty() {
            return Err(SyncError::InvalidUrl(
                "at least one rpc url is required".into(),
            ));
        }
        for url in &rpc_urls {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err(SyncError::InvalidUrl(url.clone()));
            }
        }
        Ok(Self {
            urls: rpc_urls,
            transport,
            retry: RetryPolicy::default(),
            cursor: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
        })
    }

    /// Overrides the retry policy. Builder-style; consumes and returns `self`.
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Chain id reported by `eth_chainId`.
    pub async fn chain_id(&self) -> Result<u64, SyncError> {
        let result = self.call("eth_chainId", json!([])).await?;
        hex::quantity_u64(result_str(&result, "eth_chainId")?)
    }

    /// Latest block number reported by `eth_blockNumber`.
    pub async fn latest_block_number(&self) -> Result<u64, SyncError> {
        let result = self.call("eth_blockNumber", json!([])).await?;
        hex::quantity_u64(result_str(&result, "eth_blockNumber")?)
    }

    /// Fetches a canonical block header by number.
    pub async fn block_by_number(&self, number: u64) -> Result<BlockRef, SyncError> {
        let result = self
            .call("eth_getBlockByNumber", json!([quantity(number), false]))
            .await?;
        parse_block_ref(&result, Some(number))
    }

    /// Fetches a canonical block header by hash.
    pub async fn block_by_hash(&self, hash: B256) -> Result<BlockRef, SyncError> {
        let result = self
            .call("eth_getBlockByHash", json!([hash.to_hex(), false]))
            .await?;
        let block = parse_block_ref(&result, None)?;
        if block.hash != hash {
            return Err(SyncError::InvalidResponse(format!(
                "eth_getBlockByHash returned {} for {}",
                block.hash, hash
            )));
        }
        Ok(block)
    }

    /// Fetches everything needed to index one block: the header, full
    /// transactions, receipts, logs, and optionally call traces.
    ///
    /// Transactions are sorted by index, logs by `(transaction_index,
    /// log_index)`, receipts by index, and traces by
    /// `(transaction_index, trace_address)`.
    pub async fn block_data(
        &self,
        number: u64,
        fetch_traces: bool,
    ) -> Result<BlockData, SyncError> {
        let raw = self
            .call("eth_getBlockByNumber", json!([quantity(number), true]))
            .await?;
        let block = parse_block_ref(&raw, Some(number))?;
        let object = raw
            .as_object()
            .ok_or_else(|| SyncError::InvalidResponse("block must be an object".into()))?;
        let transaction_values = required(object, "transactions", "block")?
            .as_array()
            .ok_or_else(|| {
                SyncError::InvalidResponse(
                    "block.transactions must be an array of full transaction objects".into(),
                )
            })?;
        let transactions = parse_transactions(transaction_values, number)?;

        let receipts = self.block_receipts(&transactions, number);
        let logs = self.block_logs(number);
        let traces = async {
            if fetch_traces {
                self.block_traces(&transactions, number).await
            } else {
                Ok(Vec::new())
            }
        };
        let (receipts, logs, traces) = tokio::join!(receipts, logs, traces);

        Ok(BlockData {
            block: Some(block),
            transactions,
            receipts: receipts?,
            traces: traces?,
            logs: logs?,
        })
    }

    /// Fetches logs for an inclusive range.
    ///
    /// The range is split into [`LOG_RANGE_CHUNK`] blocks per request and the
    /// address list into 50-address groups. `topics` is one OR
    /// list per position; an empty inner list is a wildcard. Trailing wildcard
    /// positions are omitted. `None` addresses means any address; an empty list
    /// matches nothing and sends no request. Results are deduplicated and sorted
    /// by `(block_number, transaction_index, log_index)`.
    pub async fn logs(
        &self,
        range: BlockRange,
        addresses: Option<&[Address]>,
        topics: &[Vec<Topic>],
    ) -> Result<Vec<LogRef>, SyncError> {
        if topics.len() > 4 {
            return Err(SyncError::InvalidResponse(format!(
                "eth_getLogs supports at most 4 topic positions, got {}",
                topics.len()
            )));
        }
        let addresses = addresses.map(|values| {
            let mut unique = values.to_vec();
            unique.sort_unstable();
            unique.dedup();
            unique
        });
        let chunks: Vec<Option<Vec<String>>> = match &addresses {
            None => vec![None],
            Some(values) => {
                if values.is_empty() {
                    return Ok(Vec::new());
                }
                values
                    .chunks(ADDRESS_CHUNK_SIZE)
                    .map(|chunk| Some(chunk.iter().map(|address| address.to_hex()).collect()))
                    .collect()
            }
        };
        let topic_layers = topic_layers(topics);
        let page_size = NonZeroU64::new(LOG_RANGE_CHUNK).ok_or_else(|| {
            SyncError::InvalidResponse("log range chunk size must be nonzero".into())
        })?;

        let mut logs = Vec::new();
        for page in range.chunks(page_size) {
            for chunk in &chunks {
                let mut filter = Map::new();
                filter.insert("fromBlock".into(), Value::String(quantity(page.start())));
                filter.insert("toBlock".into(), Value::String(quantity(page.end())));
                filter.insert("topics".into(), Value::Array(topic_layers.clone()));
                if let Some(chunk) = chunk {
                    filter.insert(
                        "address".into(),
                        Value::Array(chunk.iter().cloned().map(Value::String).collect()),
                    );
                }
                let result = self
                    .call("eth_getLogs", json!([Value::Object(filter)]))
                    .await?;
                let items = result.as_array().ok_or_else(|| {
                    SyncError::InvalidResponse("eth_getLogs must return an array".into())
                })?;
                for item in items {
                    logs.push(parse_log_ref(item, Some(page))?);
                }
            }
        }

        finish_logs(logs)
    }

    async fn block_receipts(
        &self,
        transactions: &[TransactionRef],
        number: u64,
    ) -> Result<Vec<ReceiptRef>, SyncError> {
        let mut stream = stream::iter(transactions.iter().map(|transaction| async move {
            let result = self
                .call(
                    "eth_getTransactionReceipt",
                    json!([transaction.hash.to_hex()]),
                )
                .await?;
            parse_receipt(&result, number, transaction)
        }))
        .buffer_unordered(RECEIPT_CONCURRENCY);

        let mut receipts = Vec::with_capacity(transactions.len());
        while let Some(receipt) = stream.next().await {
            receipts.push(receipt?);
        }
        receipts.sort_by_key(|receipt| receipt.transaction_index);
        Ok(receipts)
    }

    async fn block_logs(&self, number: u64) -> Result<Vec<LogRef>, SyncError> {
        let filter = json!({
            "fromBlock": quantity(number),
            "toBlock": quantity(number),
            "topics": [],
        });
        let result = self.call("eth_getLogs", json!([filter])).await?;
        let items = result
            .as_array()
            .ok_or_else(|| SyncError::InvalidResponse("eth_getLogs must return an array".into()))?;
        let block = BlockRange::new(number, number)
            .ok_or_else(|| SyncError::InvalidResponse("invalid block range".into()))?;
        let mut logs = Vec::with_capacity(items.len());
        for item in items {
            logs.push(parse_log_ref(item, Some(block))?);
        }
        finish_logs(logs)
    }

    async fn block_traces(
        &self,
        transactions: &[TransactionRef],
        number: u64,
    ) -> Result<Vec<TraceRef>, SyncError> {
        let result = self
            .call(
                "debug_traceBlockByNumber",
                json!([quantity(number), { "tracer": "callTracer" }]),
            )
            .await?;
        parse_traces(&result, transactions, number)
    }

    /// Performs one JSON-RPC call with failover and exponential backoff.
    async fn call(&self, method: &str, params: Value) -> Result<Value, SyncError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params,
        })
        .to_string();

        let start = self.cursor.load(Ordering::Relaxed) % self.urls.len();
        let mut last_error = None;
        let mut attempts = 0u32;
        for round in 0..=self.retry.max_retries {
            if round > 0 {
                let delay = self.retry.delay(round - 1);
                if !delay.is_zero() {
                    tracing::debug!(
                        method,
                        round,
                        delay_ms = delay.as_millis() as u64,
                        "retrying rpc request"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
            for offset in 0..self.urls.len() {
                let index = (start + offset) % self.urls.len();
                let url = &self.urls[index];
                attempts += 1;
                let attempt = match self.transport.post(url, &body).await {
                    Ok(response) => parse_response(&response, id),
                    Err(error) => Err(error),
                };
                match attempt {
                    Ok(result) => {
                        self.cursor.store(index, Ordering::Relaxed);
                        return Ok(result);
                    }
                    Err(error) => {
                        // The endpoint itself is logged, not the URL: RPC URLs
                        // usually embed provider API keys.
                        tracing::debug!(
                            method,
                            endpoint = %endpoint_label(url),
                            error_kind = error_kind(&error),
                            "rpc endpoint failed"
                        );
                        last_error = Some(error);
                    }
                }
            }
        }

        let error = last_error.unwrap_or_else(|| {
            SyncError::InvalidResponse(format!("{method} failed without an rpc endpoint"))
        });
        tracing::warn!(
            method,
            attempts,
            endpoints = self.urls.len(),
            error_kind = error_kind(&error),
            error = %redact_urls(&error.to_string(), &self.urls),
            "rpc request failed on every endpoint"
        );
        Err(error)
    }
}

/// Short identifier for an RPC error, safe to log.
fn error_kind(error: &SyncError) -> &'static str {
    match error {
        SyncError::Transport(_) => "transport",
        SyncError::Rpc { .. } => "rpc",
        SyncError::InvalidResponse(_) => "invalid_response",
        SyncError::InvalidUrl(_) => "invalid_url",
        SyncError::NotFound(_) => "not_found",
        SyncError::InvalidSchema(_) => "invalid_schema",
        SyncError::Database(_) => "database",
    }
}

/// Reduces an endpoint URL to `scheme://authority` for logging.
///
/// Paths and query strings are dropped and userinfo is stripped, because
/// hosted providers usually place the API key there.
fn endpoint_label(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return "<invalid-url>".to_owned();
    };
    let authority = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .rsplit('@')
        .next()
        .unwrap_or_default();
    format!("{scheme}://{authority}")
}

/// Replaces every configured endpoint in `message` with its redacted label.
fn redact_urls(message: &str, urls: &[String]) -> String {
    let mut redacted = message.to_owned();
    for url in urls {
        if !url.is_empty() {
            redacted = redacted.replace(url.as_str(), &endpoint_label(url));
        }
    }
    redacted
}

fn result_str<'a>(value: &'a Value, method: &str) -> Result<&'a str, SyncError> {
    value
        .as_str()
        .ok_or_else(|| SyncError::InvalidResponse(format!("{method} must return a hex string")))
}

/// Validates a JSON-RPC envelope and returns its `result`.
fn parse_response(value: &Value, expected_id: u64) -> Result<Value, SyncError> {
    let object = value
        .as_object()
        .ok_or_else(|| SyncError::InvalidResponse("json-rpc response must be an object".into()))?;
    if let Some(version) = object.get("jsonrpc") {
        if version.as_str() != Some("2.0") {
            return Err(SyncError::InvalidResponse(
                "json-rpc response has an unsupported version".into(),
            ));
        }
    }
    if let Some(error) = object.get("error") {
        if !error.is_null() {
            let error = error.as_object().ok_or_else(|| {
                SyncError::InvalidResponse("json-rpc error must be an object".into())
            })?;
            let code = error.get("code").and_then(Value::as_i64).ok_or_else(|| {
                SyncError::InvalidResponse("json-rpc error is missing a numeric code".into())
            })?;
            let message = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown rpc error")
                .to_string();
            return Err(SyncError::Rpc { code, message });
        }
    }
    let id = object
        .get("id")
        .ok_or_else(|| SyncError::InvalidResponse("json-rpc response is missing id".into()))?;
    let id_matches = match id {
        Value::Number(number) => number.as_u64() == Some(expected_id),
        Value::String(text) => text.parse::<u64>().ok() == Some(expected_id),
        _ => false,
    };
    if !id_matches {
        return Err(SyncError::InvalidResponse(format!(
            "json-rpc response id {id} does not match request id {expected_id}"
        )));
    }
    object
        .get("result")
        .cloned()
        .ok_or_else(|| SyncError::InvalidResponse("json-rpc response is missing result".into()))
}

fn parse_block_ref(value: &Value, expected: Option<u64>) -> Result<BlockRef, SyncError> {
    if value.is_null() {
        let subject =
            expected.map_or_else(|| "block".to_string(), |number| format!("block {number}"));
        return Err(SyncError::NotFound(subject));
    }
    let object = value
        .as_object()
        .ok_or_else(|| SyncError::InvalidResponse("block must be an object".into()))?;
    let number = hex::quantity_u64(required_str(object, "number", "block")?)?;
    if let Some(expected) = expected {
        if number != expected {
            return Err(SyncError::InvalidResponse(format!(
                "block number mismatch: requested {expected}, received {number}"
            )));
        }
    }
    Ok(BlockRef {
        number,
        hash: hex::hash(required_str(object, "hash", "block")?)?,
        parent_hash: hex::hash(required_str(object, "parentHash", "block")?)?,
        timestamp: hex::quantity_u64(required_str(object, "timestamp", "block")?)?,
    })
}

fn parse_transactions(
    values: &[Value],
    block_number: u64,
) -> Result<Vec<TransactionRef>, SyncError> {
    let mut transactions = Vec::with_capacity(values.len());
    let mut indexes = BTreeSet::new();
    for value in values {
        let object = value.as_object().ok_or_else(|| {
            SyncError::InvalidResponse(
                "block.transactions must contain full transaction objects".into(),
            )
        })?;
        let transaction_index =
            hex::quantity_u64(required_str(object, "transactionIndex", "transaction")?)?;
        if !indexes.insert(transaction_index) {
            return Err(SyncError::InvalidResponse(format!(
                "duplicate transaction index {transaction_index}"
            )));
        }
        match object.get("blockNumber") {
            None | Some(Value::Null) => {}
            Some(Value::String(value)) => {
                let reported = hex::quantity_u64(value)?;
                if reported != block_number {
                    return Err(SyncError::InvalidResponse(format!(
                        "transaction block number {reported} does not match {block_number}"
                    )));
                }
            }
            Some(_) => {
                return Err(SyncError::InvalidResponse(
                    "transaction.blockNumber must be a hex quantity".into(),
                ))
            }
        }
        let to = match object.get("to") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(hex::address(value)?),
            Some(_) => {
                return Err(SyncError::InvalidResponse(
                    "transaction.to must be an address or null".into(),
                ))
            }
        };
        transactions.push(TransactionRef {
            block_number,
            transaction_index,
            hash: hex::hash(required_str(object, "hash", "transaction")?)?,
            from: hex::address(required_str(object, "from", "transaction")?)?,
            to,
        });
    }
    transactions.sort_by_key(|transaction| transaction.transaction_index);
    Ok(transactions)
}

fn parse_receipt(
    value: &Value,
    block_number: u64,
    transaction: &TransactionRef,
) -> Result<ReceiptRef, SyncError> {
    if value.is_null() {
        return Err(SyncError::NotFound(format!(
            "receipt for transaction {}",
            transaction.hash
        )));
    }
    let object = value
        .as_object()
        .ok_or_else(|| SyncError::InvalidResponse("receipt must be an object".into()))?;
    let receipt_block = hex::quantity_u64(required_str(object, "blockNumber", "receipt")?)?;
    if receipt_block != block_number {
        return Err(SyncError::InvalidResponse(format!(
            "receipt block number {receipt_block} does not match {block_number}"
        )));
    }
    let transaction_index =
        hex::quantity_u64(required_str(object, "transactionIndex", "receipt")?)?;
    if transaction_index != transaction.transaction_index {
        return Err(SyncError::InvalidResponse(format!(
            "receipt index {transaction_index} does not match transaction index {}",
            transaction.transaction_index
        )));
    }
    let from = hex::address(required_str(object, "from", "receipt")?)?;
    if from != transaction.from {
        return Err(SyncError::InvalidResponse(format!(
            "receipt sender {from} does not match transaction sender {}",
            transaction.from
        )));
    }
    let to = match object.get("to") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(hex::address(value)?),
        Some(_) => {
            return Err(SyncError::InvalidResponse(
                "receipt.to must be an address or null".into(),
            ))
        }
    };
    if to != transaction.to {
        return Err(SyncError::InvalidResponse(format!(
            "receipt recipient {to:?} does not match transaction recipient {:?}",
            transaction.to
        )));
    }
    let success = match hex::quantity_u64(required_str(object, "status", "receipt")?)? {
        1 => true,
        0 => false,
        status => {
            return Err(SyncError::InvalidResponse(format!(
                "receipt status {status} must be 0 or 1"
            )))
        }
    };
    Ok(ReceiptRef {
        block_number,
        transaction_index,
        from,
        to,
        success,
    })
}

fn parse_log_ref(value: &Value, expected: Option<BlockRange>) -> Result<LogRef, SyncError> {
    let object = value
        .as_object()
        .ok_or_else(|| SyncError::InvalidResponse("log must be an object".into()))?;
    let block_number = hex::quantity_u64(required_str(object, "blockNumber", "log")?)?;
    if let Some(range) = expected {
        if block_number < range.start() || block_number > range.end() {
            return Err(SyncError::InvalidResponse(format!(
                "log block {block_number} is outside the requested range {}-{}",
                range.start(),
                range.end()
            )));
        }
    }
    let transaction_index = hex::quantity_u64(required_str(object, "transactionIndex", "log")?)?;
    let log_index = hex::quantity_u64(required_str(object, "logIndex", "log")?)?;
    let topic_values = required(object, "topics", "log")?
        .as_array()
        .ok_or_else(|| SyncError::InvalidResponse("log.topics must be an array".into()))?;
    if topic_values.len() > 4 {
        return Err(SyncError::InvalidResponse(format!(
            "log has {} topics; at most 4 are allowed",
            topic_values.len()
        )));
    }
    let mut topics = Vec::with_capacity(topic_values.len());
    for value in topic_values {
        let text = value
            .as_str()
            .ok_or_else(|| SyncError::InvalidResponse("log topic must be a hex string".into()))?;
        topics.push(hex::topic(text)?);
    }
    Ok(LogRef {
        transaction_index,
        log_index,
        log: Log {
            block_number,
            address: hex::address(required_str(object, "address", "log")?)?,
            topics,
            data: hex::data(required_str(object, "data", "log")?)?,
        },
    })
}

/// Rejects duplicate `(block, transaction, log)` positions, then sorts.
///
/// Duplicates indicate a provider sent the same log twice; address chunks are
/// disjoint and range chunks do not overlap, so this is always an error.
fn finish_logs(mut logs: Vec<LogRef>) -> Result<Vec<LogRef>, SyncError> {
    let mut seen = BTreeSet::new();
    for log in &logs {
        if !seen.insert((log.log.block_number, log.transaction_index, log.log_index)) {
            return Err(SyncError::InvalidResponse(format!(
                "duplicate log at block {} transaction {} index {}",
                log.log.block_number, log.transaction_index, log.log_index
            )));
        }
    }
    logs.sort_by_key(|log| (log.log.block_number, log.transaction_index, log.log_index));
    Ok(logs)
}

fn topic_layers(topics: &[Vec<Topic>]) -> Vec<Value> {
    // Every non-empty layer is an OR list, even when it holds one topic, to
    // match the encoding in the transport-independent `rpc` module.
    let mut layers: Vec<Value> = topics
        .iter()
        .map(|layer| {
            if layer.is_empty() {
                Value::Null
            } else {
                Value::Array(
                    layer
                        .iter()
                        .map(|topic| Value::String(topic.to_hex()))
                        .collect(),
                )
            }
        })
        .collect();
    while matches!(layers.last(), Some(Value::Null)) {
        layers.pop();
    }
    layers
}

fn parse_traces(
    value: &Value,
    transactions: &[TransactionRef],
    block_number: u64,
) -> Result<Vec<TraceRef>, SyncError> {
    let entries = value.as_array().ok_or_else(|| {
        SyncError::InvalidResponse("debug_traceBlockByNumber must return an array".into())
    })?;
    let by_hash: BTreeMap<B256, u64> = transactions
        .iter()
        .map(|transaction| (transaction.hash, transaction.transaction_index))
        .collect();

    let mut traces = Vec::new();
    for entry in entries {
        let object = entry
            .as_object()
            .ok_or_else(|| SyncError::InvalidResponse("trace entry must be an object".into()))?;
        let hash_value = object
            .get("txHash")
            .or_else(|| object.get("transactionHash"))
            .ok_or_else(|| SyncError::InvalidResponse("trace entry is missing txHash".into()))?;
        let hash_text = hash_value.as_str().ok_or_else(|| {
            SyncError::InvalidResponse("trace entry txHash must be a hex string".into())
        })?;
        let hash = hex::hash(hash_text)?;
        let transaction_index = by_hash.get(&hash).copied().ok_or_else(|| {
            SyncError::InvalidResponse(format!(
                "trace transaction {hash} is not present in block {block_number}"
            ))
        })?;
        let frame = object
            .get("result")
            .ok_or_else(|| SyncError::InvalidResponse("trace entry is missing result".into()))?;
        flatten_frame(
            frame,
            block_number,
            transaction_index,
            &[],
            false,
            &mut traces,
        )?;
    }

    traces.sort_by(|left, right| {
        (left.transaction_index, &left.trace_address)
            .cmp(&(right.transaction_index, &right.trace_address))
    });
    Ok(traces)
}

/// Appends one call frame and its descendants in pre-order.
///
/// `parent_reverted` makes every descendant of a reverted frame reverted, even
/// when the child frame itself reports no error.
fn flatten_frame(
    frame: &Value,
    block_number: u64,
    transaction_index: u64,
    path: &[u32],
    parent_reverted: bool,
    traces: &mut Vec<TraceRef>,
) -> Result<(), SyncError> {
    let frame_object = frame
        .as_object()
        .ok_or_else(|| SyncError::InvalidResponse("trace frame must be an object".into()))?;
    let call_type = parse_call_type(required_str(frame_object, "type", "trace")?)?;
    let to = match frame_object.get("to") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(hex::address(value)?),
        Some(_) => {
            return Err(SyncError::InvalidResponse(
                "trace.to must be an address or null".into(),
            ))
        }
    };
    let output = match frame_object.get("output") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(hex::data(value)?),
        Some(_) => {
            return Err(SyncError::InvalidResponse(
                "trace.output must be hex data or null".into(),
            ))
        }
    };
    let value = match frame_object.get("value") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(hex::quantity_u256(value)?),
        Some(_) => {
            return Err(SyncError::InvalidResponse(
                "trace.value must be a hex quantity or null".into(),
            ))
        }
    };
    let error = match frame_object.get("error") {
        None | Some(Value::Null) => None,
        Some(Value::String(value)) => Some(value.as_str()),
        Some(_) => {
            return Err(SyncError::InvalidResponse(
                "trace.error must be a string or null".into(),
            ))
        }
    };
    let reverted = parent_reverted || error.is_some();

    traces.push(TraceRef {
        block_number,
        transaction_index,
        trace_address: path.to_vec(),
        trace: Trace {
            from: hex::address(required_str(frame_object, "from", "trace")?)?,
            to,
            call_type,
            input: hex::data(required_str(frame_object, "input", "trace")?)?,
            value,
            output,
            outcome: if reverted {
                TraceOutcome::Reverted
            } else {
                TraceOutcome::Successful
            },
        },
    });

    match frame_object.get("calls") {
        None | Some(Value::Null) => {}
        Some(Value::Array(calls)) => {
            for (index, child) in calls.iter().enumerate() {
                let index = u32::try_from(index).map_err(|_| {
                    SyncError::InvalidResponse("trace has more than u32::MAX calls".into())
                })?;
                let mut child_path = path.to_vec();
                child_path.push(index);
                flatten_frame(
                    child,
                    block_number,
                    transaction_index,
                    &child_path,
                    reverted,
                    traces,
                )?;
            }
        }
        Some(_) => {
            return Err(SyncError::InvalidResponse(
                "trace.calls must be an array or null".into(),
            ))
        }
    }
    Ok(())
}

fn parse_call_type(value: &str) -> Result<CallType, SyncError> {
    match value.to_ascii_uppercase().as_str() {
        "CALL" => Ok(CallType::Call),
        "CALLCODE" => Ok(CallType::CallCode),
        "DELEGATECALL" => Ok(CallType::DelegateCall),
        "STATICCALL" => Ok(CallType::StaticCall),
        "CREATE" => Ok(CallType::Create),
        "CREATE2" => Ok(CallType::Create2),
        "SELFDESTRUCT" => Ok(CallType::SelfDestruct),
        other => Err(SyncError::InvalidResponse(format!(
            "unsupported call type {other:?}"
        ))),
    }
}

fn required<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    context: &str,
) -> Result<&'a Value, SyncError> {
    object
        .get(key)
        .ok_or_else(|| SyncError::InvalidResponse(format!("{context} is missing {key:?}")))
}

fn required_str<'a>(
    object: &'a Map<String, Value>,
    key: &str,
    context: &str,
) -> Result<&'a str, SyncError> {
    required(object, key, context)?
        .as_str()
        .ok_or_else(|| SyncError::InvalidResponse(format!("{context}.{key} must be a string")))
}
