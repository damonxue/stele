//! Transport-independent JSON-RPC retrieval and an in-memory request cache.
//!
//! A transport must serialize requests, validate JSON-RPC version and response
//! ID, and surface provider errors. This module does not implement HTTP, retries,
//! rate limiting, MD5 database keys, or RPC result standardization.

use std::collections::BTreeMap;
use std::num::NonZeroU64;

use crate::interval::BlockRange;

/// Maximum number of addresses sent in one `eth_getLogs` filter, matching
/// The provider-compatibility limit.
pub const ADDRESS_CHUNK_SIZE: usize = 50;

/// JSON value exchanged with a transport. Number strings must be valid JSON
/// number literals; a transport is responsible for validation and encoding.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Json {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// A JSON number literal, not an EVM hex quantity.
    Number(String),
    /// JSON string.
    String(String),
    /// JSON array.
    Array(Vec<Json>),
    /// JSON object; keys are ordered for deterministic cache identity.
    Object(BTreeMap<String, Json>),
}

/// Method and parameters of a JSON-RPC request, excluding the transport ID.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Request {
    /// JSON-RPC method.
    pub method: String,
    /// Positional parameters.
    pub params: Vec<Json>,
}

/// A JSON-RPC transport. Implementations must reject malformed envelopes,
/// mismatched IDs, JSON-RPC errors, and transport failures rather than
/// returning them as successful values.
pub trait Transport {
    /// Transport/provider-specific failure.
    type Error;
    /// Sends one request and returns its decoded `result`.
    fn request(&mut self, request: &Request) -> Result<Json, Self::Error>;
}

/// Retrieval error, distinguishing transport failures and invalid results.
#[derive(Debug, Eq, PartialEq)]
pub enum Error<E> {
    /// Transport or provider failed.
    Transport(E),
    /// The method returned null when a non-null result was required.
    NotFound,
    /// The result did not have the expected JSON shape.
    InvalidResult(&'static str),
}

/// Explicit cache policy for a request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CachePolicy {
    /// Always call the transport; use for `latest`, `pending`, and unknown dependencies.
    Bypass,
    /// The caller asserts all chain-state dependencies are at or before this
    /// block. Reorg invalidation removes entries whose bound exceeds the ancestor.
    ThroughBlock(u64),
}

#[derive(Clone, Debug)]
struct Entry {
    through_block: u64,
    result: Json,
}

/// A chain-scoped in-memory cache. It is not the Postgres cache.
#[derive(Clone, Debug, Default)]
pub struct RequestCache {
    entries: BTreeMap<(u64, Request), Entry>,
}

impl RequestCache {
    /// Removes cached entries on `chain_id` potentially affected by a reorg.
    pub fn invalidate_after(&mut self, chain_id: u64, ancestor: u64) {
        self.entries
            .retain(|(chain, _), entry| *chain != chain_id || entry.through_block <= ancestor);
    }

    /// Clears all cached entries for a chain, including genesis-dependent data.
    pub fn clear_chain(&mut self, chain_id: u64) {
        self.entries.retain(|(chain, _), _| *chain != chain_id);
    }

    /// Number of entries across chains.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    /// Whether the cache has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// A transport and its in-memory request cache.
pub struct Rpc<T> {
    /// The caller-provided transport.
    pub transport: T,
    /// Cache, which can be invalidated when canonical history changes.
    pub cache: RequestCache,
}

impl<T: Transport> Rpc<T> {
    /// Creates a client with an empty cache.
    pub fn new(transport: T) -> Self {
        Self {
            transport,
            cache: RequestCache::default(),
        }
    }

    /// Fetches a result, running `validate` before caching and on cache hits.
    ///
    /// Caller must choose a correct policy and validation for the method. Null
    /// results are never cached. A different dependency bound cannot reuse an
    /// existing entry. This cache is not suitable for mutable provider results.
    pub fn request_checked(
        &mut self,
        chain_id: u64,
        request: Request,
        policy: CachePolicy,
        validate: impl Fn(&Json) -> Result<(), Error<T::Error>>,
    ) -> Result<Json, Error<T::Error>> {
        let key = (chain_id, request);
        if let CachePolicy::ThroughBlock(bound) = policy {
            if let Some(entry) = self.cache.entries.get(&key) {
                if entry.through_block == bound {
                    validate(&entry.result)?;
                    return Ok(entry.result.clone());
                }
            }
        }
        let result = self.transport.request(&key.1).map_err(Error::Transport)?;
        validate(&result)?;
        if let CachePolicy::ThroughBlock(through_block) = policy {
            if result != Json::Null {
                self.cache.entries.insert(
                    key,
                    Entry {
                        through_block,
                        result: result.clone(),
                    },
                );
            }
        }
        Ok(result)
    }

    /// Retrieves a numbered block using `eth_getBlockByNumber`.
    ///
    /// The result is checked for non-null object shape, not normalized or
    /// verified against the requested number/hash.
    pub fn block_by_number(
        &mut self,
        chain_id: u64,
        number: u64,
        full_transactions: bool,
    ) -> Result<Json, Error<T::Error>> {
        self.request_checked(
            chain_id,
            Request {
                method: "eth_getBlockByNumber".into(),
                params: vec![
                    Json::String(quantity(number)),
                    Json::Bool(full_transactions),
                ],
            },
            CachePolicy::ThroughBlock(number),
            |value| match value {
                Json::Null => Err(Error::NotFound),
                Json::Object(_) => Ok(()),
                _ => Err(Error::InvalidResult("expected block object")),
            },
        )
    }

    /// Fetches `eth_getLogs` in fixed inclusive chunks, splitting address
    /// arrays into at most 50 addresses.
    ///
    /// `None` addresses means unrestricted; an empty address list matches
    /// nothing and sends no RPC requests. Trailing null topics are removed.
    /// Returns only after all pages succeed; it does not mark intervals synced,
    /// sort or deduplicate logs, or adapt to provider limits.
    pub fn logs(
        &mut self,
        chain_id: u64,
        range: BlockRange,
        page_size: NonZeroU64,
        addresses: Option<&[String]>,
        topics: &[Json],
    ) -> Result<Vec<LogPage>, Error<T::Error>> {
        let mut topics = topics.to_vec();
        while topics.last() == Some(&Json::Null) {
            topics.pop();
        }
        let address_chunks: Vec<Option<&[String]>> = match addresses {
            None => vec![None],
            Some(values) => values.chunks(ADDRESS_CHUNK_SIZE).map(Some).collect(),
        };
        if address_chunks.is_empty() {
            return Ok(Vec::new());
        }
        let mut pages = Vec::new();
        for page in range.chunks(page_size) {
            let mut logs = Vec::new();
            for chunk in &address_chunks {
                let mut filter = BTreeMap::new();
                filter.insert("fromBlock".into(), Json::String(quantity(page.start())));
                filter.insert("toBlock".into(), Json::String(quantity(page.end())));
                filter.insert("topics".into(), Json::Array(topics.clone()));
                if let Some(addresses) = chunk {
                    filter.insert(
                        "address".into(),
                        Json::Array(addresses.iter().cloned().map(Json::String).collect()),
                    );
                }
                let result = self.request_checked(
                    chain_id,
                    Request {
                        method: "eth_getLogs".into(),
                        params: vec![Json::Object(filter)],
                    },
                    CachePolicy::ThroughBlock(page.end()),
                    |value| match value {
                        Json::Array(items)
                            if items.iter().all(|item| matches!(item, Json::Object(_))) =>
                        {
                            Ok(())
                        }
                        _ => Err(Error::InvalidResult("expected array of log objects")),
                    },
                )?;
                if let Json::Array(items) = result {
                    logs.extend(items);
                }
            }
            pages.push(LogPage { range: page, logs });
        }
        Ok(pages)
    }
}

/// Logs fetched for one inclusive page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogPage {
    /// Inclusive requested block bounds.
    pub range: BlockRange,
    /// Raw JSON log objects, concatenated in address-chunk order.
    pub logs: Vec<Json>,
}

/// Encodes an EVM JSON-RPC quantity in minimal lowercase hexadecimal.
pub fn quantity(value: u64) -> String {
    format!("0x{value:x}")
}
