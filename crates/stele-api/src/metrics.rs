//! Process-wide counters rendered at `GET /metrics`.
//!
//! Values live in atomics so recording is cheap on request paths. They are not
//! persisted: a restart resets them, which is the expected behaviour for
//! Prometheus counters scraped from a process. The embedder reports indexing
//! facts it owns through [`record_reorg`] and [`set_last_indexed_block`];
//! `/status` also refreshes the per-chain gauge from committed progress.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, OnceLock,
    },
};

use axum::{
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
};

static QUERIES_TOTAL: AtomicU64 = AtomicU64::new(0);
static ERRORS_TOTAL: AtomicU64 = AtomicU64::new(0);
static REORGS_TOTAL: AtomicU64 = AtomicU64::new(0);
static LAST_BLOCKS: OnceLock<Mutex<BTreeMap<u64, u64>>> = OnceLock::new();

/// Count one executed query.
///
/// Called for `/sql`, each `/sql/live` tick, and each table resolution inside a
/// `/graphql` request.
pub fn record_query() {
    QUERIES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Count one request answered with a structured API error.
pub fn record_error() {
    ERRORS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Count one detected chain reorganization.
pub fn record_reorg() {
    REORGS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Record the last indexed block of `chain_id`.
pub fn set_last_indexed_block(chain_id: u64, block_number: u64) {
    let blocks = LAST_BLOCKS.get_or_init(|| Mutex::new(BTreeMap::new()));
    if let Ok(mut blocks) = blocks.lock() {
        blocks.insert(chain_id, block_number);
    }
}

/// Serialize every counter in Prometheus text exposition format.
pub fn render() -> String {
    let mut output = String::with_capacity(512);
    output.push_str(
        "# HELP stele_api_queries_total Queries executed by the API.\n\
         # TYPE stele_api_queries_total counter\n",
    );
    output.push_str(&format!(
        "stele_api_queries_total {}\n",
        QUERIES_TOTAL.load(Ordering::Relaxed)
    ));
    output.push_str(
        "# HELP stele_api_errors_total Requests answered with a structured API error.\n\
         # TYPE stele_api_errors_total counter\n",
    );
    output.push_str(&format!(
        "stele_api_errors_total {}\n",
        ERRORS_TOTAL.load(Ordering::Relaxed)
    ));
    output.push_str(
        "# HELP stele_reorg_total Chain reorganizations observed by the indexer.\n\
         # TYPE stele_reorg_total counter\n",
    );
    output.push_str(&format!(
        "stele_reorg_total {}\n",
        REORGS_TOTAL.load(Ordering::Relaxed)
    ));
    output.push_str(
        "# HELP stele_indexing_last_block Last indexed block per chain.\n\
         # TYPE stele_indexing_last_block gauge\n",
    );
    if let Some(blocks) = LAST_BLOCKS.get() {
        if let Ok(blocks) = blocks.lock() {
            for (chain_id, block_number) in blocks.iter() {
                output.push_str(&format!(
                    "stele_indexing_last_block{{chain=\"{chain_id}\"}} {block_number}\n"
                ));
            }
        }
    }
    output
}

/// `GET /metrics` handler.
pub(crate) async fn metrics() -> Response {
    (
        [(CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        render(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn text_shape_is_valid_prometheus() {
        let text = render();
        for name in [
            "stele_api_queries_total",
            "stele_api_errors_total",
            "stele_reorg_total",
            "stele_indexing_last_block",
        ] {
            assert!(
                text.contains(&format!("# HELP {name} ")),
                "missing HELP for {name}"
            );
            assert!(
                text.contains(&format!("# TYPE {name} ")),
                "missing TYPE for {name}"
            );
        }
        assert!(text.lines().any(|line| line
            .strip_prefix("stele_api_queries_total ")
            .is_some_and(|value| value.parse::<u64>().is_ok())));
        assert!(text.ends_with('\n'));
    }

    #[test]
    fn last_indexed_block_is_exported_per_chain() {
        set_last_indexed_block(424_242, 1_000);
        let text = render();
        assert!(
            text.contains("stele_indexing_last_block{chain=\"424242\"} 1000"),
            "{text}"
        );
    }

    #[test]
    fn counters_only_move_forward() {
        let before = QUERIES_TOTAL.load(Ordering::Relaxed);
        record_query();
        assert!(QUERIES_TOTAL.load(Ordering::Relaxed) > before);
        record_error();
        record_reorg();
        assert!(ERRORS_TOTAL.load(Ordering::Relaxed) > 0);
        assert!(REORGS_TOTAL.load(Ordering::Relaxed) > 0);
    }
}
