//! Network-free tests for [`RpcClient`].

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use stele_sync::interval::BlockRange;
use stele_sync::testing::{MockCall, MockTransport};
use stele_sync::{RetryPolicy, RpcClient, SyncError};
use stele_types::{Address, CallType, Topic, TraceOutcome};

const BLOCK_NUMBER: u64 = 100;
const TIMESTAMP: u64 = 1_700_000_000;

fn hash(byte: &str) -> String {
    format!("0x{}", byte.repeat(32))
}

fn address(byte: &str) -> String {
    format!("0x{}", byte.repeat(20))
}

fn topic(byte: &str) -> Value {
    Value::String(hash(byte))
}

fn block_json() -> Value {
    json!({
        "number": "0x64",
        "hash": hash("11"),
        "parentHash": hash("22"),
        "timestamp": "0x6553f100",
        "transactions": [
            {
                "hash": hash("AA"),
                "transactionIndex": "0x0",
                "from": address("CC"),
                "to": address("DD"),
                "blockNumber": "0x64",
            },
            {
                "hash": hash("bb"),
                "transactionIndex": "0x1",
                "from": address("EE"),
                "to": Value::Null,
                "blockNumber": "0x64",
            },
        ],
    })
}

fn receipt_json(hash_value: &str) -> Value {
    match hash_value {
        value if value == hash("aa") => json!({
            "blockNumber": "0x64",
            "transactionIndex": "0x0",
            "from": address("CC"),
            "to": address("DD"),
            "status": "0x1",
        }),
        value if value == hash("bb") => json!({
            "blockNumber": "0x64",
            "transactionIndex": "0x1",
            "from": address("EE"),
            "to": Value::Null,
            "status": "0x0",
        }),
        other => json!({ "unexpected": other }),
    }
}

fn logs_json() -> Value {
    // Intentionally out of order; the client must sort by (tx index, log index).
    json!([
        {
            "blockNumber": "0x64",
            "transactionIndex": "0x1",
            "logIndex": "0x1",
            "address": address("EE"),
            "topics": [topic("33")],
            "data": "0x",
        },
        {
            "blockNumber": "0x64",
            "transactionIndex": "0x0",
            "logIndex": "0x0",
            "address": address("DD"),
            "topics": [topic("44"), topic("55")],
            "data": "0xdeadbeef",
        },
    ])
}

fn traces_json() -> Value {
    json!([
        {
            "txHash": hash("aa"),
            "result": {
                "type": "CALL",
                "from": address("CC"),
                "to": address("DD"),
                "value": "0x0",
                "input": "0x1234",
                "output": "0x",
                "calls": [
                    {
                        "type": "STATICCALL",
                        "from": address("DD"),
                        "to": address("AB"),
                        "input": "0x",
                        "output": "0x",
                    },
                    {
                        "type": "DELEGATECALL",
                        "from": address("DD"),
                        "to": address("AC"),
                        "input": "0x",
                        "output": "0x",
                        "error": "execution reverted",
                        "calls": [
                            {
                                "type": "CALL",
                                "from": address("AC"),
                                "to": address("AD"),
                                "input": "0x",
                                "output": "0x",
                            }
                        ],
                    },
                ],
            },
        },
        {
            "txHash": hash("bb"),
            "result": {
                "type": "CREATE",
                "from": address("EE"),
                "to": Value::Null,
                "value": "0x1",
                "input": "0x00",
            },
        },
    ])
}

fn block_data_client() -> (RpcClient, Arc<MockTransport>) {
    let transport = Arc::new(MockTransport::new(|_url, call: &MockCall| {
        match call.method.as_str() {
            "eth_getBlockByNumber" => Ok(block_json()),
            "eth_getTransactionReceipt" => {
                let hash = call.params[0]
                    .as_str()
                    .ok_or_else(|| SyncError::InvalidResponse("receipt hash is missing".into()))?;
                Ok(receipt_json(hash))
            }
            "eth_getLogs" => Ok(logs_json()),
            "debug_traceBlockByNumber" => Ok(traces_json()),
            other => Err(SyncError::InvalidResponse(format!(
                "unexpected method {other}"
            ))),
        }
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], transport.clone())
        .expect("client")
        .with_retry_policy(RetryPolicy::immediate());
    (client, transport)
}

#[tokio::test]
async fn block_data_assembles_transactions_receipts_logs_and_traces() {
    let (client, transport) = block_data_client();
    let data = client.block_data(BLOCK_NUMBER, true).await.unwrap();

    let block = data.block.expect("block header");
    assert_eq!(block.number, BLOCK_NUMBER);
    assert_eq!(block.hash.to_hex(), hash("11"));
    assert_eq!(block.parent_hash.to_hex(), hash("22"));
    assert_eq!(block.timestamp, TIMESTAMP);

    assert_eq!(data.transactions.len(), 2);
    assert_eq!(data.transactions[0].transaction_index, 0);
    assert_eq!(data.transactions[0].hash.to_hex(), hash("aa"));
    assert_eq!(
        data.transactions[0].from,
        Address::from_hex(&address("cc")).unwrap()
    );
    assert_eq!(
        data.transactions[0].to,
        Some(Address::from_hex(&address("dd")).unwrap())
    );
    assert_eq!(data.transactions[1].transaction_index, 1);
    assert_eq!(data.transactions[1].to, None);

    assert_eq!(data.receipts.len(), 2);
    assert!(data.receipts[0].success);
    assert!(!data.receipts[1].success);
    assert_eq!(data.receipts[1].transaction_index, 1);
    assert_eq!(data.receipts[1].to, None);

    assert_eq!(data.logs.len(), 2);
    assert_eq!(data.logs[0].transaction_index, 0);
    assert_eq!(data.logs[0].log_index, 0);
    assert_eq!(
        data.logs[0].log.address,
        Address::from_hex(&address("dd")).unwrap()
    );
    assert_eq!(data.logs[0].log.topics.len(), 2);
    assert_eq!(data.logs[0].log.data, vec![0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(data.logs[0].log.block_number, BLOCK_NUMBER);
    assert_eq!(data.logs[1].log_index, 1);

    // Pre-order flattening with path indices; the reverted frame and its child
    // are both Reverted.
    assert_eq!(data.traces.len(), 5);
    assert_eq!(data.traces[0].trace_address, Vec::<u32>::new());
    assert_eq!(data.traces[0].trace.call_type, CallType::Call);
    assert_eq!(data.traces[0].trace.outcome, TraceOutcome::Successful);
    assert_eq!(data.traces[0].trace.input, vec![0x12, 0x34]);
    assert_eq!(data.traces[1].trace_address, vec![0]);
    assert_eq!(data.traces[1].trace.call_type, CallType::StaticCall);
    assert_eq!(data.traces[1].trace.outcome, TraceOutcome::Successful);
    assert_eq!(data.traces[2].trace_address, vec![1]);
    assert_eq!(data.traces[2].trace.call_type, CallType::DelegateCall);
    assert_eq!(data.traces[2].trace.outcome, TraceOutcome::Reverted);
    assert_eq!(data.traces[3].trace_address, vec![1, 0]);
    assert_eq!(
        data.traces[3].trace.outcome,
        TraceOutcome::Reverted,
        "descendants of a reverted frame inherit the outcome"
    );
    assert_eq!(data.traces[4].transaction_index, 1);
    assert_eq!(data.traces[4].trace.call_type, CallType::Create);
    assert_eq!(data.traces[4].trace.to, None);
    let mut one = [0u8; 32];
    one[31] = 1;
    assert_eq!(data.traces[4].trace.value, Some(one));

    assert_eq!(transport.calls().len(), 5);
}

#[tokio::test]
async fn block_data_skips_traces_when_not_requested() {
    let (client, transport) = block_data_client();
    let data = client.block_data(BLOCK_NUMBER, false).await.unwrap();
    assert!(data.traces.is_empty());
    assert_eq!(
        transport
            .calls()
            .iter()
            .filter(|call| call.method == "debug_traceBlockByNumber")
            .count(),
        0
    );
}

#[tokio::test]
async fn logs_chunk_ranges_addresses_and_topics() {
    let transport = Arc::new(MockTransport::new(|_url, _call| {
        Ok(Value::Array(Vec::new()))
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], transport.clone())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());

    let addresses: Vec<Address> = (0..51u8).map(|index| Address([index; 20])).collect();
    let topics = vec![
        vec![Topic([1; 32]), Topic([2; 32])],
        Vec::new(),
        vec![Topic([3; 32])],
        Vec::new(),
    ];
    let range = BlockRange::new(1, 1_200).unwrap();
    let logs = client.logs(range, Some(&addresses), &topics).await.unwrap();
    assert!(logs.is_empty());

    let calls = transport.calls();
    assert_eq!(calls.len(), 6, "3 range chunks x 2 address chunks");

    let filter = calls[0].params[0].as_object().unwrap();
    assert_eq!(filter["fromBlock"], json!("0x1"));
    assert_eq!(filter["toBlock"], json!("0x1f4"));
    assert_eq!(
        filter["topics"],
        json!([[hash("01"), hash("02")], Value::Null, [hash("03")]]),
        "OR lists per position, wildcards preserved, trailing wildcards removed"
    );
    assert_eq!(filter["address"].as_array().unwrap().len(), 50);
    assert_eq!(filter["address"][0], json!(Address([0u8; 20]).to_hex()));

    let last_filter = calls[5].params[0].as_object().unwrap();
    assert_eq!(last_filter["fromBlock"], json!("0x3e9"));
    assert_eq!(last_filter["toBlock"], json!("0x4b0"));
    assert_eq!(last_filter["address"].as_array().unwrap().len(), 1);

    // No address restriction means a single filter per range chunk.
    client.logs(range, None, &[]).await.unwrap();
    let calls = transport.calls();
    assert_eq!(calls.len(), 9);
    assert!(calls[6].params[0].get("address").is_none());

    // An empty address list matches nothing and sends no request.
    client.logs(range, Some(&[]), &topics).await.unwrap();
    assert_eq!(transport.calls().len(), 9);
}

#[tokio::test]
async fn logs_are_sorted_and_validated_against_the_requested_range() {
    let transport = Arc::new(MockTransport::new(|_url, call| {
        let from = call.params[0]["fromBlock"].as_str().unwrap();
        if from == "0x1" {
            // Out of order on purpose.
            Ok(json!([
                {
                    "blockNumber": "0x2",
                    "transactionIndex": "0x0",
                    "logIndex": "0x5",
                    "address": address("01"),
                    "topics": [],
                    "data": "0x",
                },
                {
                    "blockNumber": "0x1",
                    "transactionIndex": "0x3",
                    "logIndex": "0x1",
                    "address": address("02"),
                    "topics": [topic("09")],
                    "data": "0x01",
                },
            ]))
        } else {
            Ok(Value::Array(Vec::new()))
        }
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], transport.clone())
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());
    let logs = client
        .logs(BlockRange::new(1, 2).unwrap(), None, &[])
        .await
        .unwrap();
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].log.block_number, 1);
    assert_eq!(logs[1].log.block_number, 2);
    assert_eq!(logs[1].log_index, 5);

    // A log outside the requested range is a provider bug.
    let transport = Arc::new(MockTransport::new(|_url, _call| {
        Ok(json!([{
            "blockNumber": "0x2",
            "transactionIndex": "0x0",
            "logIndex": "0x0",
            "address": address("01"),
            "topics": [],
            "data": "0x",
        }]))
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], transport)
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());
    assert!(matches!(
        client.logs(BlockRange::new(1, 1).unwrap(), None, &[]).await,
        Err(SyncError::InvalidResponse(_))
    ));
}

#[tokio::test]
async fn failover_prefers_the_healthy_endpoint_and_reports_exhaustion() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let counter = attempts.clone();
    let transport = Arc::new(MockTransport::new(move |url, _call| {
        counter.fetch_add(1, Ordering::SeqCst);
        if url == "http://a" {
            Err(SyncError::Transport("down".into()))
        } else {
            Ok(json!("0x1"))
        }
    }));
    let client = RpcClient::with_transport(
        vec!["http://a".into(), "http://b".into()],
        transport.clone(),
    )
    .unwrap()
    .with_retry_policy(RetryPolicy::immediate());

    assert_eq!(client.chain_id().await.unwrap(), 1);
    assert_eq!(transport.calls().len(), 2);

    // The healthy endpoint stays preferred.
    assert_eq!(client.chain_id().await.unwrap(), 1);
    let calls = transport.calls();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[2].url, "http://b");

    let failing = Arc::new(MockTransport::new(|_url, _call| {
        Err(SyncError::Transport("down".into()))
    }));
    let client = RpcClient::with_transport(vec!["http://a".into()], failing.clone())
        .unwrap()
        .with_retry_policy(RetryPolicy {
            max_retries: 2,
            ..RetryPolicy::immediate()
        });
    assert!(matches!(
        client.latest_block_number().await,
        Err(SyncError::Transport(_))
    ));
    assert_eq!(failing.calls().len(), 3, "one attempt plus two retries");
}

#[tokio::test]
async fn rpc_errors_and_malformed_responses_become_sync_errors() {
    let transport = Arc::new(MockTransport::new_raw(|_url, call| {
        Ok(json!({
            "jsonrpc": "2.0",
            "id": call.id,
            "error": { "code": -32000, "message": "boom" },
        }))
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], transport)
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());
    match client.chain_id().await {
        Err(SyncError::Rpc { code, message }) => {
            assert_eq!(code, -32000);
            assert_eq!(message, "boom");
        }
        other => panic!("expected rpc error, got {other:?}"),
    }

    let missing_hash = Arc::new(MockTransport::new(|_url, _call| {
        Ok(json!({ "number": "0x1", "parentHash": hash("22"), "timestamp": "0x1" }))
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], missing_hash)
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());
    assert!(matches!(
        client.block_by_number(1).await,
        Err(SyncError::InvalidResponse(_))
    ));

    let wrong_number = Arc::new(MockTransport::new(|_url, _call| {
        let mut block = block_json();
        block["number"] = json!("0x65");
        Ok(block)
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], wrong_number)
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());
    assert!(matches!(
        client.block_by_number(1).await,
        Err(SyncError::InvalidResponse(_))
    ));

    let null_block = Arc::new(MockTransport::new(|_url, _call| Ok(Value::Null)));
    let client = RpcClient::with_transport(vec!["http://mock".into()], null_block)
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());
    assert!(matches!(
        client.block_by_number(7).await,
        Err(SyncError::NotFound(_))
    ));
}

#[tokio::test]
async fn block_by_hash_checks_the_returned_hash() {
    let transport = Arc::new(MockTransport::new(|_url, _call| {
        let mut block = block_json();
        block["hash"] = json!(hash("99"));
        Ok(block)
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], transport)
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());
    let requested = stele_types::B256([0x11; 32]);
    assert!(matches!(
        client.block_by_hash(requested).await,
        Err(SyncError::InvalidResponse(_))
    ));
}

#[tokio::test]
async fn duplicate_logs_are_rejected() {
    let entry = json!({
        "blockNumber": "0x1",
        "transactionIndex": "0x0",
        "logIndex": "0x0",
        "address": address("01"),
        "topics": [],
        "data": "0x",
    });
    let transport = Arc::new(MockTransport::new(move |_url, _call| {
        Ok(Value::Array(vec![entry.clone(), entry.clone()]))
    }));
    let client = RpcClient::with_transport(vec!["http://mock".into()], transport)
        .unwrap()
        .with_retry_policy(RetryPolicy::immediate());
    assert!(matches!(
        client.logs(BlockRange::new(1, 1).unwrap(), None, &[]).await,
        Err(SyncError::InvalidResponse(_))
    ));
}

#[tokio::test]
async fn urls_must_be_http() {
    let transport = Arc::new(MockTransport::new(|_url, _call| Ok(json!("0x1"))));
    assert!(matches!(
        RpcClient::with_transport(Vec::new(), transport.clone()),
        Err(SyncError::InvalidUrl(_))
    ));
    assert!(matches!(
        RpcClient::with_transport(vec!["ftp://example.com".into()], transport),
        Err(SyncError::InvalidUrl(_))
    ));
}
