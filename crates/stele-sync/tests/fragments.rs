use std::collections::BTreeMap;
use stele_sync::fragment::{
    adjacent_coverage, log_fragments, transaction_fragments, FactoryAddress, Fragment,
    FragmentAddress as A,
};
use stele_sync::interval::{BlockRange, BlockRangeSet};

#[test]
fn fragment_ids_and_adjacent_order() {
    let tx = Fragment::Transaction {
        chain_id: 1,
        from: A::Address("0xa".into()),
        to: A::Address("0xb".into()),
    };
    assert_eq!(
        tx.adjacent_ids(),
        [
            "transaction_1_0xa_0xb",
            "transaction_1_0xa_null",
            "transaction_1_null_0xb",
            "transaction_1_null_null"
        ]
    );
    let log = Fragment::Log {
        chain_id: 1,
        address: A::Address("0xa".into()),
        topic0: "0x0".into(),
        topics: [None, Some("0xe".into()), None],
        receipts: false,
    };
    assert_eq!(
        log.adjacent_ids(),
        [
            "log_1_0xa_0x0_null_0xe_null_0",
            "log_1_0xa_0x0_null_0xe_null_1",
            "log_1_0xa_0x0_null_null_null_0",
            "log_1_0xa_0x0_null_null_null_1",
            "log_1_null_0x0_null_0xe_null_0",
            "log_1_null_0x0_null_0xe_null_1",
            "log_1_null_0x0_null_null_null_0",
            "log_1_null_0x0_null_null_null_1",
        ]
    );
    let factory = FactoryAddress {
        address: None,
        event_selector: "0xe".into(),
        child_address_location: "topic1".into(),
        from_block: Some(0),
        to_block: None,
    };
    assert_eq!(
        Fragment::FactoryLog {
            chain_id: 1,
            factory: factory.clone()
        }
        .id(),
        "factory_log_1_null_0xe_topic1_0_null"
    );
    let trace = Fragment::Trace {
        chain_id: 1,
        from: A::Factory(factory),
        to: A::Any,
        function_selector: "0x1234".into(),
        receipts: true,
    };
    assert_eq!(
        trace.adjacent_ids(),
        ["trace_1_null_0xe_topic1_0_null_null_0x1234_1"]
    );
    assert_eq!(
        Fragment::Block {
            chain_id: 1,
            interval: 100,
            offset: 5
        }
        .id(),
        "block_1_100_5"
    );
    assert_eq!(
        Fragment::Transfer {
            chain_id: 1,
            from: A::Any,
            to: A::Any,
            receipts: false
        }
        .adjacent_ids(),
        ["transfer_1_null_null_0", "transfer_1_null_null_1"]
    );
}

#[test]
fn expansion_and_adjacent_coverage() {
    let addresses = [A::Address("0xa".into()), A::Address("0xb".into())];
    let choices = [Some("0x1".into()), Some("0x2".into())];
    let wildcard = [None];
    let logs = log_fragments(
        1,
        &addresses,
        "0x0",
        [&choices, &wildcard, &wildcard],
        false,
    );
    assert_eq!(logs.len(), 4);
    assert_eq!(transaction_fragments(1, &addresses, &[A::Any]).len(), 2);
    let mut stored = BTreeMap::new();
    stored.insert(
        logs[0].id(),
        BlockRangeSet::new(vec![BlockRange::new(1, 3).unwrap()]),
    );
    stored.insert(
        "log_1_null_0x0_null_null_null_1".into(),
        BlockRangeSet::new(vec![BlockRange::new(4, 6).unwrap()]),
    );
    assert_eq!(
        adjacent_coverage(&logs[0], &stored).ranges(),
        &[BlockRange::new(1, 6).unwrap()]
    );
}
