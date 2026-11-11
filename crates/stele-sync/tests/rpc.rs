use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroU64;
use stele_sync::interval::BlockRange;
use stele_sync::rpc::{CachePolicy, Error, Json, Request, Rpc, Transport};

#[derive(Default)]
struct Mock {
    calls: Vec<Request>,
    responses: VecDeque<Result<Json, &'static str>>,
}
impl Transport for Mock {
    type Error = &'static str;
    fn request(&mut self, request: &Request) -> Result<Json, Self::Error> {
        self.calls.push(request.clone());
        self.responses.pop_front().expect("unexpected request")
    }
}
fn object() -> Json {
    Json::Object(BTreeMap::new())
}
fn array() -> Json {
    Json::Array(vec![object()])
}

#[test]
fn cache_is_chain_scoped_and_reorg_invalidated() {
    let mut rpc = Rpc::new(Mock {
        responses: vec![Ok(object()), Ok(object()), Ok(object()), Ok(object())].into(),
        ..Mock::default()
    });
    rpc.block_by_number(1, 10, false).unwrap();
    rpc.block_by_number(1, 10, false).unwrap();
    rpc.block_by_number(2, 10, false).unwrap();
    assert_eq!(rpc.transport.calls.len(), 2);
    rpc.cache.invalidate_after(1, 9);
    assert_eq!(rpc.cache.len(), 1);
    rpc.block_by_number(1, 10, false).unwrap();
    rpc.cache.invalidate_after(1, 10);
    rpc.block_by_number(1, 10, false).unwrap();
    assert_eq!(rpc.transport.calls.len(), 3);
    rpc.cache.clear_chain(2);
    rpc.block_by_number(2, 10, false).unwrap();
    assert_eq!(rpc.transport.calls.len(), 4);
    assert_eq!(
        rpc.transport.calls[0].params,
        vec![Json::String("0xa".into()), Json::Bool(false)]
    );
}

#[test]
fn errors_and_bad_shapes_are_not_cached() {
    let mut rpc = Rpc::new(Mock {
        responses: vec![
            Ok(Json::Null),
            Ok(Json::Bool(false)),
            Err("timeout"),
            Ok(object()),
        ]
        .into(),
        ..Mock::default()
    });
    assert_eq!(rpc.block_by_number(1, 0, true), Err(Error::NotFound));
    assert_eq!(
        rpc.block_by_number(1, 0, true),
        Err(Error::InvalidResult("expected block object"))
    );
    assert_eq!(
        rpc.block_by_number(1, 0, true),
        Err(Error::Transport("timeout"))
    );
    assert!(rpc.cache.is_empty());
    rpc.block_by_number(1, 0, true).unwrap();
    assert_eq!(rpc.cache.len(), 1);
}

#[test]
fn bypass_and_bound_changes_do_not_reuse_cached_results() {
    let mut rpc = Rpc::new(Mock {
        responses: vec![
            Ok(Json::String("a".into())),
            Ok(Json::String("b".into())),
            Ok(Json::String("c".into())),
        ]
        .into(),
        ..Mock::default()
    });
    let request = Request {
        method: "custom".into(),
        params: vec![],
    };
    let fetch = |rpc: &mut Rpc<Mock>, policy| {
        rpc.request_checked(1, request.clone(), policy, |_| Ok(()))
            .unwrap()
    };
    assert_eq!(
        fetch(&mut rpc, CachePolicy::ThroughBlock(4)),
        Json::String("a".into())
    );
    assert_eq!(
        fetch(&mut rpc, CachePolicy::Bypass),
        Json::String("b".into())
    );
    assert_eq!(
        fetch(&mut rpc, CachePolicy::ThroughBlock(5)),
        Json::String("c".into())
    );
    assert_eq!(rpc.transport.calls.len(), 3);
}

#[test]
fn log_pagination_address_chunking_and_cache() {
    let mut rpc = Rpc::new(Mock {
        responses: vec![Ok(array()), Ok(array()), Ok(array()), Ok(array())].into(),
        ..Mock::default()
    });
    let addresses: Vec<_> = (0..51).map(|i| format!("0x{i:x}")).collect();
    let range = BlockRange::new(4, 6).unwrap();
    let size = NonZeroU64::new(2).unwrap();
    let topics = [Json::String("0xaa".into()), Json::Null, Json::Null];
    let pages = rpc.logs(1, range, size, Some(&addresses), &topics).unwrap();
    assert_eq!(pages.len(), 2);
    assert_eq!(pages[0].range, BlockRange::new(4, 5).unwrap());
    assert_eq!(pages[1].range, BlockRange::new(6, 6).unwrap());
    assert_eq!(pages[0].logs.len(), 2);
    assert_eq!(rpc.transport.calls.len(), 4);
    let Json::Object(filter) = &rpc.transport.calls[0].params[0] else {
        panic!("filter")
    };
    assert_eq!(filter["fromBlock"], Json::String("0x4".into()));
    assert_eq!(filter["toBlock"], Json::String("0x5".into()));
    assert_eq!(
        filter["topics"],
        Json::Array(vec![Json::String("0xaa".into())])
    );
    assert!(matches!(&filter["address"], Json::Array(a) if a.len() == 50));
    let Json::Object(filter) = &rpc.transport.calls[1].params[0] else {
        panic!("filter")
    };
    assert!(matches!(&filter["address"], Json::Array(a) if a.len() == 1));
    assert_eq!(
        rpc.logs(1, range, size, Some(&addresses), &topics).unwrap(),
        pages
    );
    assert_eq!(rpc.transport.calls.len(), 4);
    assert!(rpc
        .logs(1, range, size, Some(&[]), &topics)
        .unwrap()
        .is_empty());
}

#[test]
fn failed_log_page_is_not_reported_complete() {
    let mut rpc = Rpc::new(Mock {
        responses: vec![Ok(array()), Ok(Json::Null), Ok(array())].into(),
        ..Mock::default()
    });
    let range = BlockRange::new(1, 2).unwrap();
    let size = NonZeroU64::new(1).unwrap();
    assert_eq!(
        rpc.logs(1, range, size, None, &[]),
        Err(Error::InvalidResult("expected array of log objects"))
    );
    assert_eq!(rpc.cache.len(), 1);
    assert_eq!(rpc.logs(1, range, size, None, &[]).unwrap().len(), 2);
    assert_eq!(rpc.transport.calls.len(), 3);
}
