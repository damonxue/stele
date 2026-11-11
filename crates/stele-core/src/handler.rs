//! Handler registration and sequential dispatch.
//!
//! Handlers are boxed closures so the runtime can store user functions without
//! generics leaking into [`crate::engine::Engine`]. Registration keys follow
//! this convention:
//!
//! - `<source>:setup` for setup handlers;
//! - `<source>:<event>` for event handlers, where `<event>` is the ABI event
//!   name, `block`, or `transaction:from`/`transfer:to` for non-contract
//!   sources.
//!
//! Dispatch is sequential and stops at the first error. Registering a key twice
//! returns [`DuplicateHandler`] and keeps the original handler.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;

use serde_json::Value;
use stele_checkpoint::Checkpoint;
use stele_store::IndexingStore;
use stele_types::{Address, BlockRef, LogRef, ReceiptRef, TraceRef, TransactionRef};
use thiserror::Error;

use crate::DuplicateHandler;
use crate::indexing::InvocationError;

/// A handler future may borrow the context and the decoded event.
pub type HandlerFuture<'a> = Pin<Box<dyn Future<Output = Result<(), HandlerError>> + Send + 'a>>;

type SetupHandler = dyn for<'a, 'b> Fn(&'a EventContext<'b>) -> HandlerFuture<'a> + Send + Sync;
type EventHandler =
    dyn for<'a, 'b> Fn(&'a EventContext<'b>, &'a DecodedEvent) -> HandlerFuture<'a> + Send + Sync;

enum Handler {
    Setup(Box<SetupHandler>),
    Event(Box<EventHandler>),
}

impl Handler {
    fn is_setup(&self) -> bool {
        matches!(self, Self::Setup(_))
    }
}

/// A handler failure, classified by how the engine should react.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum HandlerError {
    /// Bad input or an unrecoverable application error. Indexing stops.
    #[error("user error: {0}")]
    User(String),
    /// A recoverable failure such as a rate-limited RPC call. The engine
    /// retries the current block with backoff.
    #[error("transient error: {0}")]
    Transient(String),
    /// An internal invariant was violated. Indexing stops.
    #[error("fatal error: {0}")]
    Fatal(String),
}

/// Runtime services available to a handler.
///
/// `store` is the open [`stele_store::StoreBatch`] for the current block, so
/// every write made through it commits or rolls back with that block.
#[derive(Clone, Copy)]
pub struct EventContext<'a> {
    pub store: &'a dyn IndexingStore,
    pub chain_id: u64,
    pub block: &'a BlockRef,
    pub checkpoint: &'a Checkpoint,
    /// Contract addresses available to handlers, keyed by source name.
    pub contracts: &'a BTreeMap<String, Address>,
}

/// One decoded payload passed to an event handler.
///
/// Fields mirror the event object handlers receive. `transaction`, `receipt`, `trace` and
/// `log` are populated depending on the trigger: block events carry none,
/// transaction events carry `transaction` and `receipt`, log events carry
/// `log` plus a transaction when available, and trace/transfer events carry
/// `trace`, `transaction` and optionally `receipt`.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedEvent {
    /// Handler key, for example `Token:Transfer`.
    pub name: String,
    /// Source identifier, for example `Token` or `Alice`.
    pub source_id: String,
    pub chain_id: u64,
    pub checkpoint: Checkpoint,
    pub block: BlockRef,
    pub transaction: Option<TransactionRef>,
    pub receipt: Option<ReceiptRef>,
    pub trace: Option<TraceRef>,
    pub log: Option<LogRef>,
    /// Decoded event arguments, or [`Value::Null`] for sources without an ABI.
    pub args: Value,
    /// Decoded call return value, or [`Value::Null`].
    pub result: Value,
}

/// A registration store for setup and event handlers.
///
/// The set is shared behind an [`Arc`] by the engine. It is safe to build it
/// before starting the runtime and then read it concurrently: registration
/// takes `&mut self`, dispatch takes `&self`.
#[derive(Default)]
pub struct HandlerSet {
    handlers: HashMap<String, Handler>,
    /// Registration order, used by [`HandlerSet::run_setup`].
    order: Vec<String>,
}

impl HandlerSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the setup handler for `key`, normally `<source>:setup`.
    pub fn register_setup<F>(&mut self, key: String, handler: F) -> Result<(), DuplicateHandler>
    where
        F: for<'a> Fn(&'a EventContext<'_>) -> HandlerFuture<'a> + Send + Sync + 'static,
    {
        self.register(key, Handler::Setup(Box::new(handler)))
    }

    /// Registers the event handler for `key`, normally `<source>:<event>`.
    pub fn register_event<F>(&mut self, key: String, handler: F) -> Result<(), DuplicateHandler>
    where
        F: for<'a> Fn(&'a EventContext<'_>, &'a DecodedEvent) -> HandlerFuture<'a>
            + Send
            + Sync
            + 'static,
    {
        self.register(key, Handler::Event(Box::new(handler)))
    }

    fn register(&mut self, key: String, handler: Handler) -> Result<(), DuplicateHandler> {
        if self.handlers.contains_key(&key) {
            return Err(DuplicateHandler(key));
        }
        self.order.push(key.clone());
        self.handlers.insert(key, handler);
        Ok(())
    }

    /// Returns `true` when a setup handler is registered for `key`.
    pub fn has_setup(&self, key: &str) -> bool {
        self.handlers.get(key).is_some_and(Handler::is_setup)
    }

    /// Returns `true` when an event handler is registered for `key`.
    pub fn has_event(&self, key: &str) -> bool {
        self.handlers
            .get(key)
            .is_some_and(|handler| !handler.is_setup())
    }

    /// Runs every registered setup handler in registration order.
    ///
    /// Use this when a handler set belongs to a single chain. For multi-chain
    /// runs the engine calls [`HandlerSet::run_setup_for`] once per source so a
    /// setup handler only runs on its own chain.
    pub async fn run_setup(
        &self,
        context: &EventContext<'_>,
    ) -> Result<(), InvocationError<HandlerError>> {
        for key in self.order.iter().filter(|key| self.has_setup(key)) {
            self.invoke_setup(key, context).await?;
        }
        Ok(())
    }

    /// Runs the setup handler registered for `key`.
    ///
    /// Returns [`InvocationError::UnknownHandler`] when `key` is not
    /// registered.
    pub async fn run_setup_for(
        &self,
        key: &str,
        context: &EventContext<'_>,
    ) -> Result<(), InvocationError<HandlerError>> {
        self.invoke_setup(key, context).await
    }

    /// Runs the event handler named by `event.name`.
    pub async fn run_event(
        &self,
        context: &EventContext<'_>,
        event: &DecodedEvent,
    ) -> Result<(), InvocationError<HandlerError>> {
        let Some(handler) = self.handlers.get(&event.name) else {
            return Err(InvocationError::UnknownHandler(event.name.clone()));
        };
        let Handler::Event(handler) = handler else {
            return Err(InvocationError::WrongHandlerKind(event.name.clone()));
        };
        handler(context, event)
            .await
            .map_err(|source| InvocationError::Handler {
                name: event.name.clone(),
                source,
            })
    }

    async fn invoke_setup(
        &self,
        key: &str,
        context: &EventContext<'_>,
    ) -> Result<(), InvocationError<HandlerError>> {
        let Some(handler) = self.handlers.get(key) else {
            return Err(InvocationError::UnknownHandler(key.to_owned()));
        };
        let Handler::Setup(handler) = handler else {
            return Err(InvocationError::WrongHandlerKind(key.to_owned()));
        };
        handler(context)
            .await
            .map_err(|source| InvocationError::Handler {
                name: key.to_owned(),
                source,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use stele_schema::SchemaDef;
    use stele_store::{MemoryStore, Store};

    fn context<'a>(
        store: &'a dyn IndexingStore,
        block: &'a BlockRef,
        checkpoint: &'a Checkpoint,
        contracts: &'a BTreeMap<String, Address>,
    ) -> EventContext<'a> {
        EventContext {
            store,
            chain_id: 1,
            block,
            checkpoint,
            contracts,
        }
    }

    fn event(name: &str) -> DecodedEvent {
        DecodedEvent {
            name: name.to_owned(),
            source_id: "Token".to_owned(),
            chain_id: 1,
            checkpoint: Checkpoint::block_end(10, 1, 1),
            block: BlockRef {
                number: 1,
                hash: Default::default(),
                parent_hash: Default::default(),
                timestamp: 10,
            },
            transaction: None,
            receipt: None,
            trace: None,
            log: None,
            args: Value::Null,
            result: Value::Null,
        }
    }

    #[tokio::test]
    async fn dispatch_is_sequential_and_propagates_errors() {
        let store = MemoryStore::new(SchemaDef::default());
        let block = BlockRef {
            number: 1,
            hash: Default::default(),
            parent_hash: Default::default(),
            timestamp: 10,
        };
        let checkpoint = Checkpoint::block_end(10, 1, 1);
        let contracts = BTreeMap::new();
        let batch = store.begin(1, checkpoint).await.unwrap();
        let context = context(&*batch, &block, &checkpoint, &contracts);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut handlers = HandlerSet::new();

        for (name, record) in [("Token:A", "a"), ("Token:B", "b")] {
            let seen = Arc::clone(&seen);
            handlers
                .register_event(name.to_owned(), move |_, _| {
                    seen.lock().unwrap().push(record);
                    Box::pin(async { Ok(()) })
                })
                .unwrap();
        }

        handlers
            .run_event(&context, &event("Token:A"))
            .await
            .unwrap();
        handlers
            .run_event(&context, &event("Token:B"))
            .await
            .unwrap();
        assert_eq!(*seen.lock().unwrap(), ["a", "b"]);

        let mut failing = HandlerSet::new();
        failing
            .register_event("Token:A".to_owned(), |_, _| {
                Box::pin(async { Err(HandlerError::User("stop".to_owned())) })
            })
            .unwrap();
        assert_eq!(
            failing.run_event(&context, &event("Token:A")).await,
            Err(InvocationError::Handler {
                name: "Token:A".to_owned(),
                source: HandlerError::User("stop".to_owned()),
            })
        );
    }

    #[tokio::test]
    async fn duplicates_and_wrong_kinds_are_rejected() {
        let mut handlers = HandlerSet::new();
        handlers
            .register_setup("Token:setup".to_owned(), |_| Box::pin(async { Ok(()) }))
            .unwrap();
        assert_eq!(
            handlers.register_event("Token:setup".to_owned(), |_, _| Box::pin(async { Ok(()) })),
            Err(DuplicateHandler("Token:setup".to_owned()))
        );
        assert!(handlers.has_setup("Token:setup"));
        assert!(!handlers.has_event("Token:setup"));

        let store = MemoryStore::new(SchemaDef::default());
        let block = BlockRef {
            number: 1,
            hash: Default::default(),
            parent_hash: Default::default(),
            timestamp: 10,
        };
        let checkpoint = Checkpoint::block_end(10, 1, 1);
        let contracts = BTreeMap::new();
        let batch = store.begin(1, checkpoint).await.unwrap();
        let context = context(&*batch, &block, &checkpoint, &contracts);
        assert_eq!(
            handlers.run_event(&context, &event("Token:setup")).await,
            Err(InvocationError::WrongHandlerKind("Token:setup".to_owned()))
        );
        assert_eq!(
            handlers.run_setup_for("Token:missing", &context).await,
            Err(InvocationError::UnknownHandler("Token:missing".to_owned()))
        );
    }

    #[tokio::test]
    async fn setup_handlers_run_in_registration_order() {
        let store = MemoryStore::new(SchemaDef::default());
        let block = BlockRef {
            number: 1,
            hash: Default::default(),
            parent_hash: Default::default(),
            timestamp: 10,
        };
        let checkpoint = Checkpoint::block_start(10, 1, 1);
        let contracts = BTreeMap::new();
        let batch = store.begin(1, checkpoint).await.unwrap();
        let context = context(&*batch, &block, &checkpoint, &contracts);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut handlers = HandlerSet::new();
        for name in ["A:setup", "B:setup"] {
            let calls = Arc::clone(&calls);
            handlers
                .register_setup(name.to_owned(), move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(()) })
                })
                .unwrap();
        }
        handlers.run_setup(&context).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(store.progress(1).await.unwrap().is_none());
    }
}
