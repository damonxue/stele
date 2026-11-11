//! Facade crate for stele.
//!
//! A Rust indexing application builds a [`SteleBuilder`], registers handlers
//! with [`SteleBuilder::on`] and [`SteleBuilder::setup`], then awaits
//! [`SteleBuilder::run`]. The builder owns the wiring that the TypeScript
//! runtime performs implicitly: database connection, schema
//! initialization, per-chain RPC clients, plan construction, the indexing
//! engine and, optionally, the HTTP API.
//!
//! ```no_run
//! use stele::{Address, SteleBuilder};
//!
//! # async fn run() -> Result<(), stele::SteleError> {
//! let config = stele::Config::from_file("stele.toml")?;
//! SteleBuilder::new()
//!     .config(config)
//!     .on("Token:Transfer", |ctx, event| {
//!         Box::pin(async move {
//!             let _ = (ctx, event);
//!             Ok(())
//!         })
//!     })
//!     .run()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use sqlx::postgres::PgPoolOptions;
use stele_api::{ApiConfig, ApiState};
use stele_core::{Engine, HandlerFuture};
use stele_sync::RpcClient;
use thiserror::Error;

pub use stele_checkpoint::Checkpoint;
pub use stele_config::{
    AbiSource, AccountConfig, BlockConfig, ChainConfig, Config, ContractConfig, DatabaseConfig,
    FactoryConfig, FactoryParameter, OrderingMode,
};
pub use stele_core::events::Filter;
pub use stele_core::filter::AddressFilter;
pub use stele_core::{
    CallbackPlan, ChainPlan, DecodedEvent, EngineError, EngineOptions, EventContext, FactoryPlan,
    HandlerError, HandlerSet, Metrics, PlanBuild, build_plans, decode::DecodeKind,
};
pub use stele_macros::OnchainTable;
pub use stele_schema::{
    ColumnDef, ColumnType, EnumDef, IndexDef, ScalarType, SchemaDef, TableDef, ViewDef,
    quote_identifier,
};
pub use stele_store::{
    CellValue, IndexingStore, MemoryStore, PostgresStore, Progress, Row, Store, StoreBatch,
    TableRef,
};
pub use stele_types::{
    Address, B256, BlockRef, Log, LogRef, ReceiptRef, Topic, Trace, TransactionRef,
};

/// A fatal facade error. Indexing errors keep their classification so callers
/// can distinguish user mistakes from transient infrastructure failures.
#[derive(Debug, Error)]
pub enum SteleError {
    #[error(transparent)]
    Config(#[from] stele_config::ConfigError),
    #[error(transparent)]
    Store(#[from] stele_store::StoreError),
    #[error(transparent)]
    Engine(#[from] stele_core::EngineError),
    #[error(transparent)]
    Sync(#[from] stele_sync::SyncError),
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("invalid configuration: {0}")]
    Invalid(String),
    #[error("http server error: {0}")]
    Http(String),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<&'static str> for SteleError {
    fn from(value: &'static str) -> Self {
        Self::Invalid(value.to_owned())
    }
}

/// HTTP API settings for [`SteleBuilder::api`].
#[derive(Clone, Debug)]
pub struct ApiOptions {
    pub host: String,
    pub port: u16,
}

impl Default for ApiOptions {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_owned(),
            port: 42069,
        }
    }
}

/// Builder for a stele application.
pub struct SteleBuilder {
    config: Option<Config>,
    schema: SchemaDef,
    handlers: HandlerSet,
    options: EngineOptions,
    api: Option<ApiOptions>,
    api_router: Option<Router>,
    registration_error: Option<String>,
}

impl Default for SteleBuilder {
    fn default() -> Self {
        Self {
            config: None,
            schema: SchemaDef::default(),
            handlers: HandlerSet::new(),
            options: EngineOptions::default(),
            api: None,
            api_router: None,
            registration_error: None,
        }
    }
}

impl SteleBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }

    /// Registers the user schema. All tables are created on first run.
    pub fn schema(mut self, schema: SchemaDef) -> Self {
        self.schema = schema;
        self
    }

    /// Registers a setup handler. The key is conventionally `<source>:setup`.
    pub fn setup<F>(mut self, key: impl Into<String>, handler: F) -> Self
    where
        F: for<'a> Fn(&'a EventContext<'_>) -> HandlerFuture<'a> + Send + Sync + 'static,
    {
        let key = key.into();
        if let Err(error) = self.handlers.register_setup(key, handler) {
            self.registration_error
                .get_or_insert_with(|| error.to_string());
        }
        self
    }

    /// Registers an event handler. The key is `<source>:<event>`, for example
    /// `Token:Transfer`.
    pub fn on<F>(mut self, key: impl Into<String>, handler: F) -> Self
    where
        F: for<'a> Fn(&'a EventContext<'_>, &'a DecodedEvent) -> HandlerFuture<'a>
            + Send
            + Sync
            + 'static,
    {
        let key = key.into();
        if let Err(error) = self.handlers.register_event(key, handler) {
            self.registration_error
                .get_or_insert_with(|| error.to_string());
        }
        self
    }

    pub fn follow(mut self, follow: bool) -> Self {
        self.options.follow = follow;
        self
    }

    pub fn fetch_traces(mut self, fetch_traces: bool) -> Self {
        self.options.fetch_traces = fetch_traces;
        self
    }

    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.options.poll_interval = interval;
        self
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.options.batch_size = batch_size;
        self
    }

    pub fn max_retries(mut self, max_retries: usize) -> Self {
        self.options.max_retries = max_retries;
        self
    }

    pub fn api(mut self, host: impl Into<String>, port: u16) -> Self {
        self.api = Some(ApiOptions {
            host: host.into(),
            port,
        });
        self
    }

    /// Adds application-specific HTTP routes. They are merged into the
    /// standard API router and run on the same listener.
    pub fn api_route(mut self, router: Router) -> Self {
        self.api_router = Some(router);
        self
    }

    /// Starts indexing. When an API is configured, it is spawned on the same
    /// Tokio runtime and the future resolves when indexing finishes.
    pub async fn run(self) -> Result<(), SteleError> {
        let Self {
            config,
            schema,
            handlers,
            mut options,
            api,
            api_router,
            registration_error,
        } = self;

        if let Some(error) = registration_error {
            return Err(SteleError::Invalid(error));
        }
        let config = config.ok_or_else(|| {
            SteleError::Invalid("a configuration is required before run()".to_owned())
        })?;
        options.ordering = config.ordering;
        start(config, schema, handlers, options, api, api_router).await
    }
}

/// Convenience entry point used by embedding applications and the CLI.
pub async fn run_from_config(
    config: Config,
    schema: SchemaDef,
    handlers: HandlerSet,
    mut options: EngineOptions,
    api: Option<ApiOptions>,
) -> Result<(), SteleError> {
    options.ordering = config.ordering;
    start(config, schema, handlers, options, api, None).await
}

/// Shared runtime wiring for [`SteleBuilder::run`] and [`run_from_config`].
async fn start(
    config: Config,
    schema: SchemaDef,
    handlers: HandlerSet,
    options: EngineOptions,
    api: Option<ApiOptions>,
    api_router: Option<Router>,
) -> Result<(), SteleError> {
    config.validate()?;
    tracing::info!(
        chains = config.chains.len(),
        contracts = config.contracts.len(),
        accounts = config.accounts.len(),
        blocks = config.blocks.len(),
        ordering = ?config.ordering,
        follow = options.follow,
        // The engine narrows this to plans that actually filter on traces and
        // logs the effective value.
        fetch_traces_requested = options.fetch_traces,
        schema = %config.database.schema,
        "starting indexer"
    );

    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database.url)
        .await?;
    let store = Arc::new(PostgresStore::from_pool(pool.clone()));
    store.initialize(&schema).await?;
    let sources = build_sources(&config)?;
    let plans = build_plans(&config)?.plans;
    tracing::debug!(
        plans = plans.len(),
        rpc_endpoints = config
            .chains
            .values()
            .map(|chain| chain.rpc.len())
            .sum::<usize>(),
        "chain plans built"
    );
    let handlers = Arc::new(handlers);
    let engine = Engine::new(store.clone(), sources, plans.clone(), handlers, options)?;

    if let Some(api) = api {
        let state = ApiState::new(
            pool.clone(),
            ApiConfig {
                schema: config.database.schema.clone(),
                custom_schema: Some(schema.clone()),
                ..ApiConfig::default()
            },
        )?;
        let router = stele_api::router(state);
        let router = match api_router {
            Some(custom) => router.merge(custom),
            None => router,
        };
        spawn_api(&api, router).await?;
    }

    engine.run().await?;
    tracing::info!(
        events = engine.metrics().events_processed(),
        decode_failures = engine.metrics().decode_failures(),
        reorgs = engine.metrics().reorgs(),
        "indexer stopped"
    );
    Ok(())
}

/// Runs only the HTTP API for a configuration. Useful for `stele serve`.
pub async fn serve(
    config: Config,
    schema: SchemaDef,
    host: impl Into<String>,
    port: u16,
) -> Result<(), SteleError> {
    let pool = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database.url)
        .await?;
    let state = ApiState::new(
        pool,
        ApiConfig {
            schema: config.database.schema.clone(),
            custom_schema: Some(schema),
            ..ApiConfig::default()
        },
    )?;
    let router = stele_api::router(state);
    let listener = tokio::net::TcpListener::bind((host.into(), port))
        .await
        .map_err(|error| SteleError::Http(error.to_string()))?;
    tracing::info!(
        address = %listener_address(&listener),
        schema = %config.database.schema,
        "serving api without indexing"
    );
    axum::serve(listener, router)
        .await
        .map_err(|error| SteleError::Http(error.to_string()))
}

/// Applies the user schema to the configured database without indexing.
pub async fn migrate(config: Config, schema: SchemaDef) -> Result<(), SteleError> {
    config.validate()?;
    let pool = PgPoolOptions::new().connect(&config.database.url).await?;
    let store = PostgresStore::from_pool(pool);
    store.initialize(&schema).await?;
    tracing::info!(
        schema = %config.database.schema,
        tables = schema.tables.len(),
        views = schema.views.len(),
        "database schema applied"
    );
    Ok(())
}

async fn spawn_api(options: &ApiOptions, router: Router) -> Result<(), SteleError> {
    let listener = tokio::net::TcpListener::bind((options.host.as_str(), options.port))
        .await
        .map_err(|error| SteleError::Http(error.to_string()))?;
    tracing::info!(address = %listener_address(&listener), "http api listening");
    tokio::spawn(async move {
        if let Err(error) = axum::serve(listener, router).await {
            tracing::error!(%error, "api server stopped");
        }
    });
    Ok(())
}

/// The bound address, falling back to the configured host and port.
fn listener_address(listener: &tokio::net::TcpListener) -> String {
    listener
        .local_addr()
        .map(|address| address.to_string())
        .unwrap_or_else(|_| "unknown".to_owned())
}

fn build_sources(
    config: &Config,
) -> Result<BTreeMap<u64, Arc<dyn stele_core::ChainSource>>, SteleError> {
    let mut sources: BTreeMap<u64, Arc<dyn stele_core::ChainSource>> = BTreeMap::new();
    for chain in config.chains.values() {
        if chain.rpc.is_empty() {
            return Err(SteleError::Invalid(format!(
                "chain {} has no RPC endpoints",
                chain.id
            )));
        }
        let client = RpcClient::new(chain.rpc.clone())?;
        // Endpoints are not logged: hosted RPC URLs usually embed API keys.
        tracing::debug!(
            chain_id = chain.id,
            endpoints = chain.rpc.len(),
            cache_requests = chain.cache_rpc_requests,
            "configured rpc client"
        );
        sources.insert(chain.id, Arc::new(client));
    }
    Ok(sources)
}

/// Application handle type used by binaries that want a shorter name.
pub struct Stele;

impl Stele {
    pub fn builder() -> SteleBuilder {
        SteleBuilder::new()
    }
}

/// Re-export of the chain plan type so custom runners can inspect plans.
pub type Chains = Vec<ChainPlan>;

/// Re-export of the store trait object used by the engine.
pub type DynStore = Arc<dyn Store>;

#[cfg(test)]
mod tests {
    use super::*;

    fn noop_setup<'a, 'b>(_: &'a EventContext<'b>) -> HandlerFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    fn noop_event<'a, 'b>(_: &'a EventContext<'b>, _: &'a DecodedEvent) -> HandlerFuture<'a> {
        Box::pin(async { Ok(()) })
    }

    #[tokio::test]
    async fn run_requires_a_configuration() {
        let error = SteleBuilder::new().run().await.unwrap_err();
        assert!(error.to_string().contains("configuration is required"));
    }

    #[tokio::test]
    async fn duplicate_registration_is_reported_before_running() {
        let error = SteleBuilder::new()
            .setup("Token:setup", noop_setup)
            .setup("Token:setup", noop_setup)
            .on("Token:Transfer", noop_event)
            .run()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("already registered"), "{error}");
    }
}
