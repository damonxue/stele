//! Command line interface for stele.
//!
//! `start` runs the indexing engine and the HTTP API in one process. `serve`
//! runs only the API. Applications that register handlers should call
//! [`stele::SteleBuilder`] from their own binary; the CLI keeps the same
//! database, sync and API wiring for deployments that only need the runtime.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, SystemTime};

use clap::{Parser, Subcommand};
use stele::{
    ApiOptions, Config, EngineOptions, HandlerSet, SchemaDef, SteleError, run_from_config, serve,
};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "stele", version, about = "EVM indexing runtime")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Index and serve the API in one process.
    Start {
        #[arg(long, short, default_value = "stele.toml")]
        config: PathBuf,
        /// Stop after the configured backfill range instead of following the tip.
        #[arg(long)]
        once: bool,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 42069)]
        port: u16,
        /// Disable the HTTP API.
        #[arg(long)]
        no_api: bool,
    },
    /// Serve the HTTP API without indexing.
    Serve {
        #[arg(long, short, default_value = "stele.toml")]
        config: PathBuf,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 42069)]
        port: u16,
    },
    /// Restart indexing when the configuration file changes.
    Dev {
        #[arg(long, short, default_value = "stele.toml")]
        config: PathBuf,
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        #[arg(long, default_value_t = 42069)]
        port: u16,
    },
    /// Database maintenance.
    Db {
        #[command(subcommand)]
        command: DbCommand,
    },
    /// Check an API's readiness endpoint.
    Healthcheck {
        #[arg(long, default_value = "http://127.0.0.1:42069")]
        url: String,
    },
}

#[derive(Subcommand, Debug)]
enum DbCommand {
    /// Create the internal tables and the configured user schema.
    Migrate {
        #[arg(long, short, default_value = "stele.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(log_filter())
        .init();

    let command = Cli::parse().command;
    match run(command).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "command failed");
            ExitCode::FAILURE
        }
    }
}

/// Builds the log filter from `RUST_LOG`.
///
/// `RUST_LOG` is frequently set for another program in the same shell, for
/// example `RUST_LOG=some_service=debug`. Such a value only names targets, and
/// without a global directive every unlisted target falls back to `ERROR`, which
/// hides all logs this binary emits. Append an `info` fallback in that case; a
/// global directive in `RUST_LOG` (such as `RUST_LOG=warn`) still wins.
fn log_filter() -> EnvFilter {
    let raw = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or_default();
    let has_global_directive = raw.split(',').any(|directive| {
        let directive = directive.trim();
        !directive.is_empty() && !directive.contains('=')
    });
    let spec = if has_global_directive {
        raw
    } else {
        format!("{raw},info")
    };
    EnvFilter::builder().parse_lossy(spec)
}

async fn run(command: Command) -> Result<(), SteleError> {
    match command {
        Command::Start {
            config,
            once,
            host,
            port,
            no_api,
        } => start(&config, !once, host, port, !no_api).await,
        Command::Serve { config, host, port } => {
            let config = Config::from_file(&config)?;
            serve(config, SchemaDef::default(), host, port).await
        }
        Command::Dev { config, host, port } => dev(&config, host, port).await,
        Command::Db {
            command: DbCommand::Migrate { config },
        } => {
            let config = Config::from_file(&config)?;
            stele::migrate(config, SchemaDef::default()).await
        }
        Command::Healthcheck { url } => healthcheck(&url).await,
    }
}

async fn start(
    path: &Path,
    follow: bool,
    host: String,
    port: u16,
    serve_api: bool,
) -> Result<(), SteleError> {
    let config = Config::from_file(path)?;
    let options = EngineOptions {
        follow,
        ..EngineOptions::default()
    };
    let api = serve_api.then_some(ApiOptions { host, port });
    tracing::info!(chains = config.chains.len(), follow, "starting stele");
    run_from_config(
        config,
        SchemaDef::default(),
        HandlerSet::new(),
        options,
        api,
    )
    .await
}

async fn dev(path: &Path, host: String, port: u16) -> Result<(), SteleError> {
    let mut task: Option<tokio::task::JoinHandle<Result<(), SteleError>>> = None;
    let mut modified = file_modified(path);
    loop {
        if task.is_none() {
            let path = path.to_owned();
            let host = host.clone();
            task = Some(tokio::spawn(async move {
                start(&path, true, host, port, true).await
            }));
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
        let current = file_modified(path);
        if current != modified {
            modified = current;
            tracing::info!("configuration changed, restarting");
            if let Some(task) = task.take() {
                task.abort();
            }
        } else if task.as_ref().is_some_and(|task| task.is_finished()) {
            if let Some(task) = task.take() {
                match task.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::error!(%error, "indexing stopped"),
                    Err(error) => tracing::error!(%error, "indexing task failed"),
                }
            }
            // Retry after a short delay so a transient RPC or database failure
            // does not spin.
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}

fn file_modified(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path)
        .ok()
        .and_then(|metadata| metadata.modified().ok())
}

async fn healthcheck(base: &str) -> Result<(), SteleError> {
    let url = format!("{}/health", base.trim_end_matches('/'));
    let response = reqwest::get(&url)
        .await
        .map_err(|error| SteleError::Http(error.to_string()))?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(SteleError::Http(format!(
            "health endpoint returned {}",
            response.status()
        )))
    }
}
