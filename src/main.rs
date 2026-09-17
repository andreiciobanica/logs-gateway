//! logs-gateway: sits between a FiveM server and an external log panel.
//!
//! Request path: `POST /api/*` -> bounded queue -> 202. Delivery happens in
//! background tasks (`forward`), failures go to the retry actor (`retry`),
//! the bearer token is owned by `auth`, and paths listed in `LOCAL_LOG_PATHS`
//! are written to disk by `local` instead. On SIGTERM the listener closes,
//! the queue is drained, and the retry actor flushes its file before exit.

mod auth;
mod config;
mod forward;
mod local;
mod retry;

use std::sync::Arc;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{OriginalUri, State},
    http::StatusCode,
    routing::post,
};
use serde_json::Value;
use tokio::signal;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Semaphore, mpsc};

use crate::auth::TokenManager;
use crate::config::Config;
use crate::forward::{LogMessage, Shared};
use crate::local::LocalLog;
use crate::retry::{FailedLog, RetryMsg};

const QUEUE_CAPACITY: usize = 1000;
const RETRY_CAPACITY: usize = 1000;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct AppState {
    cfg: Arc<Config>,
    local: Arc<LocalLog>,
    queue: mpsc::Sender<LogMessage>,
    retry: mpsc::Sender<RetryMsg>,
}

#[tokio::main]
async fn main() {
    let cfg = match Config::from_env() {
        Ok(cfg) => Arc::new(cfg),
        Err(errors) => {
            eprintln!("configuration error:\n{errors}");
            std::process::exit(1);
        }
    };

    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT)
        .build()
        .expect("reqwest client");

    // Blocks (with backoff) until the panel hands out a first token.
    let (tokens, refresher) = TokenManager::start(client.clone(), Arc::clone(&cfg)).await;

    let shared = Arc::new(Shared {
        cfg: Arc::clone(&cfg),
        client,
        tokens,
        inflight: Arc::new(Semaphore::new(cfg.max_inflight)),
    });

    let (retry_tx, retry_rx) = mpsc::channel(RETRY_CAPACITY);
    let retry_actor = retry::spawn(Arc::clone(&shared), retry_rx);

    let (queue_tx, queue_rx) = mpsc::channel::<LogMessage>(QUEUE_CAPACITY);
    let forwarders = tokio::spawn(forward::run(
        queue_rx,
        Arc::clone(&shared),
        retry_tx.clone(),
    ));

    let state = AppState {
        cfg: Arc::clone(&cfg),
        local: Arc::new(LocalLog::new(cfg.local_log_paths.clone())),
        queue: queue_tx,
        retry: retry_tx.clone(),
    };
    let app = Router::new()
        .route("/api/*rest", post(receive_log))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("cannot bind {addr}: {e}");
            std::process::exit(1);
        });
    println!("listening on http://{addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server error");

    // Shutdown, in dependency order:
    // 1. `serve` returned, dropping the router and with it the last queue
    //    sender, so `forward::run` drains what is left and waits for every
    //    in-flight forward.
    println!("shutting down: draining queue");
    let _ = forwarders.await;

    // 2. Drop our retry sender; the forwarders' clones are already gone, so
    //    the actor sees a closed channel, compacts its file and exits.
    drop(retry_tx);
    let _ = retry_actor.await;

    // 3. Nothing needs tokens any more.
    refresher.abort();
    println!("bye");
}

/// Accepts any JSON body for any `/api/...` path. Paths listed in
/// `LOCAL_LOG_PATHS` are written to a local file; everything else is queued
/// for delivery to the same path on the panel.
async fn receive_log(
    State(state): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<Value>,
) -> StatusCode {
    let path = uri.path();

    if state.local.matches(path) {
        return match state.local.record(path, &body).await {
            Ok(()) => StatusCode::ACCEPTED,
            Err(e) => {
                eprintln!("[local] cannot write log for {path}: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
    }

    let url = state.cfg.target_url(path);

    match state.queue.try_send((url, body)) {
        Ok(()) => StatusCode::ACCEPTED,
        // Live queue full (panel slow or down): persist through the retry
        // actor instead of dropping the log or blocking the game server.
        Err(TrySendError::Full((url, body))) => {
            match state
                .retry
                .send(RetryMsg::Failed(FailedLog { url, body }))
                .await
            {
                Ok(()) => StatusCode::ACCEPTED,
                Err(_) => StatusCode::SERVICE_UNAVAILABLE,
            }
        }
        Err(TrySendError::Closed(_)) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

/// Resolves on Ctrl-C or SIGTERM (what systemd sends on `stop`).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => eprintln!("cannot listen for SIGTERM: {e}"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
