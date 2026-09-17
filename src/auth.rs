//! Bearer token lifecycle.
//!
//! One background task owns the token. It fetches it at startup with
//! exponential backoff, refreshes it on a timer, and refreshes it on demand
//! when a forwarder gets a `401`. The current value is published through a
//! `watch` channel: readers always see the latest token and can wait for the
//! next one. Refresh requests go through a channel of capacity 1, so a burst
//! of `401`s collapses into a single login call.

use std::sync::Arc;
use std::time::Duration;

use reqwest::Client;
use serde_json::Value;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use crate::config::Config;

/// How long a forwarder waits for a refreshed token after a `401`.
const REFRESH_WAIT: Duration = Duration::from_secs(20);
const BACKOFF_START: Duration = Duration::from_secs(1);
const BACKOFF_CAP: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct TokenManager {
    current: watch::Receiver<String>,
    refresh: mpsc::Sender<()>,
}

impl TokenManager {
    /// Blocks until a first token is obtained, then spawns the refresher.
    pub async fn start(client: Client, cfg: Arc<Config>) -> (TokenManager, JoinHandle<()>) {
        let first = fetch_with_backoff(&client, &cfg).await;
        let (tx, rx) = watch::channel(first);
        let (refresh_tx, refresh_rx) = mpsc::channel(1);

        let handle = tokio::spawn(refresher(client, cfg, tx, refresh_rx));
        let manager = TokenManager {
            current: rx,
            refresh: refresh_tx,
        };
        (manager, handle)
    }

    /// The latest token, formatted as an `Authorization` header value.
    pub fn current(&self) -> String {
        self.current.borrow().clone()
    }

    /// Called after a `401`. Returns a token that is newer than `stale`,
    /// or `None` if no refresh happened within `REFRESH_WAIT`.
    ///
    /// If another forwarder already triggered a refresh and the token has
    /// changed since the caller read it, this returns immediately without
    /// asking for another login.
    pub async fn refreshed_after(&self, stale: &str) -> Option<String> {
        let mut rx = self.current.clone();

        let latest = rx.borrow_and_update().clone();
        if latest != stale {
            return Some(latest);
        }

        // Capacity is 1: if a refresh is already queued, `Full` is fine.
        let _ = self.refresh.try_send(());

        match timeout(REFRESH_WAIT, rx.changed()).await {
            Ok(Ok(())) => Some(rx.borrow().clone()),
            _ => None,
        }
    }
}

async fn refresher(
    client: Client,
    cfg: Arc<Config>,
    tx: watch::Sender<String>,
    mut requests: mpsc::Receiver<()>,
) {
    loop {
        // The timer restarts after every refresh, including on-demand ones,
        // so a token refreshed because of a 401 is not refreshed again a
        // second later by the periodic timer.
        tokio::select! {
            _ = sleep(cfg.token_refresh) => {}
            req = requests.recv() => {
                if req.is_none() {
                    return; // every TokenManager was dropped: shutting down
                }
            }
        }

        match fetch_token(&client, &cfg).await {
            Some(token) => {
                // Drop requests that queued up while we were logging in;
                // they were all about the token we just replaced.
                while requests.try_recv().is_ok() {}
                if tx.send(token).is_err() {
                    return; // no receivers left
                }
                println!("[auth] token refreshed");
            }
            None => eprintln!("[auth] token refresh failed, will retry on next cycle"),
        }
    }
}

async fn fetch_with_backoff(client: &Client, cfg: &Config) -> String {
    let mut delay = BACKOFF_START;
    loop {
        if let Some(token) = fetch_token(client, cfg).await {
            println!("[auth] token acquired");
            return token;
        }
        eprintln!("[auth] login failed, retrying in {}s", delay.as_secs());
        sleep(delay).await;
        delay = (delay * 2).min(BACKOFF_CAP);
    }
}

async fn fetch_token(client: &Client, cfg: &Config) -> Option<String> {
    let response = client
        .post(&cfg.external_auth_url)
        .form(&[
            ("email", cfg.external_email.as_str()),
            ("password", cfg.external_password.as_str()),
        ])
        .send()
        .await
        .map_err(|e| eprintln!("[auth] login request failed: {e}"))
        .ok()?;

    if !response.status().is_success() {
        eprintln!("[auth] login rejected: {}", response.status());
        return None;
    }

    let body: Value = response
        .json()
        .await
        .map_err(|e| eprintln!("[auth] login response was not JSON: {e}"))
        .ok()?;

    let token = body.get("token")?.as_str()?;
    Some(format!("Bearer {token}"))
}
