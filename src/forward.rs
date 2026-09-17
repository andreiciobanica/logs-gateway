//! Delivery of queued logs to the panel.
//!
//! `run` pulls from the live queue and spawns one task per log, capped by a
//! semaphore. A task that fails hands its log to the retry actor; it never
//! touches the failed-log file itself.

use std::sync::Arc;

use reqwest::{Client, StatusCode};
use serde_json::Value;
use tokio::sync::{Semaphore, mpsc};

use crate::auth::TokenManager;
use crate::config::Config;
use crate::retry::{FailedLog, RetryMsg};

/// Everything a forwarder needs, shared between the live forwarders and the
/// retry actor.
pub struct Shared {
    pub cfg: Arc<Config>,
    pub client: Client,
    pub tokens: TokenManager,
    /// Caps forwards in flight to the panel, live and retried alike.
    pub inflight: Arc<Semaphore>,
}

/// A log waiting to be forwarded: target URL plus JSON body.
pub type LogMessage = (String, Value);

#[derive(Debug)]
pub enum Outcome {
    Sent,
    /// The panel rejected the token, even after one refresh.
    Unauthorized,
    /// The panel answered with an error other than 401.
    Rejected(StatusCode),
    /// No usable answer: connection error or timeout.
    Unreachable(String),
}

impl Outcome {
    pub fn is_sent(&self) -> bool {
        matches!(self, Outcome::Sent)
    }

    /// Short reason for log lines.
    pub fn describe(&self) -> String {
        match self {
            Outcome::Sent => "sent".to_string(),
            Outcome::Unauthorized => "401 even after a token refresh".to_string(),
            Outcome::Rejected(status) => format!("HTTP {status}"),
            Outcome::Unreachable(e) => format!("unreachable: {e}"),
        }
    }
}

/// One delivery attempt, with a single retry on `401` after refreshing the
/// token. The caller is responsible for holding an `inflight` permit.
pub async fn forward(shared: &Shared, url: &str, body: &Value) -> Outcome {
    let token = shared.tokens.current();

    match send_once(&shared.client, url, body, &token).await {
        Outcome::Unauthorized => match shared.tokens.refreshed_after(&token).await {
            Some(fresh) => send_once(&shared.client, url, body, &fresh).await,
            None => Outcome::Unauthorized,
        },
        other => other,
    }
}

async fn send_once(client: &Client, url: &str, body: &Value, token: &str) -> Outcome {
    match client
        .post(url)
        .header("Authorization", token)
        .json(body)
        .send()
        .await
    {
        Ok(res) if res.status().is_success() => Outcome::Sent,
        Ok(res) if res.status() == StatusCode::UNAUTHORIZED => Outcome::Unauthorized,
        Ok(res) => {
            let status = res.status();
            let text = res.text().await.unwrap_or_default();
            eprintln!("[forward] {url} -> {status}: {}", text.trim());
            Outcome::Rejected(status)
        }
        Err(e) => Outcome::Unreachable(e.to_string()),
    }
}

/// Consumes the live queue until every sender is dropped, then waits for all
/// in-flight forwards to finish before returning.
pub async fn run(
    mut queue: mpsc::Receiver<LogMessage>,
    shared: Arc<Shared>,
    retry: mpsc::Sender<RetryMsg>,
) {
    while let Some((url, body)) = queue.recv().await {
        // Back-pressure: when all permits are taken this await blocks, the
        // queue fills up, and eventually the HTTP handlers start diverting
        // logs to the retry actor.
        let permit = shared
            .inflight
            .clone()
            .acquire_owned()
            .await
            .expect("inflight semaphore is never closed");

        let shared = Arc::clone(&shared);
        let retry = retry.clone();

        tokio::spawn(async move {
            let _permit = permit;
            let outcome = forward(&shared, &url, &body).await;
            if !outcome.is_sent() {
                eprintln!(
                    "[forward] {url} failed ({}), queued for retry",
                    outcome.describe()
                );
                let msg = RetryMsg::Failed(FailedLog { url, body });
                if retry.send(msg).await.is_err() {
                    eprintln!("[forward] retry actor is gone, log dropped");
                }
            }
        });
    }

    // Queue closed: wait until every spawned forward has released its permit.
    let max = shared.cfg.max_inflight as u32;
    let _all = shared.inflight.acquire_many(max).await;
    println!("[forward] drained");
}
