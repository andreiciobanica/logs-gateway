//! The retry actor: the only task that touches `failed_logs.json`.
//!
//! Forwarders that fail send their log here over a channel. The actor keeps
//! the queue in memory, appends every new failure as one NDJSON line (O(1)
//! per failure, crash-safe up to the last line), retries the whole queue on
//! a timer, and after each round rewrites the file through a temporary file
//! plus `rename`, so a crash mid-write leaves either the old file or the new
//! one, never a truncated one.
//!
//! Files written by the old version (a pretty-printed JSON array of
//! `[body, url]` pairs) are migrated on startup.

use std::io;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval_at};

use crate::forward::{Outcome, Shared, forward};

pub const FAILED_LOGS_FILE: &str = "failed_logs.json";
const TMP_FILE: &str = "failed_logs.json.tmp";

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct FailedLog {
    pub url: String,
    pub body: Value,
}

pub enum RetryMsg {
    Failed(FailedLog),
}

pub fn spawn(shared: Arc<Shared>, rx: mpsc::Receiver<RetryMsg>) -> JoinHandle<()> {
    tokio::spawn(run(shared, rx))
}

async fn run(shared: Arc<Shared>, mut rx: mpsc::Receiver<RetryMsg>) {
    let loaded = load().await;
    let mut queue = loaded.logs;

    if loaded.legacy {
        println!("[retry] migrating legacy array file ({} logs)", queue.len());
        compact(&queue).await;
    } else if !queue.is_empty() {
        println!("[retry] loaded {} failed logs from disk", queue.len());
    }
    if loaded.skipped > 0 {
        eprintln!("[retry] skipped {} unreadable lines", loaded.skipped);
    }

    let period = shared.cfg.retry_interval;
    let mut tick = interval_at(Instant::now() + period, period);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(RetryMsg::Failed(log)) => {
                    append(&log).await;
                    queue.push(log);
                }
                // Every sender dropped: the gateway is shutting down.
                None => break,
            },
            _ = tick.tick() => {
                if queue.is_empty() {
                    continue;
                }
                let before = queue.len();
                retry_round(&shared, &mut queue).await;
                compact(&queue).await;
                println!("[retry] {} of {} delivered, {} still pending",
                    before - queue.len(), before, queue.len());
            }
        }
    }

    compact(&queue).await;
    println!("[retry] flushed, {} logs left on disk", queue.len());
}

/// One pass over the queue. Stops early when the panel is unreachable, since
/// the remaining logs would only add timeouts.
async fn retry_round(shared: &Shared, queue: &mut Vec<FailedLog>) {
    let pending = std::mem::take(queue);
    let mut give_up = false;

    for log in pending {
        if give_up {
            queue.push(log);
            continue;
        }

        let _permit = shared
            .inflight
            .acquire()
            .await
            .expect("inflight semaphore is never closed");

        match forward(shared, &log.url, &log.body).await {
            Outcome::Sent => {}
            Outcome::Unreachable(e) => {
                eprintln!("[retry] panel unreachable ({e}), ending this round");
                give_up = true;
                queue.push(log);
            }
            _ => queue.push(log),
        }
    }
}

// ---- persistence --------------------------------------------------------

struct Loaded {
    logs: Vec<FailedLog>,
    legacy: bool,
    skipped: usize,
}

async fn load() -> Loaded {
    match fs::read_to_string(FAILED_LOGS_FILE).await {
        Ok(contents) => parse(&contents),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Loaded {
            logs: vec![],
            legacy: false,
            skipped: 0,
        },
        Err(e) => {
            eprintln!("[retry] cannot read {FAILED_LOGS_FILE}: {e}");
            Loaded {
                logs: vec![],
                legacy: false,
                skipped: 0,
            }
        }
    }
}

/// Parses either format: the legacy JSON array or NDJSON.
fn parse(contents: &str) -> Loaded {
    let trimmed = contents.trim_start();

    if trimmed.starts_with('[') {
        let pairs: Vec<(Value, String)> = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[retry] legacy file is not valid JSON: {e}");
                return Loaded {
                    logs: vec![],
                    legacy: true,
                    skipped: 1,
                };
            }
        };
        let logs = pairs
            .into_iter()
            .map(|(body, url)| FailedLog { url, body })
            .collect();
        return Loaded {
            logs,
            legacy: true,
            skipped: 0,
        };
    }

    let mut logs = Vec::new();
    let mut skipped = 0;
    for line in contents.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<FailedLog>(line) {
            Ok(log) => logs.push(log),
            Err(_) => skipped += 1,
        }
    }
    Loaded {
        logs,
        legacy: false,
        skipped,
    }
}

async fn append(log: &FailedLog) {
    let line = match serde_json::to_string(log) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[retry] cannot serialize log: {e}");
            return;
        }
    };

    let result: io::Result<()> = async {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(FAILED_LOGS_FILE)
            .await?;
        file.write_all(line.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await
    }
    .await;

    if let Err(e) = result {
        // The log is still in memory and will be written at the next compaction.
        eprintln!("[retry] cannot append to {FAILED_LOGS_FILE}: {e}");
    }
}

/// Rewrites the file from the in-memory queue: tmp file, fsync, rename.
async fn compact(queue: &[FailedLog]) {
    if queue.is_empty() {
        match fs::remove_file(FAILED_LOGS_FILE).await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => eprintln!("[retry] cannot remove {FAILED_LOGS_FILE}: {e}"),
        }
        return;
    }

    let mut out = String::new();
    for log in queue {
        match serde_json::to_string(log) {
            Ok(line) => {
                out.push_str(&line);
                out.push('\n');
            }
            Err(e) => eprintln!("[retry] cannot serialize log: {e}"),
        }
    }

    let result: io::Result<()> = async {
        let mut file = File::create(TMP_FILE).await?;
        file.write_all(out.as_bytes()).await?;
        file.sync_all().await?;
        drop(file);
        fs::rename(TMP_FILE, FAILED_LOGS_FILE).await
    }
    .await;

    if let Err(e) = result {
        eprintln!("[retry] cannot compact {FAILED_LOGS_FILE}: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_ndjson_and_skips_corrupt_lines() {
        let contents = concat!(
            r#"{"url":"https://p/api/a/store","body":{"k":1}}"#,
            "\n",
            "\n",
            r#"{"url":"https://p/api/b/store","body":{"k":2}}"#,
            "\n",
            r#"{"url":"https://p/api/c/store","bo"#, // torn last line after a crash
        );
        let loaded = parse(contents);
        assert!(!loaded.legacy);
        assert_eq!(loaded.skipped, 1);
        assert_eq!(loaded.logs.len(), 2);
        assert_eq!(loaded.logs[1].body, json!({ "k": 2 }));
    }

    #[test]
    fn migrates_legacy_array_format() {
        let contents = r#"[
  [ { "k": 1 }, "https://p/api/a/store" ],
  [ { "k": 2 }, "https://p/api/b/store" ]
]"#;
        let loaded = parse(contents);
        assert!(loaded.legacy);
        assert_eq!(
            loaded.logs,
            vec![
                FailedLog {
                    url: "https://p/api/a/store".into(),
                    body: json!({ "k": 1 })
                },
                FailedLog {
                    url: "https://p/api/b/store".into(),
                    body: json!({ "k": 2 })
                },
            ]
        );
    }

    #[test]
    fn empty_file_is_empty_queue() {
        let loaded = parse("");
        assert!(!loaded.legacy);
        assert!(loaded.logs.is_empty());
        assert_eq!(loaded.skipped, 0);
    }
}
