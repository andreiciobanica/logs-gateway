//! Paths that are recorded locally instead of being forwarded.
//!
//! Some logs are only useful on the box the game server runs on (audit
//! trails for admin actions, for example). Paths listed in
//! `LOCAL_LOG_PATHS` are appended, one JSON line each, to a daily file under
//! `logs/` and never sent to the panel.

use std::collections::HashSet;
use std::io;

use serde_json::{Value, json};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

const LOG_DIR: &str = "logs";

pub struct LocalLog {
    paths: HashSet<String>,
}

impl LocalLog {
    pub fn new(paths: impl IntoIterator<Item = String>) -> Self {
        Self {
            paths: paths.into_iter().collect(),
        }
    }

    pub fn matches(&self, path: &str) -> bool {
        self.paths.contains(path)
    }

    /// Appends one NDJSON line to `logs/local-YYYY-MM-DD.log`.
    pub async fn record(&self, path: &str, body: &Value) -> io::Result<()> {
        let now = chrono::Local::now();
        let file = format!("{LOG_DIR}/local-{}.log", now.format("%Y-%m-%d"));
        let line = format_line(&now.to_rfc3339(), path, body);

        fs::create_dir_all(LOG_DIR).await?;
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file)
            .await?;
        f.write_all(line.as_bytes()).await
    }
}

fn format_line(timestamp: &str, path: &str, body: &Value) -> String {
    let entry = json!({ "ts": timestamp, "path": path, "body": body });
    let mut line = entry.to_string();
    line.push('\n');
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_listed_paths_match() {
        let local = LocalLog::new(vec!["/api/admin-action-log/store".to_string()]);
        assert!(local.matches("/api/admin-action-log/store"));
        assert!(!local.matches("/api/kill-log/store"));
        assert!(!local.matches("/api/admin-action-log/store/"));
    }

    #[test]
    fn line_is_one_json_object_with_the_body_nested() {
        let line = format_line(
            "2026-01-01T10:00:00+02:00",
            "/api/admin-action-log/store",
            &json!({ "user_id": 12, "action": "teleport" }),
        );
        assert!(line.ends_with('\n'));
        let parsed: Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed["path"], "/api/admin-action-log/store");
        assert_eq!(parsed["body"]["user_id"], 12);
        assert_eq!(parsed["ts"], "2026-01-01T10:00:00+02:00");
    }
}
