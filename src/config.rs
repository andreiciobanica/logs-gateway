//! Runtime configuration, read from environment variables at startup.
//!
//! Required variables make the process exit with a message listing every
//! missing one, so a half-configured gateway never starts.

use std::env;
use std::str::FromStr;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Config {
    /// Base URL of the panel. The original request path (`/api/...`) is
    /// appended to it unchanged.
    pub external_url_base: String,
    /// Login endpoint that returns `{ "token": "..." }`.
    pub external_auth_url: String,
    pub external_email: String,
    pub external_password: String,
    pub port: u16,
    pub retry_interval: Duration,
    pub token_refresh: Duration,
    pub max_inflight: usize,
    /// Request paths recorded locally instead of being forwarded.
    pub local_log_paths: Vec<String>,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    /// Builds the config from any lookup function, so it can be tested
    /// without touching the process environment.
    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut errors = Vec::new();

        let mut required = |name: &str| -> String {
            match get(name).filter(|v| !v.trim().is_empty()) {
                Some(v) => v.trim().to_string(),
                None => {
                    errors.push(format!("{name} is required"));
                    String::new()
                }
            }
        };

        let external_url_base = required("EXTERNAL_URL_BASE");
        let external_auth_url = required("EXTERNAL_AUTH_URL");
        let external_email = required("EXTERNAL_EMAIL");
        let external_password = required("EXTERNAL_PASSWORD");

        let mut optional = |name: &str, default: u64| -> u64 {
            match get(name).filter(|v| !v.trim().is_empty()) {
                None => default,
                Some(v) => match u64::from_str(v.trim()) {
                    Ok(n) => n,
                    Err(_) => {
                        errors.push(format!("{name}: expected a number, got {v:?}"));
                        default
                    }
                },
            }
        };

        let port = optional("PORT", 3000);
        let retry_interval = optional("RETRY_INTERVAL_SECS", 30);
        let token_refresh = optional("TOKEN_REFRESH_SECS", 1800);
        let max_inflight = optional("MAX_INFLIGHT", 64);

        let local_log_paths: Vec<String> = get("LOCAL_LOG_PATHS")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(str::to_string)
            .collect();
        for path in &local_log_paths {
            if !path.starts_with('/') {
                errors.push(format!("LOCAL_LOG_PATHS: {path:?} must start with '/'"));
            }
        }

        if port > u16::MAX as u64 {
            errors.push(format!("PORT: {port} is out of range"));
        }
        if max_inflight == 0 {
            errors.push("MAX_INFLIGHT must be at least 1".to_string());
        }
        if retry_interval == 0 || token_refresh == 0 {
            errors
                .push("RETRY_INTERVAL_SECS and TOKEN_REFRESH_SECS must be at least 1".to_string());
        }

        if !errors.is_empty() {
            return Err(errors.join("\n"));
        }

        Ok(Self {
            external_url_base,
            external_auth_url,
            external_email,
            external_password,
            port: port as u16,
            retry_interval: Duration::from_secs(retry_interval),
            token_refresh: Duration::from_secs(token_refresh),
            max_inflight: max_inflight as usize,
            local_log_paths,
        })
    }

    /// Joins the panel base URL with an incoming request path.
    /// `https://panel.example.ro/` + `/api/x/store` -> `https://panel.example.ro/api/x/store`
    pub fn target_url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.external_url_base.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    fn minimal() -> Vec<(&'static str, &'static str)> {
        vec![
            ("EXTERNAL_URL_BASE", "https://panel.example.ro/"),
            (
                "EXTERNAL_AUTH_URL",
                "https://panel.example.ro/api/auth/login",
            ),
            ("EXTERNAL_EMAIL", "bot@example.ro"),
            ("EXTERNAL_PASSWORD", "secret"),
        ]
    }

    #[test]
    fn defaults_apply_when_optional_vars_are_missing() {
        let cfg = Config::from_lookup(lookup(&minimal())).unwrap();
        assert_eq!(cfg.port, 3000);
        assert_eq!(cfg.retry_interval, Duration::from_secs(30));
        assert_eq!(cfg.token_refresh, Duration::from_secs(1800));
        assert_eq!(cfg.max_inflight, 64);
    }

    #[test]
    fn every_missing_required_var_is_reported() {
        let err = Config::from_lookup(lookup(&[("EXTERNAL_EMAIL", "x")])).unwrap_err();
        assert!(err.contains("EXTERNAL_URL_BASE is required"));
        assert!(err.contains("EXTERNAL_AUTH_URL is required"));
        assert!(err.contains("EXTERNAL_PASSWORD is required"));
        assert!(!err.contains("EXTERNAL_EMAIL"));
    }

    #[test]
    fn invalid_numbers_are_rejected() {
        let mut pairs = minimal();
        pairs.push(("MAX_INFLIGHT", "lots"));
        let err = Config::from_lookup(lookup(&pairs)).unwrap_err();
        assert!(err.contains("MAX_INFLIGHT"));
    }

    #[test]
    fn local_log_paths_are_split_and_validated() {
        let mut pairs = minimal();
        pairs.push((
            "LOCAL_LOG_PATHS",
            " /api/admin-action-log/store, /api/audit/store ,",
        ));
        let cfg = Config::from_lookup(lookup(&pairs)).unwrap();
        assert_eq!(
            cfg.local_log_paths,
            vec!["/api/admin-action-log/store", "/api/audit/store"]
        );

        let mut bad = minimal();
        bad.push(("LOCAL_LOG_PATHS", "api/no-slash"));
        assert!(
            Config::from_lookup(lookup(&bad))
                .unwrap_err()
                .contains("LOCAL_LOG_PATHS")
        );
    }

    #[test]
    fn target_url_joins_without_double_slashes() {
        let cfg = Config::from_lookup(lookup(&minimal())).unwrap();
        assert_eq!(
            cfg.target_url("/api/kill-log/store"),
            "https://panel.example.ro/api/kill-log/store"
        );
    }
}
