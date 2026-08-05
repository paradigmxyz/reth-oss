//! Differential Geth/Reth JSON-RPC compatibility runner.

use eyre::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use similar::TextDiff;
use std::{fs, path::Path, time::Duration};

/// A method entry in the Geth RPC manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct Method {
    /// Stable manifest identifier.
    pub id: String,
    /// JSON-RPC method name.
    pub method: String,
    /// Parameters sent to both clients.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Comparison policy.
    #[serde(default)]
    pub comparison: Comparison,
    /// Whether this method is enabled by default.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Whether this method can mutate node state or process state.
    #[serde(default)]
    pub dangerous: bool,
}

/// Comparison policy for a Geth method.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Comparison {
    /// Compare complete normalized JSON responses.
    #[default]
    Exact,
    /// Compare only whether both clients returned an error or a result.
    Outcome,
    /// Compare only the JSON-RPC error code.
    ErrorCode,
}

const fn default_enabled() -> bool {
    true
}

#[derive(Debug, Deserialize)]
struct Manifest {
    #[serde(rename = "method")]
    methods: Vec<Method>,
}

/// Options for a differential run.
#[derive(Debug, Clone)]
pub struct Options {
    /// Geth HTTP JSON-RPC endpoint.
    pub geth_url: String,
    /// Reth HTTP JSON-RPC endpoint.
    pub reth_url: String,
    /// Method inclusion patterns.
    pub include: Vec<String>,
    /// Method exclusion patterns.
    pub exclude: Vec<String>,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Stop after the first mismatch.
    pub fail_fast: bool,
    /// Include mutating and operational methods.
    pub include_dangerous: bool,
    /// Optional JSON report path.
    pub report: Option<std::path::PathBuf>,
}

#[derive(Debug, serde::Serialize)]
struct ResultRecord {
    id: String,
    method: String,
    outcome: &'static str,
    detail: Option<String>,
}

/// Loads a manifest and compares every selected method against Geth and Reth.
pub async fn run(manifest_path: &Path, options: Options) -> Result<()> {
    let manifest: Manifest =
        toml::from_str(&fs::read_to_string(manifest_path).wrap_err_with(|| {
            format!("failed to read Geth RPC manifest {}", manifest_path.display())
        })?)
        .wrap_err_with(|| {
            format!("failed to parse Geth RPC manifest {}", manifest_path.display())
        })?;

    let client = reqwest::Client::builder().timeout(options.timeout).build()?;
    let mut records = Vec::new();
    let mut mismatches = 0usize;

    for method in manifest.methods.into_iter().filter(|method| {
        method.enabled &&
            (options.include_dangerous || !method.dangerous) &&
            selected(&method.id, &options.include, &options.exclude)
    }) {
        let request =
            json!({"jsonrpc":"2.0", "id": 1, "method": method.method, "params": method.params});
        let geth = request_json(&client, &options.geth_url, &request).await;
        let reth = request_json(&client, &options.reth_url, &request).await;
        let detail = match (geth, reth) {
            (Ok(geth), Ok(reth)) => compare(method.comparison, &geth, &reth),
            (Err(geth), Err(reth)) => {
                Err(format!("both endpoints failed\n-- geth: {geth}\n++ reth: {reth}"))
            }
            (Err(geth), Ok(reth)) => {
                Err(format!("Geth transport error: {geth}\nReth response: {reth}"))
            }
            (Ok(geth), Err(reth)) => {
                Err(format!("Geth response: {geth}\nReth transport error: {reth}"))
            }
        }
        .err();

        let outcome = if detail.is_some() { "mismatch" } else { "pass" };
        if detail.is_some() {
            mismatches += 1;
            println!("FAIL {} ({})", method.id, method.method);
            if let Some(detail) = &detail {
                println!("{detail}");
            }
        } else {
            println!("PASS {} ({})", method.id, method.method);
        }
        records.push(ResultRecord { id: method.id, method: method.method, outcome, detail });
        if options.fail_fast && mismatches != 0 {
            break;
        }
    }

    if let Some(path) = options.report {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, serde_json::to_vec_pretty(&records)?)?;
    }
    if mismatches != 0 {
        eyre::bail!("Geth/Reth RPC comparison found {mismatches} mismatch(es)")
    }
    Ok(())
}

async fn request_json(
    client: &reqwest::Client,
    url: &str,
    request: &Value,
) -> Result<Value, String> {
    let body = serde_json::to_vec(request).map_err(|error| error.to_string())?;
    let response = client
        .post(url)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    let body = response.text().await.map_err(|error| error.to_string())?;
    serde_json::from_str(&body).map_err(|error| format!("HTTP {status}: {error}: {body}"))
}

fn compare(comparison: Comparison, geth: &Value, reth: &Value) -> Result<(), String> {
    let geth = normalize(geth);
    let reth = normalize(reth);
    let matches = match comparison {
        Comparison::Exact => geth == reth,
        Comparison::Outcome => geth.get("error").is_some() == reth.get("error").is_some(),
        Comparison::ErrorCode => geth["error"]["code"] == reth["error"]["code"],
    };
    if matches {
        return Ok(())
    }
    let geth = serde_json::to_string_pretty(&geth).unwrap_or_default();
    let reth = serde_json::to_string_pretty(&reth).unwrap_or_default();
    let diff = TextDiff::from_lines(&geth, &reth).unified_diff().header("geth", "reth").to_string();
    Err(format!("response differs (-- geth, ++ reth):\n{diff}"))
}

fn normalize(value: &Value) -> Value {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        object.remove("id");
    }
    value
}

fn selected(id: &str, include: &[String], exclude: &[String]) -> bool {
    let included = include.is_empty() || include.iter().any(|pattern| wildcard(pattern, id));
    included && !exclude.iter().any(|pattern| wildcard(pattern, id))
}

fn wildcard(pattern: &str, value: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == value
    }
    let starts = !pattern.starts_with('*');
    let ends = !pattern.ends_with('*');
    let mut remainder = value;
    for part in pattern.split('*') {
        if part.is_empty() {
            continue;
        }
        let Some(index) = remainder.find(part) else { return false };
        remainder = &remainder[index + part.len()..];
    }
    (!starts || value.starts_with(pattern.split('*').next().unwrap_or_default())) &&
        (!ends || value.ends_with(pattern.rsplit('*').next().unwrap_or_default()))
}

#[cfg(test)]
mod tests {
    use super::wildcard;

    #[test]
    fn wildcard_patterns_match_expected_ids() {
        assert!(wildcard("eth_*", "eth_getBalance"));
        assert!(wildcard("*_receipt", "get_transaction_receipt"));
        assert!(wildcard("eth_get*Receipt", "eth_getBlockReceipt"));
        assert!(!wildcard("eth_get*Receipt", "eth_getBlockByNumber"));
        assert!(wildcard("exact", "exact"));
        assert!(!wildcard("exact", "other"));
    }
}
