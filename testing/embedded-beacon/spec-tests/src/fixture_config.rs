//! Case-local configuration layers shared by typed fixture parameter readers.

use super::{
    config_override, config_version_override, config_yaml, fixed_hex, phase0_processing, Case,
};
use serde_yaml::Value;

#[derive(Clone, Debug)]
pub(super) struct FixtureConfig {
    layers: Vec<Result<Value, String>>,
}

impl FixtureConfig {
    fn load(case: &Case, max_bytes: usize) -> Self {
        let mut ancestors = case.path.ancestors().take(6).collect::<Vec<_>>();
        ancestors.reverse();
        let mut layers = Vec::new();
        for ancestor in ancestors {
            let path = ancestor.join("config.yaml");
            if path.is_file() {
                layers.push(config_yaml(&path, max_bytes));
            }
        }
        Self { layers }
    }

    pub(super) fn unsigned(&self, key: &str, default: u64) -> Result<u64, String> {
        let mut value = default;
        // Validate every present layer, including shadowed malformed values.
        for layer in &self.layers {
            let layer = layer.as_ref().map_err(Clone::clone)?;
            if !layer[key].is_null() {
                value = layer[key]
                    .as_u64()
                    .or_else(|| layer[key].as_str().and_then(|text| text.parse().ok()))
                    .ok_or_else(|| format!("invalid unsigned config override: {key}"))?;
            }
        }
        Ok(value)
    }

    pub(super) fn version(&self, key: &str, default: [u8; 4]) -> Result<[u8; 4], String> {
        let mut value = default;
        for layer in &self.layers {
            let layer = layer.as_ref().map_err(Clone::clone)?;
            if !layer[key].is_null() {
                value = fixed_hex::<4>(&layer[key])?;
            }
        }
        Ok(value)
    }

    pub(super) fn value(&self, key: &str) -> Result<Option<Value>, String> {
        for layer in self.layers.iter().rev() {
            let layer = layer.as_ref().map_err(Clone::clone)?;
            if let Some(value) = layer.get(key) {
                return Ok(Some(value.clone()));
            }
        }
        Ok(None)
    }
}

pub(super) fn with_config<T>(
    case: &Case,
    max_bytes: usize,
    read: impl FnOnce(&FixtureConfig) -> Result<T, String>,
) -> Result<T, String> {
    let (limit, config) =
        case.configuration.get_or_init(|| (max_bytes, FixtureConfig::load(case, max_bytes)));
    if *limit == max_bytes {
        read(config)
    } else {
        // Changing the byte limit requires fresh bounded reads.
        read(&FixtureConfig::load(case, max_bytes))
    }
}

pub(super) fn apply_phase0(
    case: &Case,
    max_bytes: usize,
    parameters: &mut phase0_processing::Phase0Parameters,
) -> Result<(), String> {
    parameters.ejection_balance =
        config_override(case, "EJECTION_BALANCE", parameters.ejection_balance, max_bytes)?;
    parameters.min_per_epoch_churn_limit = config_override(
        case,
        "MIN_PER_EPOCH_CHURN_LIMIT",
        parameters.min_per_epoch_churn_limit,
        max_bytes,
    )?;
    parameters.churn_limit_quotient =
        config_override(case, "CHURN_LIMIT_QUOTIENT", parameters.churn_limit_quotient, max_bytes)?;
    parameters.min_validator_withdrawability_delay = config_override(
        case,
        "MIN_VALIDATOR_WITHDRAWABILITY_DELAY",
        parameters.min_validator_withdrawability_delay,
        max_bytes,
    )?;
    parameters.shard_committee_period = config_override(
        case,
        "SHARD_COMMITTEE_PERIOD",
        parameters.shard_committee_period,
        max_bytes,
    )?;
    parameters.genesis_fork_version = config_version_override(
        case,
        "GENESIS_FORK_VERSION",
        parameters.genesis_fork_version,
        max_bytes,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    fn case(path: &Path) -> Case {
        Case {
            path: path.to_path_buf(),
            relative: String::new(),
            preset: String::new(),
            fork: String::new(),
            runner: String::new(),
            handler: String::new(),
            suite: String::new(),
            name: String::new(),
            configuration: Default::default(),
        }
    }

    #[test]
    fn nearest_overrides_are_reused_and_byte_limits_remain_independent() {
        let directory = TempDir::new().unwrap();
        let child = directory.path().join("case");
        reth_fs_util::create_dir_all(&child).unwrap();
        reth_fs_util::write(
            directory.path().join("config.yaml"),
            b"VALUE: 1\nVERSION: 0x01020304\n",
        )
        .unwrap();
        reth_fs_util::write(child.join("config.yaml"), b"VALUE: 2\n").unwrap();
        let case = case(&child);
        assert_eq!(config_override(&case, "VALUE", 0, 4096).unwrap(), 2);
        reth_fs_util::remove_file(child.join("config.yaml")).unwrap();
        assert_eq!(config_override(&case, "VALUE", 0, 4096).unwrap(), 2);
        assert_eq!(config_version_override(&case, "VERSION", [0; 4], 4096).unwrap(), [1, 2, 3, 4]);
        assert!(config_override(&case, "VALUE", 0, 1).is_err());
        assert_eq!(config_override(&case, "VALUE", 0, 8192).unwrap(), 1);
    }

    #[test]
    fn malformed_enclosing_value_cannot_be_hidden_by_nearest_override() {
        let config = FixtureConfig {
            layers: vec![
                Ok(serde_yaml::from_str("VALUE: invalid").unwrap()),
                Ok(serde_yaml::from_str("VALUE: 2").unwrap()),
            ],
        };
        assert_eq!(
            config.unsigned("VALUE", 0),
            Err("invalid unsigned config override: VALUE".into())
        );
    }
    #[test]
    fn errors_follow_reader_precedence_and_nearest_raw_values_keep_null() {
        let config = FixtureConfig {
            layers: vec![
                Ok(serde_yaml::from_str("VALUE: invalid").unwrap()),
                Err("malformed nearest YAML".into()),
            ],
        };
        assert_eq!(
            config.unsigned("VALUE", 0),
            Err("invalid unsigned config override: VALUE".into())
        );
        assert_eq!(config.value("VALUE"), Err("malformed nearest YAML".into()));
        let config = FixtureConfig {
            layers: vec![
                Err("malformed enclosing YAML".into()),
                Ok(serde_yaml::from_str("VALUE: null").unwrap()),
            ],
        };
        assert_eq!(config.value("VALUE"), Ok(Some(Value::Null)));
    }
}
