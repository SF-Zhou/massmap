use std::hash::BuildHasher;
use std::io::{Error, ErrorKind, Result};

use foldhash::fast::FixedState;
use serde::{Deserialize, Serialize};

/// Configuration for the hash function used in a massmap.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MassMapHashConfig {
    /// Name of hash function used.
    pub name: String,
    /// Parameters for the hash function.
    pub parameters: serde_json::Value,
}

impl Default for MassMapHashConfig {
    fn default() -> Self {
        MassMapHashConfig {
            name: MassMapDefaultHashLoader::NAME.to_string(),
            parameters: serde_json::json!({ "seed": 0 }),
        }
    }
}

pub trait MassMapHashLoader {
    type BuildHasher: BuildHasher;

    fn load(config: &MassMapHashConfig) -> Result<Self::BuildHasher>;
}

#[derive(Debug, Default)]
pub struct MassMapDefaultHashLoader;

impl MassMapDefaultHashLoader {
    pub const NAME: &'static str = "foldhash";
}

impl MassMapHashLoader for MassMapDefaultHashLoader {
    type BuildHasher = FixedState;

    fn load(config: &MassMapHashConfig) -> Result<Self::BuildHasher> {
        if config.name != Self::NAME {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unsupported hash type: {}", config.name),
            ));
        }
        if let Some(seed) = config.parameters.get("seed").and_then(|v| v.as_u64()) {
            Ok(FixedState::with_seed(seed))
        } else {
            Err(Error::new(
                ErrorKind::InvalidData,
                "Missing or invalid 'seed' parameter for foldhash hash",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_hash_config() {
        let mut config = MassMapHashConfig::default();
        assert_eq!(config.name, "foldhash");
        assert_eq!(config.parameters["seed"], 0);
        let _ = MassMapDefaultHashLoader::load(&config).unwrap();

        config.parameters = serde_json::json!({});
        MassMapDefaultHashLoader::load(&config).unwrap_err();

        config.name = "unknown".to_string();
        MassMapDefaultHashLoader::load(&config).unwrap_err();
    }
}
