//! ABI sources: inline JSON or a path on disk.

use std::{fmt, fs, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ConfigError;

/// Where a contract ABI comes from.
///
/// In TOML, a string is treated as a path and any other JSON value is treated
/// as an inline ABI:
///
/// ```toml
/// abi = "abis/Token.json"
/// # or
/// abi = [{ type = "event", name = "Transfer", inputs = [] }]
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AbiSource {
    /// A JSON file. Relative paths are resolved against the process working
    /// directory by [`AbiSource::load_json`]; [`crate::Config::from_file`]
    /// rewrites them to be relative to the configuration file's directory.
    Path(String),
    /// An ABI already present in the configuration.
    Inline(Value),
}

impl AbiSource {
    /// Loads and parses the ABI.
    ///
    /// Inline ABIs are deep-cloned; path ABIs are read from disk and parsed as
    /// JSON. Relative paths are resolved against the process working directory,
    /// so callers that own a configuration file should prefer
    /// [`crate::Config::from_file`], which rewrites paths during loading.
    pub fn load_json(&self) -> Result<Value, ConfigError> {
        match self {
            Self::Inline(value) => Ok(value.clone()),
            Self::Path(path) => {
                let contents = fs::read_to_string(path).map_err(|error| ConfigError::AbiRead {
                    path: path.clone(),
                    message: error.to_string(),
                })?;
                serde_json::from_str(&contents).map_err(|error| ConfigError::AbiParse {
                    path: path.clone(),
                    message: error.to_string(),
                })
            }
        }
    }

    /// Returns the configured path for [`AbiSource::Path`].
    pub fn path(&self) -> Option<&str> {
        match self {
            Self::Path(path) => Some(path),
            Self::Inline(_) => None,
        }
    }

    /// Returns `true` for [`AbiSource::Inline`].
    pub fn is_inline(&self) -> bool {
        matches!(self, Self::Inline(_))
    }

    /// Resolves a relative path against `base`, leaving inline ABIs and
    /// absolute paths untouched.
    pub fn resolve_relative_to(&self, base: &Path) -> Self {
        match self {
            Self::Path(path) => {
                let candidate = Path::new(path);
                if candidate.is_absolute() {
                    Self::Path(path.clone())
                } else {
                    Self::Path(base.join(candidate).to_string_lossy().into_owned())
                }
            }
            Self::Inline(value) => Self::Inline(value.clone()),
        }
    }
}

impl fmt::Display for AbiSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => f.write_str(path),
            Self::Inline(_) => f.write_str("<inline abi>"),
        }
    }
}

/// Returns the ABI item array from a raw ABI document.
///
/// Both a bare array and a compiler artifact object with an `abi` array are
/// accepted, matching what Foundry and Hardhat emit.
pub(crate) fn abi_items<'a>(value: &'a Value, source: &str) -> Result<&'a [Value], ConfigError> {
    match value {
        Value::Array(items) => Ok(items),
        Value::Object(map) => map
            .get("abi")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .ok_or_else(|| ConfigError::AbiInvalid {
                abi: source.to_owned(),
            }),
        _ => Err(ConfigError::AbiInvalid {
            abi: source.to_owned(),
        }),
    }
}

/// Returns `true` when the ABI declares an event with `name`.
pub(crate) fn abi_has_event(items: &[Value], name: &str) -> bool {
    items.iter().any(|item| {
        item.get("type").and_then(Value::as_str) == Some("event")
            && item.get("name").and_then(Value::as_str) == Some(name)
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;

    use super::*;

    #[test]
    fn inline_abi_is_returned_unchanged() {
        let inline = json!([{ "type": "event", "name": "Transfer", "inputs": [] }]);
        let source = AbiSource::Inline(inline.clone());
        assert!(source.is_inline());
        assert_eq!(source.path(), None);
        assert_eq!(source.load_json().unwrap(), inline);
    }

    #[test]
    fn path_abi_is_read_from_disk() {
        let directory = std::env::temp_dir().join(format!(
            "stele-config-abi-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("Token.json");
        fs::write(&path, r#"[{"type":"event","name":"Transfer","inputs":[]}]"#).unwrap();

        let source = AbiSource::Path(path.to_string_lossy().into_owned());
        assert_eq!(source.path(), Some(path.to_string_lossy().as_ref()));
        let value = source.load_json().unwrap();
        assert_eq!(value[0]["name"], json!("Transfer"));

        fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn missing_and_malformed_files_produce_clear_errors() {
        let missing = AbiSource::Path("/definitely/not/a/real/abi.json".to_owned());
        assert!(matches!(
            missing.load_json(),
            Err(ConfigError::AbiRead { .. })
        ));

        let directory = std::env::temp_dir().join(format!(
            "stele-config-abi-bad-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        fs::create_dir_all(&directory).unwrap();
        let path = directory.join("Broken.json");
        fs::write(&path, "not json").unwrap();
        let broken = AbiSource::Path(path.to_string_lossy().into_owned());
        assert!(matches!(
            broken.load_json(),
            Err(ConfigError::AbiParse { .. })
        ));
        fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn artifact_objects_and_bare_arrays_are_accepted() {
        let bare = json!([{ "type": "event", "name": "PoolCreated" }]);
        assert_eq!(abi_items(&bare, "Factory").unwrap().len(), 1);

        let artifact = json!({ "abi": [{ "type": "event", "name": "PoolCreated" }] });
        assert!(abi_has_event(
            abi_items(&artifact, "Factory").unwrap(),
            "PoolCreated"
        ));
        assert!(!abi_has_event(
            abi_items(&bare, "Factory").unwrap(),
            "Transfer"
        ));

        assert_eq!(
            abi_items(&json!({ "notAbi": [] }), "Factory"),
            Err(ConfigError::AbiInvalid {
                abi: "Factory".to_owned()
            })
        );
    }
}
