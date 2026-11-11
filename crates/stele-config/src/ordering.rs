//! Chain ordering modes.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize};

use crate::ConfigError;

/// How the runtime orders events across chains.
///
/// The isolated mode is `experimental_isolated`; the TOML parser accepts
/// `isolated`, `experimental_isolated`, `multichain`, and `omnichain`.
/// The default is [`OrderingMode::Multichain`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingMode {
    /// All chains share one global ordering, with deterministic tie-breaking.
    Omnichain,
    /// Each chain is ordered independently and events interleave at read time.
    #[default]
    Multichain,
    /// Each chain maintains fully isolated state. This is called
    /// `experimental_isolated`.
    Isolated,
}

impl OrderingMode {
    /// Returns the configuration token for this mode.
    ///
    /// The isolated mode renders as `"isolated"`, which the parser also
    /// accepts; it renders as `"experimental_isolated"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Omnichain => "omnichain",
            Self::Multichain => "multichain",
            Self::Isolated => "isolated",
        }
    }

    /// Returns `true` for [`OrderingMode::Isolated`].
    pub const fn is_isolated(self) -> bool {
        matches!(self, Self::Isolated)
    }
}

impl fmt::Display for OrderingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OrderingMode {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "omnichain" => Ok(Self::Omnichain),
            "multichain" => Ok(Self::Multichain),
            "isolated" | "experimental_isolated" => Ok(Self::Isolated),
            other => Err(ConfigError::InvalidOrdering(other.to_owned())),
        }
    }
}

/// Accepts both `ordering = "multichain"` and `[ordering] mode = "multichain"`.
#[derive(Deserialize)]
#[serde(untagged)]
enum OrderingRepr {
    Name(String),
    Section { mode: String },
}

impl<'de> Deserialize<'de> for OrderingMode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = match OrderingRepr::deserialize(deserializer)? {
            OrderingRepr::Name(name) => name,
            OrderingRepr::Section { mode } => mode,
        };
        name.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_supported_token() {
        assert_eq!(
            "omnichain".parse::<OrderingMode>(),
            Ok(OrderingMode::Omnichain)
        );
        assert_eq!(
            "multichain".parse::<OrderingMode>(),
            Ok(OrderingMode::Multichain)
        );
        assert_eq!(
            "experimental_isolated".parse::<OrderingMode>(),
            Ok(OrderingMode::Isolated)
        );
        assert_eq!(
            "isolated".parse::<OrderingMode>(),
            Ok(OrderingMode::Isolated)
        );
        assert_eq!(
            "bogus".parse::<OrderingMode>(),
            Err(ConfigError::InvalidOrdering("bogus".to_owned()))
        );
    }

    #[test]
    fn default_is_multichain() {
        assert_eq!(OrderingMode::default(), OrderingMode::Multichain);
        assert!(!OrderingMode::default().is_isolated());
    }
}
