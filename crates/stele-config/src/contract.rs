//! Contract and factory configuration.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use stele_types::Address;

use crate::serde_util::deserialize_optional_one_or_many;
use crate::{AbiSource, ConfigError};

/// How the child address is extracted from a factory event.
///
/// TOML accepts a plain string for a nested parameter and a table for the
/// low-level locations:
///
/// ```toml
/// parameter = "pool"            # event argument name, including tuples ("pool.addr")
/// parameter = { topic = 2 }     # indexed topic 1..=3
/// parameter = { offset = 64 }   # byte offset into the non-indexed data
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FactoryParameter {
    /// ABI parameter path such as `pool` or `pool.address`.
    Nested(String),
    /// Indexed topic position, restricted to `1..=3`.
    Topic(u8),
    /// Byte offset into the event data.
    Offset(usize),
}

impl FactoryParameter {
    /// Validates the parameter shape.
    ///
    /// Only nested paths and topics can be invalid: the path may be blank and
    /// the topic must be between one and three.
    pub fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::Nested(path) => {
                if path.trim().is_empty() {
                    Err(ConfigError::EmptyFactoryParameter)
                } else {
                    Ok(())
                }
            }
            Self::Topic(topic) if (1..=3).contains(topic) => Ok(()),
            Self::Topic(topic) => Err(ConfigError::InvalidFactoryTopic(*topic)),
            Self::Offset(_) => Ok(()),
        }
    }
}

impl fmt::Display for FactoryParameter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Nested(path) => f.write_str(path),
            Self::Topic(topic) => write!(f, "topic{topic}"),
            Self::Offset(offset) => write!(f, "offset{offset}"),
        }
    }
}

/// Wire format used while deserializing [`FactoryParameter`].
#[derive(Deserialize)]
#[serde(untagged)]
enum FactoryParameterRepr {
    Nested(String),
    NestedTable(NestedParameter),
    Topic(TopicParameter),
    Offset(OffsetParameter),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NestedParameter {
    nested: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TopicParameter {
    topic: u8,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OffsetParameter {
    offset: usize,
}

impl<'de> Deserialize<'de> for FactoryParameter {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = FactoryParameterRepr::deserialize(deserializer)?;
        Ok(match repr {
            FactoryParameterRepr::Nested(path) => Self::Nested(path),
            FactoryParameterRepr::NestedTable(NestedParameter { nested }) => Self::Nested(nested),
            FactoryParameterRepr::Topic(TopicParameter { topic }) => Self::Topic(topic),
            FactoryParameterRepr::Offset(OffsetParameter { offset }) => Self::Offset(offset),
        })
    }
}

impl Serialize for FactoryParameter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        match self {
            Self::Nested(path) => serializer.serialize_str(path),
            Self::Topic(topic) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("topic", topic)?;
                map.end()
            }
            Self::Offset(offset) => {
                let mut map = serializer.serialize_map(Some(1))?;
                map.serialize_entry("offset", offset)?;
                map.end()
            }
        }
    }
}

/// A factory declaration on a contract.
///
/// The parent must be a contract registered in the same configuration; its ABI
/// must declare `event`. `start_block` and `end_block` override the child
/// contract's range when set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactoryConfig {
    /// Name of the registered parent contract that emits `event`.
    pub parent: String,
    /// Factory event name declared in the parent ABI.
    pub event: String,
    /// Location of the child address inside the factory event.
    pub parameter: FactoryParameter,
    /// First block to scan for factory events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_block: Option<u64>,
    /// Last block to scan for factory events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_block: Option<u64>,
}

/// Indexed contract configuration.
///
/// A contract must provide either `address` or `factory`; both may be present.
/// TOML accepts a single address string as well as an array, and `events`
/// accepts a single event name as well as an array.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContractConfig {
    /// Contract ABI, inline or loaded from a JSON file.
    pub abi: AbiSource,
    /// Name of a chain declared in `[chains]`.
    pub chain: String,
    /// Static addresses, or `None` for a factory-only contract.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_one_or_many",
        skip_serializing_if = "Option::is_none"
    )]
    pub address: Option<Vec<Address>>,
    /// Dynamic address list discovered from a factory event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factory: Option<FactoryConfig>,
    /// First block to index, inclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_block: Option<u64>,
    /// Last block to index, inclusive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_block: Option<u64>,
    /// Event names to index. `None` indexes every event in the ABI.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_one_or_many",
        skip_serializing_if = "Option::is_none"
    )]
    pub events: Option<Vec<String>>,
    /// Function names to index through call traces. Handlers are registered as
    /// `Contract.function()`.
    #[serde(
        default,
        deserialize_with = "deserialize_optional_one_or_many",
        skip_serializing_if = "Option::is_none"
    )]
    pub calls: Option<Vec<String>>,
}

impl ContractConfig {
    /// Returns the static address list, or an empty slice when absent.
    pub fn addresses(&self) -> &[Address] {
        self.address.as_deref().unwrap_or_default()
    }

    /// Returns `true` when the contract has at least one static address.
    pub fn has_address(&self) -> bool {
        self.address
            .as_ref()
            .is_some_and(|addresses| !addresses.is_empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_parameter(value: &str) -> FactoryParameter {
        #[derive(Deserialize)]
        struct Wrapper {
            parameter: FactoryParameter,
        }

        toml::from_str::<Wrapper>(value).unwrap().parameter
    }

    #[test]
    fn nested_topic_and_offset_parameters_parse() {
        assert_eq!(
            parse_parameter("parameter = \"pool\""),
            FactoryParameter::Nested("pool".to_owned())
        );
        assert_eq!(
            parse_parameter("parameter = { nested = \"pool.addr\" }"),
            FactoryParameter::Nested("pool.addr".to_owned())
        );
        assert_eq!(
            parse_parameter("parameter = { topic = 2 }"),
            FactoryParameter::Topic(2)
        );
        assert_eq!(
            parse_parameter("parameter = { offset = 64 }"),
            FactoryParameter::Offset(64)
        );
    }

    #[test]
    fn parameters_round_trip_through_toml() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Wrapper {
            parameter: FactoryParameter,
        }

        for parameter in [
            FactoryParameter::Nested("pool".to_owned()),
            FactoryParameter::Topic(1),
            FactoryParameter::Offset(0),
        ] {
            let encoded = toml::to_string(&Wrapper {
                parameter: parameter.clone(),
            })
            .unwrap();
            let decoded: Wrapper = toml::from_str(&encoded).unwrap();
            assert_eq!(decoded.parameter, parameter);
        }
    }

    #[test]
    fn parameter_validation_matches_stele_bounds() {
        assert!(
            FactoryParameter::Nested("pool".to_owned())
                .validate()
                .is_ok()
        );
        assert_eq!(
            FactoryParameter::Nested("  ".to_owned()).validate(),
            Err(ConfigError::EmptyFactoryParameter)
        );
        assert!(FactoryParameter::Topic(1).validate().is_ok());
        assert!(FactoryParameter::Topic(3).validate().is_ok());
        assert_eq!(
            FactoryParameter::Topic(0).validate(),
            Err(ConfigError::InvalidFactoryTopic(0))
        );
        assert_eq!(
            FactoryParameter::Topic(4).validate(),
            Err(ConfigError::InvalidFactoryTopic(4))
        );
        assert!(FactoryParameter::Offset(0).validate().is_ok());
    }

    #[test]
    fn addresses_accept_single_and_list_forms() {
        let single: ContractConfig = toml::from_str(
            r#"
abi = []
chain = "mainnet"
address = "0x0000000000000000000000000000000000000001"
events = "Transfer"
"#,
        )
        .unwrap();
        assert!(single.has_address());
        assert_eq!(single.addresses().len(), 1);
        assert_eq!(
            single.events,
            Some(vec!["Transfer".to_owned()]),
            "a single event name should normalize to a list"
        );

        let list: ContractConfig = toml::from_str(
            r#"
abi = []
chain = "mainnet"
address = [
  "0x0000000000000000000000000000000000000001",
  "0x0000000000000000000000000000000000000002",
]
"#,
        )
        .unwrap();
        assert_eq!(list.addresses().len(), 2);
        assert_eq!(list.events, None);
        assert!(list.factory.is_none());
    }
}
