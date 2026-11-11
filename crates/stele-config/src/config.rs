//! The top-level configuration document.

use std::{collections::BTreeMap, fs, path::Path};

use serde::{Deserialize, Serialize};

use crate::abi::{abi_has_event, abi_items};
use crate::chain::{DatabaseConfig, validate_chains};
use crate::contract::{ContractConfig, FactoryConfig};
use crate::error::ConfigSection;
use crate::source::{AccountConfig, BlockConfig};
use crate::{ChainConfig, ConfigError, OrderingMode};

/// A complete configuration document.
///
/// Parse with [`Config::from_toml_str`] or [`Config::from_file`], then call
/// [`Config::validate`]. Parsing only checks the document shape; validation
/// enforces the cross-references and the numeric limits.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// Cross-chain ordering mode. Defaults to `multichain`.
    #[serde(default)]
    pub ordering: OrderingMode,
    /// PostgreSQL connection settings.
    #[serde(default)]
    pub database: DatabaseConfig,
    /// Chains keyed by name. Must not be empty after validation.
    #[serde(default)]
    pub chains: BTreeMap<String, ChainConfig>,
    /// Contracts keyed by name.
    #[serde(default)]
    pub contracts: BTreeMap<String, ContractConfig>,
    /// Accounts keyed by name.
    #[serde(default)]
    pub accounts: BTreeMap<String, AccountConfig>,
    /// Block intervals keyed by name.
    #[serde(default)]
    pub blocks: BTreeMap<String, BlockConfig>,
}

impl Config {
    /// Parses a configuration from a TOML string.
    ///
    /// Relative ABI paths are kept as written; they are resolved against the
    /// process working directory when loaded. Use [`Config::from_file`] to
    /// resolve them against the configuration file's directory instead.
    pub fn from_toml_str(value: &str) -> Result<Self, ConfigError> {
        toml::from_str(value).map_err(|error| ConfigError::TomlParse {
            message: error.to_string(),
        })
    }

    /// Reads and parses a configuration file.
    ///
    /// Relative ABI paths are rewritten relative to the directory containing
    /// the configuration file, so [`crate::AbiSource::load_json`] works no
    /// matter where the process was started from.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|error| ConfigError::ReadFile {
            path: path.display().to_string(),
            message: error.to_string(),
        })?;

        let mut config = Self::from_toml_str(&contents)?;
        if let Some(base) = path.parent().filter(|base| !base.as_os_str().is_empty()) {
            config.resolve_relative_abi_paths(base);
        }
        Ok(config)
    }

    /// Rewrites relative contract ABI paths to be relative to `base`.
    ///
    /// Inline ABIs and absolute paths are left untouched. [`Config::from_file`]
    /// calls this automatically.
    pub fn resolve_relative_abi_paths(&mut self, base: &Path) {
        for contract in self.contracts.values_mut() {
            contract.abi = contract.abi.resolve_relative_to(base);
        }
    }

    /// Validates the document against the configuration rules.
    ///
    /// Checks, in order:
    ///
    /// 1. At least one chain is declared and the database settings are
    ///    non-empty.
    /// 2. Every chain satisfies [`ChainConfig::validate`] and chain IDs are
    ///    unique.
    /// 3. Every contract, account, and block interval references a declared
    ///    chain.
    /// 4. Every contract has a non-empty address list or a factory.
    /// 5. Every factory parameter is valid, references a registered parent
    ///    contract, and names an event included in the parent ABI. Parent ABIs
    ///    are loaded, so an unreadable ABI file is reported here rather than
    ///    during indexing.
    /// 6. Every account has at least one address and every block interval is
    ///    greater than zero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.chains.is_empty() {
            return Err(ConfigError::EmptyChains);
        }
        self.database.validate()?;
        validate_chains(&self.chains)?;

        for (name, contract) in &self.contracts {
            self.validate_chain_reference(ConfigSection::Contracts, name, &contract.chain)?;
            if !contract.has_address() && contract.factory.is_none() {
                return Err(ConfigError::MissingContractAddress {
                    contract: name.clone(),
                });
            }
            if let Some(factory) = &contract.factory {
                self.validate_factory(name, factory)?;
            }
        }

        for (name, account) in &self.accounts {
            self.validate_chain_reference(ConfigSection::Accounts, name, &account.chain)?;
            if account.address.is_empty() {
                return Err(ConfigError::EmptyAddresses {
                    section: ConfigSection::Accounts,
                    name: name.clone(),
                });
            }
        }

        for (name, block) in &self.blocks {
            self.validate_chain_reference(ConfigSection::Blocks, name, &block.chain)?;
            if block.interval == 0 {
                return Err(ConfigError::BlockIntervalZero {
                    block: name.clone(),
                });
            }
        }

        Ok(())
    }

    fn validate_chain_reference(
        &self,
        section: ConfigSection,
        name: &str,
        chain: &str,
    ) -> Result<(), ConfigError> {
        if self.chains.contains_key(chain) {
            Ok(())
        } else {
            Err(ConfigError::UnknownChain {
                section,
                name: name.to_owned(),
                chain: chain.to_owned(),
            })
        }
    }

    fn validate_factory(&self, contract: &str, factory: &FactoryConfig) -> Result<(), ConfigError> {
        factory.parameter.validate()?;

        let parent = self.contracts.get(&factory.parent).ok_or_else(|| {
            ConfigError::FactoryParentMissing {
                contract: contract.to_owned(),
                parent: factory.parent.clone(),
            }
        })?;

        let abi = parent.abi.load_json()?;
        let items = abi_items(&abi, &factory.parent)?;
        if !abi_has_event(items, &factory.event) {
            return Err(ConfigError::FactoryEventMissing {
                contract: contract.to_owned(),
                parent: factory.parent.clone(),
                event: factory.event.clone(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use serde_json::json;
    use stele_types::Address;

    use super::*;
    use crate::{AbiSource, FactoryParameter};

    const ADDRESS_1: &str = "0x0000000000000000000000000000000000000001";
    const ADDRESS_2: &str = "0x0000000000000000000000000000000000000002";

    /// The example document from the API contract.
    const CONTRACT_EXAMPLE: &str = r#"
ordering = "multichain"

[database]
url = "postgres://localhost/stele"
schema = "public"

[chains.mainnet]
id = 1
rpc = ["https://eth.llamarpc.com"]
polling_interval = 1000
reorg_window = 180
cache_rpc_requests = true

[contracts.Token]
abi = "abis/Token.json"
chain = "mainnet"
address = ["0x0000000000000000000000000000000000000001"]
start_block = 1
end_block = 1000
events = ["Transfer", "Approval"]

[contracts.Pool]
abi = "abis/Pool.json"
chain = "mainnet"
start_block = 1
[contracts.Pool.factory]
parent = "Factory"
event = "PoolCreated"
parameter = "pool"

[accounts.Alice]
chain = "mainnet"
address = ["0x0000000000000000000000000000000000000002"]
start_block = 1

[blocks.hourly]
chain = "mainnet"
interval = 300
offset = 0
start_block = 1
"#;

    /// Base document with a single chain and database.
    fn base_document() -> &'static str {
        r#"
[database]
url = "postgres://localhost/stele"

[chains.mainnet]
id = 1
"#
    }

    fn factory_document(abi: &str, parameter: &str) -> String {
        format!(
            r#"
{base}

[contracts.Factory]
abi = {abi}
chain = "mainnet"
address = ["{ADDRESS_1}"]

[contracts.Pool]
abi = []
chain = "mainnet"
[contracts.Pool.factory]
parent = "Factory"
event = "PoolCreated"
parameter = {parameter}
"#,
            base = base_document()
        )
    }

    fn parse_contract_example() -> Config {
        Config::from_toml_str(CONTRACT_EXAMPLE).expect("example document should parse")
    }

    #[test]
    fn parses_the_contract_example() {
        let config = parse_contract_example();

        assert_eq!(config.ordering, OrderingMode::Multichain);
        assert_eq!(config.database.url, "postgres://localhost/stele");
        assert_eq!(config.database.schema, "public");

        let chain = &config.chains["mainnet"];
        assert_eq!(chain.id, 1);
        assert_eq!(chain.rpc, vec!["https://eth.llamarpc.com".to_owned()]);
        assert_eq!(chain.polling_interval_ms, 1_000);
        assert_eq!(chain.reorg_window_seconds, 180);
        assert!(chain.cache_rpc_requests);

        let token = &config.contracts["Token"];
        assert_eq!(token.chain, "mainnet");
        assert_eq!(
            token.address,
            Some(vec![ADDRESS_1.parse::<Address>().unwrap()])
        );
        assert_eq!(token.start_block, Some(1));
        assert_eq!(token.end_block, Some(1_000));
        assert_eq!(
            token.events,
            Some(vec!["Transfer".to_owned(), "Approval".to_owned()])
        );

        let pool = &config.contracts["Pool"];
        assert_eq!(pool.address, None);
        let factory = pool.factory.as_ref().expect("factory should parse");
        assert_eq!(factory.parent, "Factory");
        assert_eq!(factory.event, "PoolCreated");
        assert_eq!(
            factory.parameter,
            FactoryParameter::Nested("pool".to_owned())
        );
        assert_eq!(factory.start_block, None);

        let alice = &config.accounts["Alice"];
        assert_eq!(alice.address, vec![ADDRESS_2.parse::<Address>().unwrap()]);
        assert!(alice.include_transactions);
        assert!(alice.include_transfers);

        let hourly = &config.blocks["hourly"];
        assert_eq!(hourly.chain, "mainnet");
        assert_eq!(hourly.interval, 300);
        assert_eq!(hourly.offset, 0);
        assert_eq!(hourly.start_block, Some(1));
    }

    #[test]
    fn contract_example_reports_the_unregistered_factory_parent() {
        let config = parse_contract_example();
        assert_eq!(
            config.validate(),
            Err(ConfigError::FactoryParentMissing {
                contract: "Pool".to_owned(),
                parent: "Factory".to_owned(),
            })
        );
    }

    #[test]
    fn defaults_match_documentation() {
        let config = Config::from_toml_str(&format!(
            r#"
[database]
url = "postgres://localhost/stele"

[chains.mainnet]
id = 1

[accounts.Alice]
chain = "mainnet"
address = "{ADDRESS_1}"
"#
        ))
        .unwrap();

        assert_eq!(config.ordering, OrderingMode::Multichain);
        assert_eq!(config.database.schema, "public");
        assert!(config.contracts.is_empty());
        assert!(config.blocks.is_empty());

        let chain = &config.chains["mainnet"];
        assert_eq!(chain.polling_interval_ms, 1_000);
        assert_eq!(chain.reorg_window_seconds, 180);
        assert!(chain.cache_rpc_requests);
        assert!(chain.rpc.is_empty());

        let account = &config.accounts["Alice"];
        assert!(account.include_transactions);
        assert!(account.include_transfers);

        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn ordering_accepts_a_section_form() {
        let config = Config::from_toml_str(&format!(
            r#"
[ordering]
mode = "experimental_isolated"

{base}
"#,
            base = base_document()
        ))
        .unwrap();
        assert_eq!(config.ordering, OrderingMode::Isolated);
        assert!(config.ordering.is_isolated());
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn ordering_rejects_unknown_modes() {
        let error = Config::from_toml_str(&format!(
            r#"
ordering = "not-a-mode"

{base}
"#,
            base = base_document()
        ))
        .unwrap_err();

        match error {
            ConfigError::TomlParse { message } => {
                assert!(
                    message.contains("invalid ordering 'not-a-mode'"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected a TOML parse error, got {other:?}"),
        }
    }

    #[test]
    fn requires_at_least_one_chain() {
        let config = Config::from_toml_str(
            r#"
[database]
url = "postgres://localhost/stele"
"#,
        )
        .unwrap();
        assert_eq!(config.validate(), Err(ConfigError::EmptyChains));
    }

    #[test]
    fn rejects_duplicate_chain_ids() {
        let config = Config::from_toml_str(
            r#"
[database]
url = "postgres://localhost/stele"

[chains.mainnet]
id = 1

[chains.optimism]
id = 1
"#,
        )
        .unwrap();
        assert_eq!(
            config.validate(),
            Err(ConfigError::DuplicateChainId {
                id: 1,
                first: "mainnet".to_owned(),
                second: "optimism".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_unknown_chain_references() {
        let contract = Config::from_toml_str(&format!(
            r#"
{base}

[contracts.Token]
abi = []
chain = "goerli"
address = ["{ADDRESS_1}"]
"#,
            base = base_document()
        ))
        .unwrap();
        assert_eq!(
            contract.validate(),
            Err(ConfigError::UnknownChain {
                section: ConfigSection::Contracts,
                name: "Token".to_owned(),
                chain: "goerli".to_owned(),
            })
        );

        let account = Config::from_toml_str(&format!(
            r#"
{base}

[accounts.Alice]
chain = "goerli"
address = ["{ADDRESS_1}"]
"#,
            base = base_document()
        ))
        .unwrap();
        assert_eq!(
            account.validate(),
            Err(ConfigError::UnknownChain {
                section: ConfigSection::Accounts,
                name: "Alice".to_owned(),
                chain: "goerli".to_owned(),
            })
        );

        let blocks = Config::from_toml_str(&format!(
            r#"
{base}

[blocks.hourly]
chain = "goerli"
interval = 10
"#,
            base = base_document()
        ))
        .unwrap();
        assert_eq!(
            blocks.validate(),
            Err(ConfigError::UnknownChain {
                section: ConfigSection::Blocks,
                name: "hourly".to_owned(),
                chain: "goerli".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_contracts_without_address_or_factory() {
        let config = Config::from_toml_str(&format!(
            r#"
{base}

[contracts.Token]
abi = []
chain = "mainnet"
address = []
"#,
            base = base_document()
        ))
        .unwrap();
        assert_eq!(
            config.validate(),
            Err(ConfigError::MissingContractAddress {
                contract: "Token".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_factory_without_registered_parent() {
        let config = Config::from_toml_str(&format!(
            r#"
{base}

[contracts.Pool]
abi = []
chain = "mainnet"
[contracts.Pool.factory]
parent = "Factory"
event = "PoolCreated"
parameter = "pool"
"#,
            base = base_document()
        ))
        .unwrap();
        assert_eq!(
            config.validate(),
            Err(ConfigError::FactoryParentMissing {
                contract: "Pool".to_owned(),
                parent: "Factory".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_factories_whose_event_is_absent_from_the_parent_abi() {
        let abi = r#"[{ type = "event", name = "Transfer", inputs = [] }]"#;
        let config = Config::from_toml_str(&factory_document(abi, "\"pool\"")).unwrap();
        assert_eq!(
            config.validate(),
            Err(ConfigError::FactoryEventMissing {
                contract: "Pool".to_owned(),
                parent: "Factory".to_owned(),
                event: "PoolCreated".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_invalid_factory_topics() {
        let abi = r#"[{ type = "event", name = "PoolCreated", inputs = [] }]"#;

        let zero = Config::from_toml_str(&factory_document(abi, "{ topic = 0 }")).unwrap();
        assert_eq!(zero.validate(), Err(ConfigError::InvalidFactoryTopic(0)));

        let four = Config::from_toml_str(&factory_document(abi, "{ topic = 4 }")).unwrap();
        assert_eq!(four.validate(), Err(ConfigError::InvalidFactoryTopic(4)));

        let valid = Config::from_toml_str(&factory_document(abi, "{ topic = 3 }")).unwrap();
        assert_eq!(valid.validate(), Ok(()));

        let offset = Config::from_toml_str(&factory_document(abi, "{ offset = 64 }")).unwrap();
        assert_eq!(offset.validate(), Ok(()));

        let nested = Config::from_toml_str(&factory_document(abi, "\"pool\"")).unwrap();
        assert_eq!(nested.validate(), Ok(()));
    }

    #[test]
    fn validates_factories_with_inline_artifact_abis() {
        let abi = r#"{ abi = [{ type = "event", name = "PoolCreated", inputs = [] }] }"#;
        let config = Config::from_toml_str(&factory_document(abi, "\"pool\"")).unwrap();
        assert_eq!(config.validate(), Ok(()));
    }

    #[test]
    fn rejects_empty_and_zero_valued_sources() {
        let account = Config::from_toml_str(&format!(
            r#"
{base}

[accounts.Alice]
chain = "mainnet"
address = []
"#,
            base = base_document()
        ))
        .unwrap();
        assert_eq!(
            account.validate(),
            Err(ConfigError::EmptyAddresses {
                section: ConfigSection::Accounts,
                name: "Alice".to_owned(),
            })
        );

        let blocks = Config::from_toml_str(&format!(
            r#"
{base}

[blocks.hourly]
chain = "mainnet"
interval = 0
"#,
            base = base_document()
        ))
        .unwrap();
        assert_eq!(
            blocks.validate(),
            Err(ConfigError::BlockIntervalZero {
                block: "hourly".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_invalid_chain_limits() {
        for (document, expected) in [
            (
                "[database]\nurl = \"x\"\n[chains.mainnet]\nid = 1\npolling_interval = 99\n",
                ConfigError::PollingIntervalTooSmall(99),
            ),
            (
                "[database]\nurl = \"x\"\n[chains.mainnet]\nid = 1\nreorg_window = 601\n",
                ConfigError::ReorgWindowTooLarge(601),
            ),
        ] {
            let config = Config::from_toml_str(document).unwrap();
            assert_eq!(config.validate(), Err(expected));
        }
    }

    #[test]
    fn rejects_invalid_database_settings() {
        let missing_url = Config::from_toml_str("[chains.mainnet]\nid = 1\n").unwrap();
        assert_eq!(missing_url.validate(), Err(ConfigError::EmptyDatabaseUrl));

        let empty_schema = Config::from_toml_str(
            "[database]\nurl = \"x\"\nschema = \"\"\n[chains.mainnet]\nid = 1\n",
        )
        .unwrap();
        assert_eq!(
            empty_schema.validate(),
            Err(ConfigError::EmptyDatabaseSchema)
        );
    }

    #[test]
    fn inline_and_path_abis_load() {
        let inline = AbiSource::Inline(json!([{ "type": "event", "name": "Transfer" }]));
        assert_eq!(inline.load_json().unwrap()[0]["name"], json!("Transfer"));

        let directory = TempDir::new("abi-load");
        directory.write(
            "Factory.json",
            r#"[{"type":"event","name":"PoolCreated","inputs":[]}]"#,
        );
        let path = directory.path().join("Factory.json");
        let source = AbiSource::Path(path.to_string_lossy().into_owned());
        let value = source.load_json().unwrap();
        assert_eq!(value[0]["name"], json!("PoolCreated"));
    }

    #[test]
    fn from_file_resolves_relative_abi_paths() {
        let directory = TempDir::new("from-file");
        directory.write(
            "abis/Factory.json",
            r#"[{"type":"event","name":"PoolCreated","inputs":[{"name":"pool","type":"address","indexed":false}]}]"#,
        );
        directory.write("abis/Pool.json", "[]");
        directory.write(
            "stele.toml",
            &format!(
                r#"
[database]
url = "postgres://localhost/stele"

[chains.mainnet]
id = 1

[contracts.Factory]
abi = "abis/Factory.json"
chain = "mainnet"
address = ["{ADDRESS_1}"]

[contracts.Pool]
abi = "abis/Pool.json"
chain = "mainnet"
[contracts.Pool.factory]
parent = "Factory"
event = "PoolCreated"
parameter = "pool"
"#
            ),
        );

        let config = Config::from_file(directory.path().join("stele.toml")).unwrap();
        assert_eq!(config.validate(), Ok(()));

        for name in ["Factory", "Pool"] {
            let abi = &config.contracts[name].abi;
            let path = abi.path().expect("path should be preserved");
            assert!(
                Path::new(path).is_absolute(),
                "path for {name} was not resolved: {path}"
            );
            assert!(abi.load_json().is_ok());
        }
    }

    #[test]
    fn from_file_reports_unreadable_and_malformed_abis() {
        let directory = TempDir::new("abi-errors");
        directory.write(
            "stele.toml",
            r#"
[database]
url = "postgres://localhost/stele"

[chains.mainnet]
id = 1

[contracts.Factory]
abi = "abis/Missing.json"
chain = "mainnet"
address = ["0x0000000000000000000000000000000000000001"]

[contracts.Pool]
abi = []
chain = "mainnet"
[contracts.Pool.factory]
parent = "Factory"
event = "PoolCreated"
parameter = "pool"
"#,
        );
        let missing = Config::from_file(directory.path().join("stele.toml")).unwrap();
        assert!(matches!(
            missing.validate(),
            Err(ConfigError::AbiRead { .. })
        ));

        directory.write("abis/Missing.json", "not json");
        let malformed = Config::from_file(directory.path().join("stele.toml")).unwrap();
        assert!(matches!(
            malformed.validate(),
            Err(ConfigError::AbiParse { .. })
        ));
    }

    #[test]
    fn from_file_reports_missing_configuration_files() {
        let missing = PathBuf::from("/definitely/not/a/real/stele.toml");
        assert!(matches!(
            Config::from_file(missing),
            Err(ConfigError::ReadFile { .. })
        ));
    }

    #[test]
    fn config_round_trips_through_toml() {
        let config = Config::from_toml_str(&factory_document(
            r#"[{ type = "event", name = "PoolCreated", inputs = [] }]"#,
            "{ topic = 1 }",
        ))
        .unwrap();
        let encoded = toml::to_string(&config).unwrap();
        let decoded = Config::from_toml_str(&encoded).unwrap();
        assert_eq!(decoded, config);
    }

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let mut path = std::env::temp_dir();
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after the epoch")
                .as_nanos();
            path.push(format!("stele-config-{label}-{nanos}"));
            fs::create_dir_all(&path).expect("temp directory should be creatable");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn write(&self, relative: &str, contents: &str) {
            let path = self.0.join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).expect("parent directory should be creatable");
            }
            fs::write(path, contents).expect("file should be writable");
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}
