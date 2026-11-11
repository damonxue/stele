//! ABI decoding for contract event logs and call traces.
//!
//! Decoding is deliberately separate from event matching: [`crate::events`]
//! decides which logs and traces reach a callback, and this module turns their
//! payloads into `serde_json::Value` arguments. A decoding failure is reported
//! as [`DecodeError`] so the engine can count and skip the event without
//! interrupting indexing.
//!
//! Values follow the viem JSON shape:
//!
//! - booleans become JSON booleans;
//! - integers become decimal strings, preserving the full `uint256` range;
//! - addresses, hashes, fixed bytes and dynamic bytes become lowercase `0x`
//!   hex strings;
//! - arrays and tuples become JSON arrays;
//! - structs become JSON objects keyed by their field names;
//! - event arguments become an object keyed by parameter name, or by position
//!   when a parameter is unnamed;
//! - call arguments become arrays in declaration order, matching
//!   `decodeFunctionData`.

use alloy_dyn_abi::{DecodedEvent, DynSolEvent, DynSolValue, FunctionExt, JsonAbiExt, Specifier};
use alloy_json_abi::{Event, Function, JsonAbi};
use serde_json::{Map, Value};
use stele_types::{Log, Trace};
use thiserror::Error;

/// What to decode and which ABI item to use.
#[derive(Clone, Debug)]
pub struct DecodeSpec {
    /// Name used in diagnostics, normally the handler key suffix.
    pub event_name: String,
    /// ABI containing the event or function.
    pub abi: JsonAbi,
    /// The item selected from `abi`.
    pub kind: DecodeKind,
}

/// The ABI item a [`DecodeSpec`] resolves to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DecodeKind {
    /// A log event such as `Transfer`.
    Log {
        /// Event name inside the ABI.
        event: String,
    },
    /// A contract function matched through a call trace.
    Function {
        /// Function name inside the ABI.
        function: String,
    },
}

/// Decoded arguments and, for calls, the decoded return value.
///
/// `result` is [`Value::Null`] for logs and for calls whose trace has no
/// output data.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedValues {
    pub args: Value,
    pub result: Value,
}

/// A decoding failure.
///
/// The variants distinguish a plan error (the ABI does not contain the named
/// item) from a payload error (the log or trace does not match the ABI item).
/// Both are skipped by the engine, but only the former indicates a
/// configuration mistake.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("event {name:?} is not present in the ABI")]
    UnknownEvent { name: String },
    #[error("function {name:?} is not present in the ABI")]
    UnknownFunction { name: String },
    #[error("event {name:?} cannot be resolved: {message}")]
    InvalidEvent { name: String, message: String },
    #[error("function {name:?} cannot be resolved: {message}")]
    InvalidFunction { name: String, message: String },
    #[error("failed to decode event {name:?}: {message}")]
    Log { name: String, message: String },
    #[error("failed to decode function {name:?} input: {message}")]
    Input { name: String, message: String },
    #[error("failed to decode function {name:?} output: {message}")]
    Output { name: String, message: String },
    #[error("cannot decode a function call without a four-byte selector")]
    MissingSelector,
    #[error("decode spec for {name:?} requires a trace but the event has none")]
    MissingTrace { name: String },
    #[error("decode spec for {name:?} requires a log but the event has none")]
    MissingLog { name: String },
}

/// Decodes the payload selected by `spec`.
///
/// `log` must be supplied for [`DecodeKind::Log`] and `trace` for
/// [`DecodeKind::Function`]; the unused argument is ignored.
pub fn decode(
    spec: &DecodeSpec,
    log: Option<&Log>,
    trace: Option<&Trace>,
) -> Result<DecodedValues, DecodeError> {
    match &spec.kind {
        DecodeKind::Log { event } => {
            let log = log.ok_or_else(|| DecodeError::MissingLog {
                name: spec.event_name.clone(),
            })?;
            Ok(DecodedValues {
                args: decode_log(&spec.abi, event, log)?,
                result: Value::Null,
            })
        }
        DecodeKind::Function { function } => {
            let trace = trace.ok_or_else(|| DecodeError::MissingTrace {
                name: spec.event_name.clone(),
            })?;
            decode_call(&spec.abi, function, trace)
        }
    }
}

/// Decodes `log` with the event called `name`.
pub fn decode_log(abi: &JsonAbi, name: &str, log: &Log) -> Result<Value, DecodeError> {
    let event = find_event(abi, name)?;
    let dyn_event: DynSolEvent = event.resolve().map_err(|error| DecodeError::InvalidEvent {
        name: name.to_owned(),
        message: error.to_string(),
    })?;
    let decoded = dyn_event
        .decode_log_parts(
            log.topics
                .iter()
                .map(|topic| alloy_primitives::B256::from(topic.0)),
            &log.data,
        )
        .map_err(|error| DecodeError::Log {
            name: name.to_owned(),
            message: error.to_string(),
        })?;
    Ok(event_args(event, decoded))
}

/// Decodes the input and output of the function called `name` from `trace`.
pub fn decode_call(abi: &JsonAbi, name: &str, trace: &Trace) -> Result<DecodedValues, DecodeError> {
    let function = find_function(abi, name)?;
    let input = trace.input.get(4..).ok_or(DecodeError::MissingSelector)?;
    let arguments = function
        .abi_decode_input(input)
        .map_err(|error| DecodeError::Input {
            name: name.to_owned(),
            message: error.to_string(),
        })?;

    let result = match trace.output.as_deref() {
        Some(output) if !output.is_empty() => {
            let values =
                function
                    .abi_decode_output(output)
                    .map_err(|error| DecodeError::Output {
                        name: name.to_owned(),
                        message: error.to_string(),
                    })?;
            flatten_output(values)
        }
        _ => Value::Null,
    };

    Ok(DecodedValues {
        args: Value::Array(arguments.iter().map(value_to_json).collect()),
        result,
    })
}

fn find_event<'a>(abi: &'a JsonAbi, name: &str) -> Result<&'a Event, DecodeError> {
    abi.event(name)
        .and_then(|events| events.first())
        .ok_or_else(|| DecodeError::UnknownEvent {
            name: name.to_owned(),
        })
}

fn find_function<'a>(abi: &'a JsonAbi, name: &str) -> Result<&'a Function, DecodeError> {
    abi.function(name)
        .and_then(|functions| functions.first())
        .ok_or_else(|| DecodeError::UnknownFunction {
            name: name.to_owned(),
        })
}

/// Interleaves the decoded indexed and non-indexed values in declaration order.
fn event_args(event: &Event, decoded: DecodedEvent) -> Value {
    let mut indexed = decoded.indexed.into_iter();
    let mut body = decoded.body.into_iter();
    let mut arguments = Map::with_capacity(event.inputs.len());
    for (position, input) in event.inputs.iter().enumerate() {
        let value = if input.indexed {
            indexed.next()
        } else {
            body.next()
        };
        let Some(value) = value else {
            // A successful `decode_log_parts` yields exactly one value per
            // input, so this is unreachable short of an alloy bug.
            continue;
        };
        let key = if input.name.is_empty() {
            position.to_string()
        } else {
            input.name.clone()
        };
        arguments.insert(key, value_to_json(&value));
    }
    Value::Object(arguments)
}

/// `decodeFunctionResult` returns the single output directly and an
/// array for multiple outputs. No outputs decode to `null`.
fn flatten_output(values: Vec<DynSolValue>) -> Value {
    match values.as_slice() {
        [] => Value::Null,
        [value] => value_to_json(value),
        values => Value::Array(values.iter().map(value_to_json).collect()),
    }
}

fn value_to_json(value: &DynSolValue) -> Value {
    match value {
        DynSolValue::Bool(value) => Value::Bool(*value),
        DynSolValue::Int(value, _) => Value::String(value.to_string()),
        DynSolValue::Uint(value, _) => Value::String(value.to_string()),
        DynSolValue::FixedBytes(word, size) => Value::String(hex(&word[..*size])),
        DynSolValue::Address(address) => Value::String(hex(address.as_slice())),
        DynSolValue::Function(function) => Value::String(hex(function.as_slice())),
        DynSolValue::Bytes(bytes) => Value::String(hex(bytes)),
        DynSolValue::String(value) => Value::String(value.clone()),
        DynSolValue::Array(values)
        | DynSolValue::FixedArray(values)
        | DynSolValue::Tuple(values) => Value::Array(values.iter().map(value_to_json).collect()),
    }
}

fn hex(bytes: &[u8]) -> String {
    alloy_primitives::hex::encode_prefixed(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_dyn_abi::JsonAbiExt;
    use alloy_primitives::{B256, U256};
    use serde_json::json;
    use stele_types::{Address, Topic};

    const ABI: &str = r#"[
        {
            "type": "event",
            "name": "Transfer",
            "anonymous": false,
            "inputs": [
                { "name": "from", "type": "address", "indexed": true },
                { "name": "to", "type": "address", "indexed": true },
                { "name": "amount", "type": "uint256", "indexed": false }
            ]
        },
        {
            "type": "event",
            "name": "Order",
            "anonymous": false,
            "inputs": [
                { "name": "id", "type": "uint256", "indexed": true },
                {
                    "name": "order",
                    "type": "tuple",
                    "indexed": false,
                    "components": [
                        { "name": "maker", "type": "address" },
                        { "name": "amount", "type": "uint256" }
                    ]
                },
                { "name": "memo", "type": "string", "indexed": false }
            ]
        },
        {
            "type": "function",
            "name": "transfer",
            "stateMutability": "nonpayable",
            "inputs": [
                { "name": "to", "type": "address" },
                { "name": "amount", "type": "uint256" }
            ],
            "outputs": [{ "name": "", "type": "bool" }]
        },
        {
            "type": "function",
            "name": "balanceOf",
            "stateMutability": "view",
            "inputs": [{ "name": "owner", "type": "address" }],
            "outputs": [{ "name": "", "type": "uint256" }]
        }
    ]"#;

    fn abi() -> JsonAbi {
        serde_json::from_str(ABI).expect("test ABI is valid")
    }

    fn spec(kind: DecodeKind) -> DecodeSpec {
        DecodeSpec {
            event_name: "Token".to_owned(),
            abi: abi(),
            kind,
        }
    }

    fn address(value: &str) -> Address {
        value.parse().expect("test address is valid")
    }

    fn word_address(address: Address) -> Topic {
        let mut word = [0; 32];
        word[12..].copy_from_slice(address.as_bytes());
        Topic(word)
    }

    fn word_u256(value: u64) -> [u8; 32] {
        U256::from(value).to_be_bytes()
    }

    #[test]
    fn decodes_a_log_into_named_arguments() {
        let from = address("0x0000000000000000000000000000000000000001");
        let to = address("0x0000000000000000000000000000000000000002");
        let event_abi = abi().event("Transfer").unwrap()[0].clone();
        let log = Log {
            block_number: 1,
            address: address("0x0000000000000000000000000000000000000009"),
            topics: vec![
                Topic(event_abi.selector().0),
                word_address(from),
                word_address(to),
            ],
            data: word_u256(1_000).to_vec(),
        };

        let decoded = decode(
            &spec(DecodeKind::Log {
                event: "Transfer".to_owned(),
            }),
            Some(&log),
            None,
        )
        .unwrap();

        assert_eq!(
            decoded.args,
            json!({
                "from": "0x0000000000000000000000000000000000000001",
                "to": "0x0000000000000000000000000000000000000002",
                "amount": "1000",
            })
        );
        assert_eq!(decoded.result, Value::Null);
    }

    #[test]
    fn decodes_tuples_strings_and_non_indexed_values() {
        let event_abi = abi().event("Order").unwrap()[0].clone();
        let maker = address("0x0000000000000000000000000000000000000003");
        // (address, uint256) head occupies two words, then the string tail.
        let mut data = Vec::new();
        data.extend_from_slice(&word_address(maker).0);
        data.extend_from_slice(&word_u256(42));
        data.extend_from_slice(&word_u256(96));
        data.extend_from_slice(&word_u256(3));
        let mut memo = [0u8; 32];
        memo[..3].copy_from_slice(b"hi!");
        data.extend_from_slice(&memo);

        let log = Log {
            block_number: 2,
            address: Address::ZERO,
            topics: vec![Topic(event_abi.selector().0), Topic(word_u256(7))],
            data,
        };

        let args = decode_log(&abi(), "Order", &log).unwrap();
        assert_eq!(
            args,
            json!({
                "id": "7",
                "order": [
                    "0x0000000000000000000000000000000000000003",
                    "42",
                ],
                "memo": "hi!",
            })
        );
    }

    #[test]
    fn decodes_function_input_and_output() {
        let function = abi().function("transfer").unwrap()[0].clone();
        let to = address("0x0000000000000000000000000000000000000005");
        let mut input = function
            .abi_encode_input(&[
                DynSolValue::Address(alloy_primitives::Address::from(to.0)),
                DynSolValue::Uint(U256::from(500), 256),
            ])
            .unwrap();
        assert_eq!(&input[..4], &function.selector()[..]);

        let trace = Trace {
            from: Address::ZERO,
            to: Some(Address::ZERO),
            call_type: stele_types::CallType::Call,
            input: std::mem::take(&mut input),
            value: None,
            output: Some(U256::from(1).to_be_bytes::<32>().to_vec()),
            outcome: stele_types::TraceOutcome::Successful,
        };

        let decoded = decode(
            &spec(DecodeKind::Function {
                function: "transfer".to_owned(),
            }),
            None,
            Some(&trace),
        )
        .unwrap();

        assert_eq!(
            decoded.args,
            json!(["0x0000000000000000000000000000000000000005", "500"])
        );
        assert_eq!(decoded.result, json!(true));
    }

    #[test]
    fn decodes_a_single_uint_output_without_an_array() {
        let function = abi().function("balanceOf").unwrap()[0].clone();
        let mut input = function
            .abi_encode_input(&[DynSolValue::Address(alloy_primitives::Address::from(
                [1u8; 20],
            ))])
            .unwrap();
        let trace = Trace {
            from: Address::ZERO,
            to: None,
            call_type: stele_types::CallType::StaticCall,
            input: std::mem::take(&mut input),
            value: None,
            output: Some(U256::from(9).to_be_bytes::<32>().to_vec()),
            outcome: stele_types::TraceOutcome::Successful,
        };

        let decoded = decode_call(&abi(), "balanceOf", &trace).unwrap();
        assert_eq!(
            decoded.args,
            json!(["0x0101010101010101010101010101010101010101"])
        );
        assert_eq!(decoded.result, json!("9"));
    }

    #[test]
    fn missing_output_decodes_to_null() {
        let function = abi().function("transfer").unwrap()[0].clone();
        let mut input = function
            .abi_encode_input(&[
                DynSolValue::Address(alloy_primitives::Address::ZERO),
                DynSolValue::Uint(U256::from(1), 256),
            ])
            .unwrap();
        let trace = Trace {
            from: Address::ZERO,
            to: None,
            call_type: stele_types::CallType::Call,
            input: std::mem::take(&mut input),
            value: None,
            output: None,
            outcome: stele_types::TraceOutcome::Successful,
        };
        assert_eq!(
            decode_call(&abi(), "transfer", &trace).unwrap().result,
            Value::Null
        );
    }

    #[test]
    fn unknown_items_and_bad_payloads_are_typed_errors() {
        let log = Log {
            block_number: 1,
            address: Address::ZERO,
            topics: vec![Topic([0; 32])],
            data: vec![0; 32],
        };
        assert_eq!(
            decode_log(&abi(), "Missing", &log),
            Err(DecodeError::UnknownEvent {
                name: "Missing".to_owned()
            })
        );
        assert!(matches!(
            decode_log(&abi(), "Transfer", &log),
            Err(DecodeError::Log { .. })
        ));

        let trace = Trace {
            from: Address::ZERO,
            to: None,
            call_type: stele_types::CallType::Call,
            input: vec![1, 2],
            value: None,
            output: None,
            outcome: stele_types::TraceOutcome::Successful,
        };
        assert_eq!(
            decode_call(&abi(), "transfer", &trace),
            Err(DecodeError::MissingSelector)
        );

        let missing = DecodeSpec {
            event_name: "Token".to_owned(),
            abi: JsonAbi::default(),
            kind: DecodeKind::Log {
                event: "Transfer".to_owned(),
            },
        };
        assert!(matches!(
            decode(&missing, Some(&log), None),
            Err(DecodeError::UnknownEvent { .. })
        ));
        assert!(matches!(
            decode(
                &spec(DecodeKind::Log {
                    event: "Transfer".to_owned()
                }),
                None,
                None
            ),
            Err(DecodeError::MissingLog { .. })
        ));
    }

    #[test]
    fn bytes_and_hash_values_use_lowercase_hex() {
        let value = DynSolValue::FixedBytes(B256::from([0xab; 32]), 32);
        assert_eq!(
            value_to_json(&value),
            json!("0xabababababababababababababababababababababababababababababababab")
        );
        assert_eq!(
            value_to_json(&DynSolValue::Bytes(vec![0x00, 0xff])),
            json!("0x00ff")
        );
        assert_eq!(
            value_to_json(&DynSolValue::Int(
                alloy_primitives::I256::try_from(-2).unwrap(),
                8
            )),
            json!("-2")
        );
        assert_eq!(value_to_json(&DynSolValue::Bool(true)), json!(true));
        assert_eq!(
            value_to_json(&DynSolValue::String("hi".to_owned())),
            json!("hi")
        );
    }
}
