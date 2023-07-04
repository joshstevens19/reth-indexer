use alloy_sol_types::{sol_data, SolType};
use regex::RegexBuilder;
use reth_primitives::{hex::ToHex, keccak256, Address, Log, H256};

use crate::types::{ABIInput, ABIItem};

/// Converts an ABI item to a topic ID by calculating the Keccak-256 hash
/// of the item's name concatenated with its input types.
///
/// # Arguments
///
/// * `item` - The ABI item to convert.
///
/// # Returns
///
/// The topic ID as a `reth_primitives::H256` hash.
pub fn abi_item_to_topic_id(item: &ABIItem) -> H256 {
    let input_types: Vec<String> = item
        .inputs
        .iter()
        .map(|input| input.type_.clone())
        .collect();

    keccak256(format!("{}({})", item.name, input_types.join(",")))
}

#[derive(Debug)]
pub struct DecodedTopic {
    pub name: String,
    pub value: String,
}

/// Represents a decoded structure with a name and a corresponding value.
#[derive(Debug)]
pub struct DecodedLog {
    pub address: Address,
    pub topics: Vec<DecodedTopic>,
}

/// Decodes logs that match the specified topic ID using the given ABI.
///
/// # Arguments
///
/// * `topic_id` - The topic ID to match against the logs' first topic.
/// * `logs` - The logs to decode.
/// * `abi` - The ABI item used for decoding.
///
/// # Returns
///
/// An optional vector of decoded logs. If there are no matching logs, `None` is returned.
///
/// # Examples
///
/// ```
/// use your_crate::{H256, Log, ABIItem, DecodedLog};
///
/// let topic_id = H256::from_low_u64_be(123);
/// let logs: Vec<Log> = vec![/* logs here */];
/// let abi: ABIItem = /* ABI item here */;
///
/// let decoded_logs = decode_logs(topic_id, &logs, &abi);
/// if let Some(logs) = decoded_logs {
///     for log in logs {
///         // Process the decoded log
///     }
/// }
/// ```
pub fn decode_logs(topic_id: H256, logs: &[Log], abi: &ABIItem) -> Option<Vec<DecodedLog>> {
    let result: Vec<DecodedLog> = logs
        .iter()
        .filter_map(|log| {
            if log.topics.get(0) == Some(&topic_id) {
                decode_log(log, abi).ok()
            } else {
                None
            }
        })
        .collect();

    result.into_iter().next().map(|log| vec![log])
}

/// Decodes the value of a topic using the provided ABI input definition.
///
/// # Arguments
///
/// * `topic` - The topic value to decode as a byte slice.
/// * `abi` - The ABI input definition for the topic.
///
/// # Panics
///
/// This function will panic if the ABI input type is unknown.
///
/// # Returns
///
/// The decoded value as a string.
fn decode_topic_value(topic: &[u8], abi: &ABIInput) -> String {
    // TODO probably a nicer way to do this with sol_data directly PR welcome
    match abi.type_.as_str() {
        "address" => sol_data::Address::decode_single(topic, true)
            .unwrap()
            .to_checksum(None),
        "bool" => sol_data::Bool::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "bytes" => sol_data::Bytes::decode_single(topic, true)
            .unwrap()
            .encode_hex(),
        "string" => sol_data::String::decode_single(topic, true).unwrap(),
        "uint8" => sol_data::Uint::<8>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "uint16" => sol_data::Uint::<16>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "uint32" => sol_data::Uint::<32>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "uint64" => sol_data::Uint::<64>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "uint128" => sol_data::Uint::<128>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "uint256" => sol_data::Uint::<256>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "int8" => sol_data::Int::<8>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "int16" => sol_data::Int::<16>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "int32" => sol_data::Int::<32>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "int64" => sol_data::Int::<64>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "int128" => sol_data::Int::<128>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        "int256" => sol_data::Int::<256>::decode_single(topic, true)
            .unwrap()
            .to_string(),
        _ => panic!("Unknown type: {}", abi.type_),
    }
}

/// Matches a value against a regular expression pattern, ignoring case.
///
/// # Arguments
///
/// * `abi_name` - The name of the ABI item.
/// * `regex` - The regular expression pattern to match against.
/// * `value` - The value to match against the regular expression.
///
/// # Returns
///
/// Returns `true` if the value matches the regular expression pattern, ignoring case.
/// Otherwise, returns `false`.
///
/// # Panics
///
/// This function will panic if the provided regular expression is invalid.
fn regex_match(abi_name: &str, regex: &str, value: &str) -> bool {
    let regex_builder = RegexBuilder::new(regex)
        .case_insensitive(true)
        .build()
        .unwrap_or_else(|_| panic!("Your regex is invalid for {}", abi_name));
    regex_builder.is_match(value)
}

/// Decodes a log using the provided ABI and returns the decoded log.
///
/// This function decodes the indexed topics and non-indexed data of the log using the given ABI
/// definition. The decoded log is sorted based on the order of inputs in the ABI.
///
/// # Arguments
///
/// * `log` - The log to decode.
/// * `abi` - The ABI definition used for decoding.
///
/// # Returns
///
/// A `Result` containing the decoded log if decoding is successful, or an empty `()` if decoding fails.
///
/// # Examples
///
/// ```
/// use my_abi_lib::{Log, ABIItem, DecodedLog};
///
/// let log = Log { /* log data */ };
/// let abi = ABIItem { /* ABI definition */ };
///
/// match decode_log(&log, &abi) {
///     Ok(decoded_log) => {
///         // Handle the decoded log
///         println!("Decoded log: {:?}", decoded_log);
///     }
///     Err(_) => {
///         // Handle decoding error
///         println!("Failed to decode log");
///     }
/// }
/// ```
fn decode_log(log: &Log, abi: &ABIItem) -> Result<DecodedLog, ()> {
    let decoded_indexed_topics = decode_log_topics(log, abi)?;
    let decoded_non_indexed_data = decode_log_data(log, abi)?;

    let mut topics: Vec<DecodedTopic> = decoded_indexed_topics
        .into_iter()
        .chain(decoded_non_indexed_data.into_iter())
        .collect();

    topics.sort_by_key(|item| abi.inputs.iter().position(|input| input.name == item.name));

    Ok(DecodedLog {
        address: log.address,
        topics,
    })
}

/// Decodes a topic value based on the provided ABI input definition.
///
/// This function decodes the raw bytes of a topic into a human-readable value
/// based on the data type specified in the ABI input. It also performs a regex
/// match against the decoded value if a regex pattern is specified in the ABI input.
///
/// # Arguments
///
/// * `topic` - The raw bytes of the topic.
/// * `abi_input` - The ABI input definition.
///
/// # Returns
///
/// * `Result<DecodedTopic, ()>` - The decoded topic value wrapped in a `Result`.
///   - `Ok(DecodedTopic)` if the decoding and regex match are successful.
///   - `Err(())` if there is an error during decoding or the regex match fails.
///
fn decode_topic_log(topic: &[u8], abi_input: &ABIInput) -> Result<DecodedTopic, ()> {
    let value = decode_topic_value(topic, abi_input);

    // check regex and bail out if it doesn't match
    if let Some(regex) = &abi_input.regex {
        if !regex_match(&abi_input.name, regex, &value) {
            return Err(());
        }
    }

    Ok(DecodedTopic {
        name: abi_input.name.clone(),
        value,
    })
}

/// Decodes the indexed topics of a log based on the provided ABI item.
///
/// This function decodes the indexed topics of a log event based on the ABI item's
/// input definitions. It returns a vector of decoded topics, where each topic
/// consists of the name and value. The decoding is performed by calling the
/// `decode_topic_log` function for each topic.
///
/// # Arguments
///
/// * `log` - The log containing the indexed topics.
/// * `abi` - The ABI item definition.
///
/// # Returns
///
/// * `Result<Vec<DecodedTopic>, ()>` - The vector of decoded topics wrapped in a `Result`.
///   - `Ok(Vec<DecodedTopic>)` if the decoding is successful.
///   - `Err(())` if there is an error during decoding.
fn decode_log_topics(log: &Log, abi: &ABIItem) -> Result<Vec<DecodedTopic>, ()> {
    let indexed_inputs: Vec<&ABIInput> = abi
        .inputs
        .iter()
        .filter(|input| input.indexed)
        .collect::<Vec<_>>();

    if indexed_inputs.len() != log.topics.len() - 1 {
        // -1 because the first topic is the event signature
        return Err(());
    }

    let mut results: Vec<DecodedTopic> = Vec::<DecodedTopic>::new();

    for (i, topic) in log.topics.iter().enumerate().skip(1) {
        let abi_input = indexed_inputs[i - 1];
        results.push(decode_topic_log(topic.as_bytes(), abi_input)?);
    }

    Ok(results)
}

/// Decodes the non-indexed data of a log based on the provided ABI item.
///
/// This function decodes the non-indexed data of a log event based on the ABI item's
/// input definitions. It returns a vector of decoded topics, where each topic
/// consists of the name and value. The decoding is performed by calling the
/// `decode_topic_log` function for each data chunk.
///
/// # Arguments
///
/// * `log` - The log containing the non-indexed data.
/// * `abi` - The ABI item definition.
///
/// # Returns
///
/// * `Result<Vec<DecodedTopic>, ()>` - The vector of decoded topics wrapped in a `Result`.
///   - `Ok(Vec<DecodedTopic>)` if the decoding is successful.
///   - `Err(())` if there is an error during decoding.
///
fn decode_log_data(log: &Log, abi: &ABIItem) -> Result<Vec<DecodedTopic>, ()> {
    let non_indexed_inputs: Vec<&ABIInput> = abi
        .inputs
        .iter()
        .filter(|input| !input.indexed)
        .collect::<Vec<_>>();

    let topics = log.data.chunks_exact(32);

    if non_indexed_inputs.len() != topics.len() {
        return Err(());
    }

    let mut results = Vec::<DecodedTopic>::new();

    for (i, topic) in topics.enumerate() {
        let abi_input = non_indexed_inputs[i];
        results.push(decode_topic_log(topic, abi_input)?);
    }

    Ok(results)
}
