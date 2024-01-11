use reth_primitives::Address;
use serde::Deserialize;
use std::path::PathBuf;

/// Represents an input parameter in the ABI.
#[derive(Debug, Deserialize, Clone)]
pub struct ABIInput {
    /// Indicates if the input parameter is indexed.
    pub indexed: bool,

    /// The internal type of the input parameter.
    #[serde(rename = "internalType")]
    pub internal_type: String,

    /// The name of the input parameter.
    pub name: String,

    /// The type of the input parameter.
    #[serde(rename = "type")]
    pub type_: String,

    #[serde(
        // deserialize_with = "deserialize_regex_option",
        rename = "rethRegexMatch"
    )]
    pub regex: Option<String>,
}

/// Represents an item in the ABI.
#[derive(Debug, Deserialize, Clone)]
pub struct ABIItem {
    /// The list of input parameters for the ABI item.
    pub inputs: Vec<ABIInput>,

    /// The name of the ABI item.
    pub name: String,

    // Apply custom indexes to the database
    pub custom_db_indexes: Option<Vec<Vec<String>>>,
}

/// Represents a contract mapping in the Indexer.
#[derive(Debug, Deserialize, Clone)]
pub struct IndexerContractMapping {
    /// The contract address.
    #[serde(rename = "filterByContractAddress")]
    // pub contract_address: Option<Address>,
    pub filter_by_contract_addresses: Option<Vec<Address>>,

    /// How often you should sync back to the postgres db.
    #[serde(rename = "syncBackRoughlyEveryNLogs")]
    pub sync_back_every_n_log: u64,

    /// The list of ABI items to decode.
    #[serde(rename = "decodeAbiItems")]
    pub decode_abi_items: Vec<ABIItem>,
}

fn default_false() -> bool {
    false
}

/// Represents a contract mapping in the Indexer.
#[derive(Debug, Deserialize)]
pub struct IndexerPostgresConfig {
    /// If true, the tables will be dropped and recreated before syncing.
    #[serde(rename = "dropTableBeforeSync")]
    pub drop_tables: bool,

    /// If true, it apply indexes before it syncs which is slower but means
    /// you can query the data straight away
    #[serde(rename = "applyIndexesBeforeSync")]
    #[serde(default = "default_false")]
    pub apply_indexes_before_sync: bool,

    /// The PostgreSQL connection string.
    #[serde(rename = "connectionString")]
    pub connection_string: String,
}

#[derive(Debug, Deserialize)]
pub struct IndexerGcpBigQueryConfig {
    #[serde(rename = "dropTableBeforeSync")]
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub drop_tables: bool,

    #[serde(rename = "projectId")]
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: String,

    #[serde(rename = "datasetId")]
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_id: String,

    #[serde(rename = "credentialsPath")]
    // #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_path: String,
}

#[derive(Debug, Deserialize)]
pub struct IndexerParquetConfig {
    #[serde(rename = "dropTableBeforeSync")]
    pub drop_tables: bool,

    #[serde(rename = "dataDirectory")]
    pub data_directory: String,
}

#[derive(Debug, Deserialize)]
pub struct IndexerConfig {
    /// The location of the rethDB.
    #[serde(rename = "rethDBLocation")]
    pub reth_db_location: PathBuf,

    /// The location of the CSV.
    #[serde(rename = "csvLocation")]
    pub csv_location: PathBuf,

    /// TODO support eth_transfers
    // #[serde(rename = "ethTransfers")]
    // pub include_eth_transfers: bool,

    /// The starting block number.
    #[serde(rename = "fromBlockNumber")]
    pub from_block: u64,

    /// The starting block number.
    #[serde(rename = "toBlockNumber")]
    pub to_block: Option<u64>,

    /// The postgres configuration.
    pub postgres: Option<IndexerPostgresConfig>,

    /// GCP configuration, if exists
    #[serde(rename = "gcpBigQuery", skip_serializing_if = "Option::is_none")]
    pub gcp_bigquery: Option<IndexerGcpBigQueryConfig>,

    /// parquet configuration, if exists
    #[serde(rename = "parquet", skip_serializing_if = "Option::is_none")]
    pub parquet: Option<IndexerParquetConfig>,

    /// The list of contract mappings.
    #[serde(rename = "eventMappings")]
    pub event_mappings: Vec<IndexerContractMapping>,
}
