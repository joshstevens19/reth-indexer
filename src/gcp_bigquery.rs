use crate::{
    csv::CsvWriter,
    datasource::DatasourceWritable,
    types::{IndexerContractMapping, IndexerGcpBigQueryConfig},
};
use async_trait::async_trait;
use csv::ReaderBuilder;
use gcp_bigquery_client::{
    error::BQError,
    model::{
        table::Table, table_data_insert_all_request::TableDataInsertAllRequest,
        table_field_schema::TableFieldSchema, table_schema::TableSchema,
    },
    Client,
};
use indexmap::IndexMap;
use phf::phf_ordered_map;
use polars::prelude::*;
use serde_json::Value;
use std::{any::Any, collections::HashMap, fs::File};

///
/// Columns common to all tables
/// Each table's columns will include the following commmon fields
///
static COMMON_COLUMNS: phf::OrderedMap<&'static str, &'static str> = phf_ordered_map! {
    "indexed_id" => "string",  // will need to generate uuid in rust; postgres allows for autogenerate
    "contract_address" => "string",
    "tx_hash" => "string",
    "block_number" => "int",
    "block_hash" => "string",
    "timestamp" => "int"
};

/// Query client
/// Impl for this struct is further below
pub struct GcpBigQueryClient {
    client: Client,
    project_id: String,
    dataset_id: String,
    drop_tables: bool,
    table_map: HashMap<String, IndexMap<String, String>>,
}

#[derive(Debug)]
pub enum GcpClientError {
    MissingParameters,
    Other(String),
    BigQueryError(BQError),
}

///
/// Factory function to initialize the GCP bigquery client
/// Also deletes / creates tables as necessary (controlled via setting)
///
/// # Arguments
///
/// * `indexer_gcp_bigquery_config` - gcp config block
/// * `event_mappings` - list of event mapping blocks (all events to parse)
pub async fn init_gcp_bigquery_db(
    indexer_gcp_bigquery_config: &IndexerGcpBigQueryConfig,
    event_mappings: &[IndexerContractMapping],
) -> Result<GcpBigQueryClient, GcpClientError> {
    let gcp_bigquery = GcpBigQueryClient::new(&indexer_gcp_bigquery_config, &event_mappings)
        .await
        .unwrap();

    gcp_bigquery.delete_tables().await;
    gcp_bigquery.create_tables().await;

    Ok(gcp_bigquery)
}

///
/// This function merges the common columns specified in COMMON_COLUMNS with the
/// columns specified in the reth-indexer-config file for the particular event type to parse
/// The types are ordered to allow for consistent column order within the GCP tables
/// This is done for each table specified in the indexer
///
/// # Arguments
///
/// * `indexer_event_mappings` - list of all event mappings
pub fn load_table_configs(
    indexer_event_mappings: &[IndexerContractMapping],
) -> HashMap<String, IndexMap<String, String>> {
    //  Config map for all tables
    //  Ordered by common columns, then by configured mapped
    let mut all_tables: HashMap<String, IndexMap<String, String>> = HashMap::new();

    for mapping in indexer_event_mappings {
        for abi_item in mapping.decode_abi_items.iter() {
            let table_name = abi_item.name.to_lowercase();
            let column_type_map: IndexMap<String, String> = abi_item
                .inputs
                .iter()
                .map(|input| {
                    (
                        input.name.clone(),
                        solidity_type_to_db_type(input.type_.clone().as_str()).to_string(),
                    )
                })
                .collect();

            let common_column_type_map: IndexMap<String, String> = COMMON_COLUMNS
                .into_iter()
                .map(|it| (it.0.to_string(), it.1.to_string()))
                .collect();

            let merged_column_types: IndexMap<String, String> = common_column_type_map
                .into_iter()
                .chain(column_type_map)
                .collect();

            all_tables.insert(table_name, merged_column_types);
        }
    }

    all_tables
}

///
/// Maps solidity types (in indexer config) to bigquery column types
/// The configuration / reth uses solidity types, we map these to types that we'll use in GCP
/// bigquery
///
/// # Arguments
///
/// * `abi_type` - the ABI type, specified as a string
fn solidity_type_to_db_type(abi_type: &str) -> &str {
    match abi_type {
        "address" => "string",
        "bool" | "bytes" | "string" | "int256" | "uint256" => "string",
        "uint8" | "uint16" | "uint32" | "uint64" | "uint128" | "int8" | "int16" | "int32"
        | "int64" | "int128" => "int",
        _ => panic!("Unsupported type {}", abi_type),
    }
}

impl GcpBigQueryClient {
    ///
    /// Creates a new GcpBigQueryClient, which includes the connection to the GCP bigquery instance
    /// As well as project / dataset settings.
    /// Instance methods encapsulate all utilized behavior (create, delete, insertion/sync)
    ///
    /// # Arguments
    ///
    /// * indexer_gcp_bigquery_config: bigquery configurations
    /// * indexer_contract_mappings: contract event mappings
    ///
    pub async fn new(
        indexer_gcp_bigquery_config: &IndexerGcpBigQueryConfig,
        indexer_contract_mappings: &[IndexerContractMapping],
    ) -> Result<Self, GcpClientError> {
        let client = gcp_bigquery_client::Client::from_service_account_key_file(
            indexer_gcp_bigquery_config.credentials_path.as_str(),
        )
        .await;

        let table_map = load_table_configs(indexer_contract_mappings);

        match client {
            Err(error) => Err(GcpClientError::BigQueryError(error)),
            Ok(client) => Ok(GcpBigQueryClient {
                client,
                drop_tables: indexer_gcp_bigquery_config.drop_tables,
                project_id: indexer_gcp_bigquery_config.project_id.to_string(),
                dataset_id: indexer_gcp_bigquery_config.dataset_id.to_string(),
                table_map,
            }),
        }
    }

    ///
    /// Deletes tables from GCP bigquery, if they exist
    /// Tables are only deleted if the configuration has specified drop_table
    pub async fn delete_tables(&self) {
        if self.drop_tables {
            for table_name in self.table_map.keys() {
                let table_ref = self
                    .client
                    .table()
                    .get(
                        self.project_id.as_str(),
                        self.dataset_id.as_str(),
                        table_name.as_str(),
                        None,
                    )
                    .await;
                match table_ref {
                    Ok(table) => {
                        // Delete table, since it exists
                        println!("deleting table: {table_name}");
                        let _ = table.delete(&self.client).await;
                    }
                    Err(..) => {}
                }
            }
        }
    }

    ///
    /// Iterates through all defined tables, calls create_table on each table
    pub async fn create_tables(&self) {
        for (table_name, column_map) in self.table_map.iter() {
            self.create_table(table_name, column_map).await;
        }
    }

    ///
    /// Create a single table in GCP bigquery, from configured datatypes
    ///
    /// # Arguments
    ///
    /// * `table_name` - name of table
    /// * `column_map` - map of column names to types
    pub async fn create_table(&self, table_name: &str, column_map: &IndexMap<String, String>) {
        let dataset_ref = self
            .client
            .dataset()
            .get(self.project_id.as_str(), self.dataset_id.as_str())
            .await;
        let table_ref = self
            .client
            .table()
            .get(
                self.project_id.as_str(),
                self.dataset_id.as_str(),
                table_name,
                None,
            )
            .await;

        match table_ref {
            Ok(..) => {
                println!("Table {table_name} already exists, skip creation.");
            }
            Err(..) => {
                // Table does not exist (err), create
                // Transform config types to GCP api schema types
                let schema_types: Vec<TableFieldSchema> = column_map
                    .iter()
                    .map(|column| {
                        GcpBigQueryClient::db_type_to_table_field_schema(
                            &column.1.to_string(),
                            &column.0.to_string(),
                        )
                    })
                    .collect();

                let dataset = &mut dataset_ref.as_ref().unwrap();
                let res = dataset
                    .create_table(
                        &self.client,
                        Table::new(
                            self.project_id.as_str(),
                            self.dataset_id.as_str(),
                            table_name,
                            TableSchema::new(schema_types),
                        ),
                    )
                    .await;

                match res {
                    Ok(_) => {
                        println!("Created table in gcp: {}", table_name);
                    }
                    Err(err) => {
                        panic!("Failed to create table: {}, reason: {:?}", table_name, err);
                    }
                }
            }
        }
    }

    ///
    ///  Given CsvWriter object (which indexer writes to / manages)
    ///  Extract and write to GCP bigquery storage
    ///
    ///  # Arguments
    ///
    ///  * `table_name` - name of table for dataset
    ///  * `csv_writer` - csv writer reference
    pub async fn write_csv_to_storage(&self, table_name: &str, csv_writer: &CsvWriter) {
        let dataframe = self.read_csv_contents(table_name, csv_writer.path());
        let bq_rowmap_vector =
            self.build_bigquery_rowmap_vector(&dataframe, &self.table_map[table_name]);
        self.write_rowmaps_to_gcp(table_name, &bq_rowmap_vector)
            .await;
    }

    ///
    /// Read CSV
    /// Return dataframe, transform to vector and write to GCP
    ///
    /// # Arguments
    ///
    /// * `table_name` - name of table for dataset
    /// * `path` - path to csv file
    pub fn read_csv_contents(&self, table_name: &str, path: &str) -> DataFrame {
        //  Get list of columns in order, from csv file
        let file = File::open(path).unwrap();
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
        let headers = rdr.headers().unwrap();
        let column_names: Vec<String> = headers.iter().map(String::from).collect();

        //  Build column data types for polars dataframe, from column mapping
        let column_map = &self.table_map[table_name];
        let plr_col_types = Some(Arc::new(
            column_names
                .iter()
                .map(|name| match column_map[name].as_str() {
                    "int" => Field::new(name, DataType::Int64),
                    "string" => Field::new(name, DataType::Utf8),
                    _ => panic!("incompatible type found"),
                })
                .collect(),
        ));

        //  Read polars dataframe, w/ specified schema
        CsvReader::from_path(path)
            .expect("file does not exist")
            .has_header(true)
            .with_schema(plr_col_types)
            .finish()
            .unwrap()
    }

    ///
    /// Constructs GCP bigquery rows for insertion into gcp tables
    /// The writer will write vector of all rows into dataset
    ///
    /// # Arguments
    ///
    /// * `df` - dataframe
    /// * `table_config` - column to type mapping for table being written to
    ///
    pub fn build_bigquery_rowmap_vector(
        &self,
        df: &DataFrame,
        table_config: &IndexMap<String, String>,
    ) -> Vec<HashMap<String, Value>> {
        let column_names = df.get_column_names();
        let mut vec_of_maps: Vec<HashMap<String, Value>> = Vec::with_capacity(df.height());

        for idx in 0..df.height() {
            let mut row_map: HashMap<String, Value> = HashMap::new();

            for name in &column_names {
                let series = df.column(name).expect("Column should exist");
                let value = series.get(idx).expect("Value should exist");

                //  Convert from AnyValue (polars) to Value (generic value wrapper)
                //  The converted-to type is already specified in the inbound configuration
                let col_type = table_config
                    .get(&name.to_string())
                    .expect("Column should exist");
                let transformed_value = match col_type.as_str() {
                    "int" => GcpBigQueryClient::bigquery_anyvalue_numeric_type(&value),
                    "string" => Value::String(value.to_string()),
                    _ => Value::Null,
                };

                row_map.insert(name.clone().into(), transformed_value);
            }

            vec_of_maps.push(row_map);
        }

        vec_of_maps
    }

    ///
    /// Write vector of rowmaps into GCP.  Construct bulk insertion request and
    /// and insert into target remote table
    ///
    /// # Arguments
    ///
    /// * `table_name` - name of table being operated upon / written to
    /// * `vec_of_rowmaps` - vector of rowmap objects to facilitate write
    pub async fn write_rowmaps_to_gcp(
        &self,
        table_name: &str,
        vec_of_rowmaps: &Vec<HashMap<String, Value>>,
    ) {
        let mut insert_request = TableDataInsertAllRequest::new();
        for row_map in vec_of_rowmaps {
            let _ = insert_request.add_row(None, row_map.clone());
        }

        let result = self
            .client
            .tabledata()
            .insert_all(
                self.project_id.as_str(),
                self.dataset_id.as_str(),
                table_name,
                insert_request,
            )
            .await;

        match result {
            Ok(response) => println!("Success response: {:?}", response),
            Err(response) => println!("Failed, reason: {:?}", response),
        }
    }

    ///
    ///  Maps converted column types and constructs bigquery column
    ///
    /// # Arguments
    ///
    /// * `db_type` - stored datatype configured for this column
    /// * `name` - name of column
    pub fn db_type_to_table_field_schema(db_type: &str, name: &str) -> TableFieldSchema {
        match db_type {
            "int" => TableFieldSchema::integer(name),
            "string" => TableFieldSchema::string(name),
            _ => panic!("Unsupported db type: {}", db_type),
        }
    }

    ///
    ///  Converts AnyValue types to numeric types used in GCP api
    ///
    /// # Arguments
    ///
    /// * `value` - value and type
    pub fn bigquery_anyvalue_numeric_type(value: &AnyValue) -> Value {
        match value {
            AnyValue::Int8(val) => Value::Number(val.clone().into()),
            AnyValue::Int16(val) => Value::Number(val.clone().into()),
            AnyValue::Int32(val) => Value::Number(val.clone().into()),
            AnyValue::Int64(val) => Value::Number(val.clone().into()),
            AnyValue::UInt8(val) => Value::Number(val.clone().into()),
            AnyValue::UInt16(val) => Value::Number(val.clone().into()),
            AnyValue::UInt32(val) => Value::Number(val.clone().into()),
            AnyValue::UInt64(val) => Value::Number(val.clone().into()),
            _ => Value::Null,
        }
    }
}

///
/// Implements DatasourceWritable wrapper
/// Allows all writers to be treated uniformly
#[async_trait]
impl DatasourceWritable for GcpBigQueryClient {
    async fn write_data(&self, table_name: &str, csv_writer: &CsvWriter) {
        println!("   writing / sync bigquery table: {:?}", table_name);
        self.write_csv_to_storage(table_name, csv_writer).await;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
