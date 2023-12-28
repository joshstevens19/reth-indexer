use crate::{
    csv::CsvWriter,
    datasource::{load_table_configs, read_csv_to_polars, DatasourceWritable},
    types::{IndexerContractMapping, IndexerGcpBigQueryConfig},
};
use async_trait::async_trait;
use gcp_bigquery_client::{
    error::BQError,
    model::{
        table::Table, table_data_insert_all_request::TableDataInsertAllRequest,
        table_field_schema::TableFieldSchema, table_schema::TableSchema,
    },
    Client,
};
use indexmap::IndexMap;
// use phf::phf_ordered_map;
use polars::prelude::*;
use serde_json::Value;
use std::{any::Any, collections::HashMap};

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
    let gcp_bigquery = GcpBigQueryClient::new(indexer_gcp_bigquery_config, event_mappings).await?;

    // Drop table iff drop_table property is true
    if indexer_gcp_bigquery_config.drop_tables {
        let res = gcp_bigquery.delete_tables().await;
        match res {
            Ok(..) => {}
            Err(err) => return Err(GcpClientError::BigQueryError(err)),
        }
    }

    // Create - will create if tables do not exist
    let res = gcp_bigquery.create_tables().await;
    match res {
        Ok(..) => {}
        Err(err) => return Err(GcpClientError::BigQueryError(err)),
    }

    Ok(gcp_bigquery)
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
    pub async fn delete_tables(&self) -> Result<(), BQError> {
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

                if let Ok(table) = table_ref {
                    // Delete table, since it exists
                    let res = table.delete(&self.client).await;
                    match res {
                        Err(err) => {
                            return Err(err);
                        }
                        Ok(_) => println!("Removed table: {}", table_name),
                    }
                }
            }
        }

        Ok(())
    }

    ///
    /// Iterates through all defined tables, calls create_table on each table
    pub async fn create_tables(&self) -> Result<(), BQError> {
        for (table_name, column_map) in self.table_map.iter() {
            let res = self.create_table(table_name, column_map).await;
            match res {
                Ok(..) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    ///
    /// Create a single table in GCP bigquery, from configured datatypes
    ///
    /// # Arguments
    ///
    /// * `table_name` - name of table
    /// * `column_map` - map of column names to types
    pub async fn create_table(
        &self,
        table_name: &str,
        column_map: &IndexMap<String, String>,
    ) -> Result<(), BQError> {
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
                return Ok(());
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
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
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
        let column_map = &self.table_map[table_name];
        let dataframe = read_csv_to_polars(csv_writer.path(), column_map);
        let bq_rowmap_vector =
            self.build_bigquery_rowmap_vector(&dataframe, &self.table_map[table_name]);
        self.write_rowmaps_to_gcp(table_name, &bq_rowmap_vector)
            .await;
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

                row_map.insert((*name).into(), transformed_value);
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
            AnyValue::Int8(val) => Value::Number((*val).into()),
            AnyValue::Int16(val) => Value::Number((*val).into()),
            AnyValue::Int32(val) => Value::Number((*val).into()),
            AnyValue::Int64(val) => Value::Number((*val).into()),
            AnyValue::UInt8(val) => Value::Number((*val).into()),
            AnyValue::UInt16(val) => Value::Number((*val).into()),
            AnyValue::UInt32(val) => Value::Number((*val).into()),
            AnyValue::UInt64(val) => Value::Number((*val).into()),
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
