mod api;
mod csv;
mod decode_events;
mod indexer;
mod postgres;
mod provider;
mod types;

use std::any::Any;
use std::collections::HashMap;
use std::{fs::File, io::Read, path::Path};
use types::{IndexerConfig, IndexerGcpBigQueryConfig};
use indexmap::{IndexMap};

use phf::{phf_map, phf_ordered_map};
use std::env;
use gcp_bigquery_client::{self, error::BQError, dataset::ListOptions, model::{table::Table, table_schema::TableSchema, table_field_schema::TableFieldSchema, table_data_insert_all_request::TableDataInsertAllRequest}};
use std::iter::zip;

use polars::prelude::*;

use serde_json::Value;
use uuid::Uuid;

// 1. read configuration: i.e. table definitions
// 2. read / define configuration: gcp section
// 3. delete tables call
// 4. create tables call (+ create columns vector)
// 5. read and parse csv columns 


// All column definitions, but eth xfer table
static ETH_TRANSFER_COLUMNS: phf::OrderedMap<&'static str, &'static str> = phf_ordered_map! {
    "indexed_id" => "string",
    "from" => "string",
    "to" => "string",
    "value" => "int",
    "tx_hash" => "string",
    "block_number" => "int",
    "block_hash" => "string",
    "timestamp" => "int"
};

// Common columns to be added to each contract 
static COMMON_COLUMNS: phf::OrderedMap<&'static str, &'static str> = phf_ordered_map! {
    "indexed_id" => "string",  // will need to generate uuid in rust; postgres allows for autogenerate
    "contract_address" => "string",
    "tx_hash" => "string",
    "block_number" => "int",
    "block_hash" => "string",
    "timestamp" => "int"   
};


#[tokio::main]
async fn main() {

    env_logger::init();

    // test_static_maps();
    let table_configs: HashMap<String, _> = test_read_config();


    println!("table configs: {:?}", table_configs);
    let gcp_config = test_read_config_gcp().unwrap();


    // Drop + create tables, if setting is enabled
    test_ddls(&table_configs, &gcp_config).await;


    let df = test_read_csv_contents();
    
    let table_config = table_configs.get("Transfer").expect("Unexpected table name");
    let vec_of_row_maps = build_row_map_list(&df, &table_config);

    // println!("{:?}", vec_of_row_maps);

    insert_to_gcp_bq(vec_of_row_maps).await;


    // test_uuid();
    // test_ddls(&table_configs, &gcp_config).await; 
    // println!("table_configs: {:?}", table_configs);
    // println!("gcp_config: {:?}", gcp_config);
    // println!("Hello, world!  in Example");
}


async fn insert_to_gcp_bq(
    vec_of_row_maps: Vec<HashMap<String, Value>>, 
) {

    let mut insert_request = TableDataInsertAllRequest::new();
    for row_map in vec_of_row_maps {
        insert_request.add_row(None, row_map.clone());
    }

    let project_id = "economic-data-391303";
    let dataset_id = "reth_index_a";
    let google_credentials_path = "/Users/jonathanl/.secrets/economic-data-391303-6c338b794579.json";


    let client = gcp_bigquery_client::Client::from_service_account_key_file(google_credentials_path).await.unwrap();
    let result = client.tabledata().insert_all(
        project_id, dataset_id, "Transfer", insert_request
    ).await;

    match result {
        Ok(response) => println!("success?: {:?}", response),
        Err(response) => println!("Failed, reason: {:?}", response)
    };

}

/// Convert polars dataframe to list of hashmaps (columns => values)
/// This will be utilized in the insertion request to GCP
fn build_row_map_list(df: &DataFrame, table_config: &IndexMap<String, String>) -> Vec<HashMap<String, Value>> {

    let column_names = df.get_column_names();
    let mut vec_of_maps: Vec<HashMap<String, Value>> = Vec::with_capacity(df.height());

    for idx in 0..df.height() {
        let mut row_map: HashMap<String, Value> = HashMap::new();

        for name in &column_names {
            let series = df.column(name).expect("Column should exist");
            let value = series.get(idx).expect("Value should exist");

            //  Convert from AnyValue (polars) to Value (generic value wrapper)
            //  The converted-to type is already specified in the inbound configuration
            let col_type = table_config.get(&name.to_string()).expect("Column should exist");
            let transformed_value = match col_type.as_str() {
                "int" => handle_any_value_numeric(&value),
                "string" => Value::String(value.to_string()),
                _ => Value::Null
            };

            row_map.insert(name.clone().into(), transformed_value);
        }

        vec_of_maps.push(row_map);
    }

    vec_of_maps


}


/// Why is this matching / deconstruction so cumbersome?
fn handle_any_value_numeric(value: &AnyValue) -> Value {
    match value {
        AnyValue::Int8(val) => Value::Number(val.clone().into()),
        AnyValue::Int16(val) => Value::Number(val.clone().into()),
        AnyValue::Int32(val) => Value::Number(val.clone().into()),
        AnyValue::Int64(val) => Value::Number(val.clone().into()),
        AnyValue::UInt8(val) => Value::Number(val.clone().into()),
        AnyValue::UInt16(val) => Value::Number(val.clone().into()),
        AnyValue::UInt32(val) => Value::Number(val.clone().into()),
        AnyValue::UInt64(val) => Value::Number(val.clone().into()),
        _ => Value::Null
    }
}

fn write_df<T>(df: &mut DataFrame, table_configs: &HashMap<String, T>, gcp_config: &IndexerGcpBigQueryConfig) {

    // println!("{:?}", df);
    // let df_struct = df.into_struct("test");
    // println!("{:?}", df_struct);

    // let dat: Vec<_> = df_struct.into_iter().map(|s| s).collect();
    // println!("{:?}", dat);

}


fn test_read_csv_contents()  -> DataFrame {

    let contents = CsvReader::from_path("/Users/jonathanl/reth-tmp/data/Transfer.csv").expect("file does not exist")
        .has_header(true)
        .finish()
        .unwrap();

    // let mut rdr = csv::Reader::from_path("/Users/jonathanl/reth-tmp/data/Transfer.csv")?
    // for result in rdr.records() {
    //     let record = result;
    //     println!("{:?}", record);
    // }

    println!("************************************");
    println!("{:?}", contents);
    println!("************************************");

    contents
}


fn test_uuid() {

    let new_uuid = Uuid::new_v4();
    println!("*** uuid generated: {:?}", new_uuid.to_string());

}


/// Loads the indexer configuration from the "reth-indexer-config.json" file.
/// Returns the loaded `IndexerConfig` if successful.
/// Panics if the file does not exist or if there is an error reading or parsing the file.
fn load_indexer_config(file_path: &Path) -> IndexerConfig {
    let mut file = File::open(file_path)
        .unwrap_or_else(|_| panic!("Failed to find config file at path - {:?}", file_path));

    let mut content = String::new();
    file.read_to_string(&mut content)
        .expect("Failed to read reth-indexer-config.json file");

    let config: IndexerConfig =
        serde_json::from_str(&content).expect("Failed to parse reth-indexer-config.json JSON");

    config
}


pub fn solidity_type_to_db_type(abi_type: &str) -> &str {
    match abi_type {
        "address" => "string",
        "bool" | "bytes" | "string" | "int256" | "uint256" => "string",
        "uint8" | "uint16" | "uint32" | "uint64" | "uint128" | "int8" | "int16" | "int32"
        | "int64" | "int128" => "int",
        _ => panic!("Unsupported type {}", abi_type),
    }
}

pub fn db_type_to_table_field_schema(db_type: &str, name: &str) -> TableFieldSchema {
    match db_type {
        "int" => TableFieldSchema::integer(name),
        "string" => TableFieldSchema::string(name),
        _ => panic!("Unsupported db type: {}", db_type)
    }
}


async fn test_ddls(
    table_configs: &HashMap<String, IndexMap<String, String>>, 
    gcp_config: &IndexerGcpBigQueryConfig
) {

    // Get project details - project/dataset/creds
    let project_id = &gcp_config.project_id;
    let dataset_id = &gcp_config.dataset_id;
    let credentials_path = &gcp_config.credentials_path;

    //  Init client
    let client = gcp_bigquery_client::Client::from_service_account_key_file(credentials_path).await.unwrap();

    // Table Deletion - if setting true + table(s) exist
    if gcp_config.drop_tables {
        for table_name in table_configs.keys() {
            let table_ref = client.table().get(project_id, dataset_id, table_name, None).await;
            match table_ref {
                Ok(table) => {
                    // Delete table, since it exists
                    println!("deleting table: {table_name}");
                    table.delete(&client).await;
                }
                Err(..) => {}
            }
        }
    }

    let dataset_ref = client.dataset().get(project_id, dataset_id).await;
    for (table_name, column_map) in table_configs.iter() {
        println!("Evaluating table: {}, column_map: {:?}", table_name, column_map);
        let table_ref = client.table().get(project_id, dataset_id, &table_name, None).await;
        match table_ref {
            Ok(..) => {} // exists, so do nothing / do not create
            Err(..) => {
                println!("   -- will create: {}", table_name);
                let schema_types: Vec<TableFieldSchema> = column_map.iter().map(|column| 
                    db_type_to_table_field_schema(&column.1.to_string(), &column.0.to_string())
                ).collect();
                println!("   -- schema types: {:?}", schema_types);

                let dataset = &mut dataset_ref.as_ref().unwrap();
                let result = dataset.create_table(
                    &client,
                    Table::new(
                        project_id, dataset_id, table_name, 
                        TableSchema::new(schema_types)
                    )
                ).await;

                println!("   -- presumably created table: {}", table_name);
            }
        }
    }

    // Create, if tables do not exist
}

fn test_static_maps() {

    // let cols_string: HashMap<_, _> = COMMON_COLUMNS.into_iter().map( |it| (it.0.to_string(), it.1.to_string()) ).collect();
    // .iter().map( |k,v| (k.to_string(), v.to_string())).collect();

    let cols_list: IndexMap<_, _> = COMMON_COLUMNS.entries().collect();
    println!("converted to elems: {:?}", cols_list);

}

fn test_read_config_gcp() -> Option<IndexerGcpBigQueryConfig> {

    let config: String = std::env::var("CONFIG").unwrap_or("./reth-indexer-config.json".to_string());
    println!("config: {}", config);
    let indexer_config: IndexerConfig = load_indexer_config(Path::new(&config));

    println!("{:?}", indexer_config.gcp_bigquery);
    return indexer_config.gcp_bigquery;
}

fn test_read_config() -> HashMap<String, IndexMap<String, String>> {
    let config: String = std::env::var("CONFIG").unwrap_or("./reth-indexer-config.json".to_string());
    println!("config: {}", config);
    let indexer_config: IndexerConfig = load_indexer_config(Path::new(&config));

    //  Config map for all tables
    //  Ordered by common columns, then by configured mapped
    let mut all_tables: HashMap<String, _> = HashMap::new();

    for mapping in indexer_config.event_mappings {
        // let table_name = mapping.decode_abi_items.iter().map(|abi_item| 
        //     abi.name.to_lowercase()
        // );
        for abi_item in mapping.decode_abi_items {
            let table_name = abi_item.name;
            let column_type_map: IndexMap<_, _> = abi_item.inputs.iter().map(|input| 
                (
                    input.name.clone(),
                    solidity_type_to_db_type(input.type_.clone().as_str()).to_string()
                )
            ).collect();

            let common_column_type_map: IndexMap<_, _> = COMMON_COLUMNS.into_iter().map( |it| 
                (it.0.to_string(), it.1.to_string()) 
            ).collect();

            let merged_column_types: IndexMap<_, _> = common_column_type_map.into_iter().chain(
                column_type_map).collect();

            all_tables.insert(table_name, merged_column_types);
        }

    }

    // println!("{:?}", all_tables);

    all_tables


}


async fn table_exists(table_name: &str) -> bool {

    let project_id = "economic-data-391303";
    let dataset_id = "reth_index_a";
    let google_credentials_path = "/Users/jonathanl/.secrets/economic-data-391303-6c338b794579.json";

    let client = gcp_bigquery_client::Client::from_service_account_key_file(google_credentials_path).await.unwrap();
    let table1 = client.table().get(project_id, dataset_id, table_name, None).await;
    match table1 {
        Ok(..) => true,
        Err(..) => false
    }
}

async fn dataset_exists(dataset_id: &str) -> bool {
    
    let project_id = "economic-data-391303";
    let google_credentials_path = "/Users/jonathanl/.secrets/economic-data-391303-6c338b794579.json";
    let client = gcp_bigquery_client::Client::from_service_account_key_file(google_credentials_path).await.unwrap();
    let dataset = client.dataset().get(project_id, dataset_id).await;
    match dataset {
        Ok(..) => true,
        Err(..) => false
    }
}

async fn test_gcp() -> Result<(), BQError> {

    // Get these from configuration file
    let project_id = "economic-data-391303";
    let dataset_id = "reth_index_a";
    let google_credentials_path = "/Users/jonathanl/.secrets/economic-data-391303-6c338b794579.json";


    // Get dataset from project
    //  Create a table or two
    let client = gcp_bigquery_client::Client::from_service_account_key_file(google_credentials_path).await?;
    // let dataset = client.dataset().get(project_id, dataset_id).await?;

    let d1_exists = dataset_exists("reth_index_a").await;
    let d2_exists = dataset_exists("reth_index_b").await;

    println!("d1_exists: {d1_exists}");
    println!("d2_exists: {d2_exists}");


    let t1_exists = table_exists("my_table").await;
    let t2_exists = table_exists("my_non_table").await;

    println!("t1_exists: {t1_exists}");
    println!("t2_exists: {t2_exists}");


    // let table1 = client.table().get(project_id, dataset_id, "my_table", None).await.unwrap();
    // let table2 = client.table().get(project_id, dataset_id, "my_non_table", None).await.unwrap();


        // .map_err (|e| { 
        //     println!("Error getting table: {:?}", e);
        // });

    // let table = dataset.create_table(
    //     &client,
    //     Table::from_dataset(
    //         &dataset,
    //         "my_table",
    //         TableSchema::new(vec![
    //             TableFieldSchema::integer("id"),
    //             TableFieldSchema::string("name"),
    //             TableFieldSchema::string("description"),
    //             TableFieldSchema::timestamp("created_at")
    //         ])
    //     )
    //     .friendly_name("my_table")
    // ).await?;

    println!("Goodbye, Moon!");
    Ok(())
}
