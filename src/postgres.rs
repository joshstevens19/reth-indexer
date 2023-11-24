use crate::csv::CsvWriter;
use crate::datasource::DatasourceWritable;
use crate::types::{ABIInput, ABIItem, IndexerContractMapping, IndexerPostgresConfig};
use async_trait::async_trait;
use std::any::Any;
use tokio_postgres::{Client, Error, NoTls, Row};

/// A wrapper around `tokio_postgres::Client` for working with PostgreSQL database.
pub struct PostgresClient {
    client: Client,
}

/// Initializes a PostgreSQL database for indexing.
///
/// This function establishes a connection to the PostgreSQL database using the provided
/// configuration and creates the necessary tables for indexing based on the given event
/// mappings. If `include_eth_transfers` is `true`, additional tables for Ethereum transfers
/// will be created.
///
/// # Arguments
///
/// * `indexer_postgres_config` - The configuration for connecting to the PostgreSQL database.
/// * `event_mappings` - A slice of `IndexerContractMapping` representing the event mappings
///   to be used for creating the tables.
/// * `include_eth_transfers` - A flag indicating whether to include tables for Ethereum transfers.
///
/// # Returns
///
/// A `Result` containing a `PostgresClient` if the initialization is successful, or `Err(Error)`
/// if an error occurs.
pub async fn init_postgres_db(
    indexer_postgres_config: &IndexerPostgresConfig,
    event_mappings: &[IndexerContractMapping],
    include_eth_transfers: bool,
) -> Result<PostgresClient, Error> {
    let mut postgres = PostgresClient::new(&indexer_postgres_config.connection_string).await?;

    create_tables(
        &mut postgres,
        indexer_postgres_config,
        event_mappings,
        include_eth_transfers,
    )
    .await?;

    Ok(postgres)
}

pub const ETH_TRANSFER_TABLE_NAME: &str = "eth_transfer";

/// Creates the necessary tables for indexing in the PostgreSQL database.
///
/// This function creates tables for event indexing based on the provided event mappings.
/// If `include_eth_transfers` is `true`, an additional table for Ethereum transfers will be created.
/// Existing tables will not be recreated if they already exist.
///
/// # Arguments
///
/// * `postgres` - A mutable reference to the `PostgresClient` representing the PostgreSQL connection.
/// * `indexer_postgres_config` - The configuration for the PostgreSQL connection and table creation options.
/// * `event_mappings` - A slice of `IndexerContractMapping` representing the event mappings
///   used for creating the tables.
/// * `include_eth_transfers` - A flag indicating whether to include tables for Ethereum transfers.
///
/// # Returns
///
/// A `Result` indicating success (`Ok(())`) or an error (`Err`) if any occurs during table creation.
async fn create_tables(
    postgres: &mut PostgresClient,
    indexer_postgres_config: &IndexerPostgresConfig,
    event_mappings: &[IndexerContractMapping],
    include_eth_transfers: bool,
) -> Result<(), Error> {
    // TODO include eth transfers ability (will always pass in false for now)
    if include_eth_transfers {
        if indexer_postgres_config.drop_tables {
            postgres.drop_table(ETH_TRANSFER_TABLE_NAME).await?;
        }

        // create table
        let create_table_query = format!(
            "
                CREATE TABLE IF NOT EXISTS {} (
                    \"indexed_id\" UUID PRIMARY KEY,
                    \"from\" CHAR(42),
                    \"to\" CHAR(42),
                    \"value\" NUMERIC,
                    \"tx_hash\" CHAR(66),
                    \"block_number\" INT,
                    \"block_hash\" CHAR(66),
                    \"timestamp\" VARCHAR(500)
                )
            ",
            ETH_TRANSFER_TABLE_NAME
        );

        postgres.execute(&create_table_query, &[]).await?;
    }

    for mapping in event_mappings {
        for abi_item in &mapping.decode_abi_items {
            let table_name = &abi_item.name;

            if indexer_postgres_config.drop_tables {
                println!("Dropping table {}", table_name);
                postgres.drop_table(table_name).await?;
            }

            let table_sql = generate_event_table_sql(&abi_item.inputs, table_name);
            println!("Generating tables, if they exist it will not create");
            println!("{}", table_sql);
            postgres.execute(&table_sql, &[]).await?;

            if indexer_postgres_config.apply_indexes_before_sync {
                println!("\x1b[33mYOU ARE CREATING INDEXES BEFORE THE DATA SYNC THIS WILL MAKE IT SLOWER BUT WILL MEAN YOU BE ABLE TO QUERY THE DATA FASTER AS IT RESYNCS!\x1b[0m");
                println!("Generating indexes, if they exist it will not create");
                generate_event_table_indexes(postgres, abi_item, table_name).await?;
            }
        }
    }
    Ok(())
}

/// Converts a Solidity ABI type to the corresponding database type.
///
/// This function maps Solidity ABI types to their equivalent database types for table column
/// definitions. It returns the corresponding database type as a `String`.
///
/// # Arguments
///
/// * `abi_type` - The Solidity ABI type to convert.
///
/// # Returns
///
/// The corresponding database type as a `String`, or panics if the ABI type is unsupported.
pub fn solidity_type_to_db_type(abi_type: &str) -> String {
    match abi_type {
        "address" => "CHAR(42)".to_string(),
        "bool" | "bytes" | "string" | "int256" | "uint256" => "TEXT".to_string(),
        "uint8" | "uint16" | "uint32" | "uint64" | "uint128" | "int8" | "int16" | "int32"
        | "int64" | "int128" => "NUMERIC".to_string(),
        _ => panic!("Unsupported type {}", abi_type),
    }
}

/// Generates indexes for the event table in the PostgreSQL database.
///
/// This function generates indexes for the event table based on the provided ABI inputs.
/// Indexes are created for each indexed ABI input. Existing indexes will not be recreated
/// if they already exist.
///
/// # Arguments
///
/// * `postgres` - A mutable reference to the `PostgresClient` representing the PostgreSQL connection.
/// * `abi_inputs` - A slice of `ABIInput` representing the ABI inputs of the event.
/// * `table_name` - The name of the event table.
///
/// # Returns
///
/// A `Result` indicating success (`Ok(())`) or an error (`Err`) if any occurs during index creation.
pub async fn generate_event_table_indexes(
    postgres: &PostgresClient,
    abi_item: &ABIItem,
    table_name: &str,
) -> Result<(), Error> {
    for abi_input in abi_item.inputs.iter() {
        if abi_input.indexed {
            let event_index_sql = format!(
                "CREATE INDEX IF NOT EXISTS {}_idx__{} ON {}(\"{}\");",
                table_name.to_lowercase(),
                abi_input.name.to_lowercase(),
                table_name.to_lowercase(),
                abi_input.name.to_lowercase(),
            );

            postgres.execute(&event_index_sql, &[]).await?;
            println!("{}", event_index_sql);

            let contract_address_event_index_sql = format!(
                "CREATE INDEX IF NOT EXISTS {}_contract_address_idx__{} ON {}(\"contract_address\",\"{}\");",
                table_name.to_lowercase(),
                abi_input.name.to_lowercase(),
                table_name.to_lowercase(),
                abi_input.name.to_lowercase(),
            );

            postgres
                .execute(&contract_address_event_index_sql, &[])
                .await?;
            println!("{}", contract_address_event_index_sql);
        }
    }

    if let Some(custom_db_indexes) = &abi_item.custom_db_indexes {
        // apply the custom indexes
        for custom_db_index in custom_db_indexes {
            let index_name: String = custom_db_index
                .iter()
                .map(|name| name.to_lowercase())
                .collect::<Vec<String>>()
                .join("_");

            let index_columns: String = custom_db_index
                .iter()
                .map(|name| format!("\"{}\"", name.to_lowercase()))
                .collect::<Vec<String>>()
                .join(", ");

            let event_index_sql = format!(
                "CREATE INDEX IF NOT EXISTS {}_idx__{} ON {}({});",
                table_name.to_lowercase(),
                index_name,
                table_name.to_lowercase(),
                index_columns
            );

            postgres.execute(&event_index_sql, &[]).await?;
            println!("{}", event_index_sql);
        }
    }

    Ok(())
}

/// Generates the SQL statement for creating an event table in the PostgreSQL database.
///
/// This function generates the SQL statement for creating an event table based on the provided
/// ABI inputs and table name. The table will have columns for each ABI input, as well as common
/// columns for transaction hash, block number, block hash, and timestamp.
///
/// # Arguments
///
/// * `abi_inputs` - A slice of `ABIInput` representing the ABI inputs of the event.
/// * `table_name` - The name of the event table.
///
/// # Returns
///
/// The SQL statement as a `String`.
pub fn generate_event_table_sql(abi_inputs: &[ABIInput], table_name: &str) -> String {
    let columns: Vec<String> = abi_inputs
        .iter()
        .map(|abi_input| {
            format!(
                "\"{}\" {}",
                abi_input.name.to_lowercase(),
                solidity_type_to_db_type(&abi_input.type_)
            )
        })
        .collect();

    let columns_str = columns.join(", ");

    let query = format!(
        "CREATE TABLE IF NOT EXISTS {} (indexed_id UUID PRIMARY KEY, contract_address CHAR(66), {}, \"tx_hash\" CHAR(66), \"block_number\" INT, \"block_hash\" CHAR(66), \"timestamp\" VARCHAR(500))",
        table_name, columns_str
    );

    query
}

impl PostgresClient {
    /// Creates a new `PostgresClient` instance and establishes a connection to the PostgreSQL database.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - The connection string for the PostgreSQL database.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `PostgresClient` instance if the connection is successful, or an `Error` if an error occurs.
    pub async fn new(connection_string: &str) -> Result<Self, Error> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        Ok(Self { client })
    }

    /// Executes a SQL query on the PostgreSQL database.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query to execute.
    /// * `params` - The parameters to bind to the query.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the number of rows affected by the query, or an `Error` if an error occurs.
    pub async fn execute<T>(
        &self,
        query: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64, Error>
    where
        T: ?Sized + tokio_postgres::ToStatement,
    {
        self.client.execute(query, params).await
    }

    /// Reads rows from the database based on the given query and parameters.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to execute. It must implement `tokio_postgres::ToStatement`.
    /// * `params` - The parameters to bind to the query. Each parameter must implement `tokio_postgres::types::ToSql + Sync`.
    ///
    /// # Returns
    ///
    /// A `Result` containing a vector of `Row` on success, or an `Error` if the query execution fails.
    pub async fn read<T>(
        &mut self,
        query: &T,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + tokio_postgres::ToStatement,
    {
        let rows = self.client.query(query, params).await?;
        Ok(rows)
    }

    pub async fn drop_table(&mut self, table_name: &str) -> Result<u64, Error> {
        self.client
            .execute(&format!("DROP TABLE IF EXISTS {}; ", table_name), &[])
            .await
    }
}

///
/// Implement DatasourceWritable wrapper / trait
#[async_trait]
impl DatasourceWritable for PostgresClient {
    async fn write_data(&self, table_name: &str, csv_writer: &CsvWriter) {
        println!("   writing / sync postgres table: {:?}", table_name);
        let copy_query = format!(
            "COPY {} FROM '{}' DELIMITER ',' CSV HEADER",
            table_name,
            csv_writer.path()
        );
        self.execute(&copy_query, &[]).await.unwrap();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
