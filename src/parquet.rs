use crate::{
    csv::CsvWriter,
    datasource::{load_table_configs, read_csv_to_polars, DatasourceWritable},
    types::{IndexerContractMapping, IndexerParquetConfig},
};
use async_trait::async_trait;
use indexmap::IndexMap;
use polars::prelude::*;
use std::{any::Any, collections::HashMap, error::Error, fs, fs::File, path::Path};

pub struct ParquetClient {
    data_directory: String,
    drop_tables: bool,
    use_pyarrow: bool,
    table_map: HashMap<String, IndexMap<String, String>>,
}

#[derive(Debug)]
pub enum ParquetClientError {
    ParquetOperationError(String),
}

///
/// Factory function to initialize parquet client
/// Will clean up files recursively (if drop_tables is true)
///
/// # Arguments
/// * `indexer_parquet_config` - parquet config settings
/// * `event_mappings` - list of events and abi fields to parse
pub async fn init_parquet_db(
    indexer_parquet_config: &IndexerParquetConfig,
    event_mappings: &[IndexerContractMapping],
) -> Result<ParquetClient, ParquetClientError> {
    let parquet_client = ParquetClient::new(indexer_parquet_config, event_mappings).await?;

    if indexer_parquet_config.drop_tables {
        let res = parquet_client.delete_tables().await;
        match res {
            Ok(..) => {}
            Err(err) => {
                return Err(ParquetClientError::ParquetOperationError(format!(
                    "Parquet client operation error: {:?}",
                    err
                )))
            }
        }
    }

    let res = parquet_client.create_tables().await;
    match res {
        Ok(..) => {}
        Err(err) => {
            return Err(ParquetClientError::ParquetOperationError(format!(
                "Parquet client operation error: {:?}",
                err
            )))
        }
    }

    Ok(parquet_client)
}

impl ParquetClient {
    ///
    /// Create new ParquetClient, which includes root directory
    ///
    /// # Arguments
    ///
    /// * indexer_parquet_config:
    /// * indexer_contract_mappings:
    ///
    pub async fn new(
        indexer_parquet_config: &IndexerParquetConfig,
        indexer_contract_mappings: &[IndexerContractMapping],
    ) -> Result<Self, ParquetClientError> {
        let table_map = load_table_configs(indexer_contract_mappings);

        // Check data directory exists - if not, then create!
        let root_data_dir: &Path = Path::new(indexer_parquet_config.data_directory.as_str());
        if !root_data_dir.exists() {
            let res = fs::create_dir(root_data_dir);
            match res {
                Ok(()) => {}
                Err(err) => {
                    return Err(ParquetClientError::ParquetOperationError(format!(
                        "Could not create root dir. err: {}",
                        err
                    )))
                }
            }
        }

        Ok(ParquetClient {
            data_directory: indexer_parquet_config.data_directory.to_string(),
            drop_tables: indexer_parquet_config.drop_tables,
            use_pyarrow: indexer_parquet_config.use_py_arrow,
            table_map,
        })
    }

    ///
    /// Deletes "tables" as specified in the event mapping configuration
    /// These are just subdirs that contain the synced files for that table / event
    ///
    pub async fn delete_tables(&self) -> Result<(), Box<dyn Error>> {
        if self.drop_tables {
            for table_name in self.table_map.keys() {
                let table_dir = format!("{}/{}/", self.data_directory, table_name);
                let table_path = Path::new(table_dir.as_str());
                if table_path.exists() && table_path.is_dir() {
                    println!("Removing directory / contents: {}", table_dir);
                    let res = fs::remove_dir_all(table_path);
                    match res {
                        Ok(()) => {}
                        Err(err) => return Err(Box::new(err)),
                    }
                }
            }
        }

        Ok(())
    }

    ///
    /// Creates "tables" as specified in the event mapping configuration
    /// These are just subdirs taht contain the synced files for that table / event
    ///
    pub async fn create_tables(&self) -> Result<(), Box<dyn Error>> {
        for table_name in self.table_map.keys() {
            // Check if table exists;  if NO, then create new
            let table_dir = format!("{}/{}/", self.data_directory, table_name);
            let table_path = Path::new(table_dir.as_str());
            if !table_path.exists() {
                println!("Creating directory: {}", table_name);
                let res = fs::create_dir(table_path);
                match res {
                    Ok(()) => {}
                    Err(err) => return Err(Box::new(err)),
                }
            }
        }

        Ok(())
    }

    pub async fn write_csv_to_storage(&self, table_name: &str, csv_writer: &CsvWriter) {
        // Given CSV, read into type-constrained / enforced polars dataframe
        // Write out to parquet file, using block boundaries in file name
        let column_map = &self.table_map[table_name];
        let mut dataframe = read_csv_to_polars(csv_writer.path(), column_map);
        let block_boundaries = self.get_block_boundaries(&dataframe);

        //  Write to parquet file
        match block_boundaries {
            Err(err) => println!("Failed to write to parquet - reason: {:?}", err),
            Ok((min, max)) => {
                let parquet_file_name = format!("{}___{}_{}", table_name, min, max);
                let full_path_name = format!(
                    "{}/{}/{}",
                    self.data_directory, table_name, parquet_file_name
                );
                println!("Writing parquet file: {}", full_path_name);

                let parquet_file = File::create(full_path_name.as_str());
                match parquet_file {
                    Err(err) => {
                        println!("Could not create file: {}, err: {:?}", full_path_name, err)
                    }
                    Ok(file) => {
                        // Write file using parquet writer
                        let res = ParquetWriter::new(file).finish(&mut dataframe);
                        match res {
                            Err(response) => {
                                println!("Failed to write parquet file, reason: {:?}", response)
                            }
                            Ok(_) => println!("Successfully wrote parquet file."),
                        }
                    }
                }
            }
        }
    }

    fn get_block_boundaries(&self, dataframe: &DataFrame) -> Result<(i64, i64), PolarsError> {
        let block_number_column = dataframe.column("block_number")?;
        let min = block_number_column
            .min()
            .expect("No minimum block number found.");
        let max = block_number_column
            .max()
            .expect("No maximum block number found.");
        Ok((min, max))
    }
}

///
/// Implement DatasourceWritable wrapper
#[async_trait]
impl DatasourceWritable for ParquetClient {
    async fn write_data(&self, table_name: &str, csv_writer: &CsvWriter) {
        println!("  writing / sync to parquet file: {:?}", table_name);
        self.write_csv_to_storage(table_name, csv_writer).await;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
