use std::{
    str::FromStr,
    thread,
    time::{Duration, Instant},
};

use log::info;
use reth_primitives::{Address, BlockHash, Bloom, Header, Log, TransactionSignedNoHash, H256};
use reth_provider::{
    BlockNumReader, BlockReader, HeaderProvider, ReceiptProvider, TransactionsProvider,
};
use reth_rpc_types::{FilteredParams, ValueOrArray};
use uuid::Uuid;

use crate::{
    csv::{create_csv_writers, CsvWriter},
    datasource::DatasourceWritable,
    decode_events::{abi_item_to_topic_id, decode_logs, DecodedLog},
    gcp_bigquery::init_gcp_bigquery_db,
    postgres::{generate_event_table_indexes, init_postgres_db, PostgresClient},
    provider::get_reth_factory,
    types::{IndexerConfig, IndexerContractMapping},
};

/// Writes a state record to a CSV file.
///
/// This function writes a state record to a CSV file using the provided `CsvWriter`. The state
/// record consists of decoded logs, transaction information, and block information.
///
/// # Arguments
///
/// * `csv_writer` - A mutable reference to a `CsvWriter` for writing the state record.
/// * `decoded_logs` - A reference to a vector of `DecodedLog` representing the decoded logs.
/// * `header_tx_info` - A reference to the `Header` containing transaction information.
/// * `tx_hash` - The transaction hash.
/// * `block_hash` - The block hash.
fn write_csv_state_record(
    csv_writer: &mut CsvWriter,
    decoded_logs: &[DecodedLog],
    header_tx_info: &Header,
    tx_hash: H256,
    block_hash: BlockHash,
) {
    for decoded_log in decoded_logs {
        let mut records: Vec<String> = Vec::new();
        records.push(Uuid::new_v4().to_string());
        records.push(format!("{:?}", decoded_log.address));

        let decoded_log_records: Vec<String> = decoded_log
            .topics
            .iter()
            .map(|input| input.value.clone())
            .collect();
        records.extend(decoded_log_records);

        // write common information every table has
        records.push(format!("{:?}", tx_hash));
        records.push(header_tx_info.number.to_string());
        records.push(format!("{:?}", block_hash));
        records.push(header_tx_info.timestamp.to_string());

        csv_writer.write(records);
    }
}

/// Checks if a contract address is present in the logs bloom filter.
///
/// This function takes a contract address and a logs bloom filter and checks if the contract
/// address is present in the logs bloom filter. It uses the `FilteredParams::address_filter`
/// method to create an address filter and then checks if the filter matches the logs bloom.
///
/// # Arguments
///
/// * `contract_address` - The contract address to check.
/// * `logs_bloom` - The logs bloom filter to match against.
fn contract_in_bloom(contract_address: Address, logs_bloom: Bloom) -> bool {
    // TODO create a issue on reth about this
    let address_filter =
        FilteredParams::address_filter(&Some(ValueOrArray::Value(contract_address)));
    FilteredParams::matches_address(logs_bloom, &address_filter)
}

/// Checks if a topic ID is present in the logs bloom filter.
///
/// This function takes a topic ID and a logs bloom filter and checks if the topic ID is present
/// in the logs bloom filter. It uses the `FilteredParams::topics_filter` method to create a
/// topics filter and then checks if the filter matches the logs bloom.
///
/// # Arguments
///
/// * `topic_id` - The topic ID to check.
/// * `logs_bloom` - The logs bloom filter to match against.
fn topic_in_bloom(topic_id: H256, logs_bloom: Bloom) -> bool {
    // TODO create a issue on reth about this
    let topic_filter =
        FilteredParams::topics_filter(&Some(vec![ValueOrArray::Value(Some(topic_id))]));
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

/// Syncs the state from a CSV writer to the Postgres database.
///
/// # Arguments
///
/// * `name` - The name of the table in the database.
/// * `csv_writer` - A mutable reference to the CSV writer.
/// * `postgres_db` - A mutable reference to the Postgres database client.
///
/// # Panics
///
/// This function will panic if executing the Postgres copy query fails.
async fn sync_state_to_db(
    name: String,
    csv_writer: &mut CsvWriter,
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
) {
    println!("Executing sync_state_to_db for table: {:?}", name);
    info!("Executing datasource insertion / sync...");
    for datasource in db_writers {
        datasource.write_data(name.as_str(), &csv_writer).await;
    }

    //  Reset csv writer
    csv_writer.reset();
}

/// Synchronizes all states from CSV files to the PostgreSQL database.
///
/// This function iterates over the event mappings specified in the `indexer_config`
/// and synchronizes the state data from CSV files to the PostgreSQL database.
/// For each ABI item in the event mappings, it finds the corresponding CSV writer
/// and invokes the `sync_state_to_db` function to perform the synchronization.
///
/// # Arguments
///
/// * `indexer_config` - A reference to the `IndexerConfig` containing the configuration settings.
/// * `csv_writers` - A mutable slice of `CsvWriter` instances representing the CSV writers for each ABI item.
/// * `db_writers` - A ref to vector of DatasourceWritable objects - to interact w/ database(s)
async fn sync_all_states_to_db(
    indexer_config: &IndexerConfig,
    reached_head: bool,
    csv_writers: &mut [CsvWriter],
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
) {
    for mapping in &indexer_config.event_mappings {
        for abi_item in &mapping.decode_abi_items {
            if let Some(csv_writer) = csv_writers.iter_mut().find(|w| w.name == abi_item.name) {
                sync_state_to_db(abi_item.name.to_lowercase(), csv_writer, db_writers).await;
            }

            // The block below is very specific to postgres
            // This is some scaffolding to allow this to work for now
            // TODO: move index creation to postgres implementation + make part of interface for datasource
            if let Some(postgres_conf) = &indexer_config.postgres {
                if !postgres_conf.apply_indexes_before_sync && !reached_head {
                    println!(
                        "applying indexes for {}, may take a little while...",
                        abi_item.name
                    );

                    // Apply to postgres client from db writer (db client) list
                    for datasource in db_writers {
                        if let Some(postgres_db) =
                            datasource.as_any().downcast_ref::<PostgresClient>()
                        {
                            generate_event_table_indexes(postgres_db, abi_item, &abi_item.name)
                                .await
                                .unwrap();
                        }
                    }
                }
            }
        }
    }
}

///
///  This is a factory to declare the various supported datasources
///  For now this will check existence of each type block to add to implementation (postgres or
///  bigquery) - but should consider a more generic implementation in the future
///
///  # Arguments
///
///  * indexer_config: the full configuration    
pub async fn init_datasource_writers(
    indexer_config: &IndexerConfig,
) -> Vec<Box<dyn DatasourceWritable>> {
    // Return a list / vector of datasource writers
    let mut writers: Vec<Box<dyn DatasourceWritable>> = Vec::new();

    // Init postgres client (if exists)
    if let Some(postgres_conf) = &indexer_config.postgres {
        let postgres_db_client =
            init_postgres_db(&postgres_conf, &indexer_config.event_mappings, false)
                .await
                .expect("Failed to initialize Postgres client");
        writers.push(Box::new(postgres_db_client));
    }

    // Init GCP bigquery client (if exists)
    if let Some(bigquery_conf) = &indexer_config.gcp_bigquery {
        let bigquery_db_client =
            init_gcp_bigquery_db(&bigquery_conf, &indexer_config.event_mappings)
                .await
                .expect("Failed to initialize bigquery client");
        writers.push(Box::new(bigquery_db_client));
    }

    if writers.len() < 1 {
        panic!("Must have at least one configured indexer datastore to run indexer");
    }

    writers
}

/// Synchronizes the indexer by processing blocks and writing the decoded logs to CSV files and
/// a PostgreSQL database.
///
/// This function performs the following steps:
///
/// 1. Initializes the PostgreSQL database based on the provided configuration.
/// 2. Initializes the `NodeDb` for accessing the reth database.
/// 3. Creates CSV writers for each ABI item based on the provided configuration.
/// 4. Iterates over blocks starting from the `from_block` specified in the configuration up to the
///    `to_block` if provided (or until reaching the maximum block number).
/// 5. Retrieves the block headers for each block number from the `NodeDb`.
/// 6. Checks if the block matches any contract addresses specified in the mapping and the RPC bloom
///    filter.
/// 7. Invokes the `process_block` function to process the block and write the logs to CSV files and
///    the PostgreSQL database.
/// 8. Performs clean-up operations for any remaining CSV files.
///
/// # Arguments
///
/// * `indexer_config` - The `IndexerConfig` containing the configuration details for the indexer.
pub async fn sync(indexer_config: &IndexerConfig) {
    info!("Starting indexer");
    info!("Initializing database writers");
    let db_writers = init_datasource_writers(&indexer_config).await;

    let mut csv_writers = create_csv_writers(
        indexer_config.csv_location.as_path(),
        &indexer_config.event_mappings,
        // TODO support eth_transfers
        false,
    );

    let mut block_number = indexer_config.from_block;
    let to_block = indexer_config.to_block.unwrap_or(u64::MAX);

    let factory = get_reth_factory(&indexer_config.reth_db_location)
        .expect("Failed to initialize reth factory");
    let mut provider: reth_provider::DatabaseProvider<'_, _> = factory
        .provider()
        .expect("Failed to initialize reth provider");

    println!("postgres syncing...");
    println!("hint: you can go to your postgres to see it writing in real time...");

    let start = Instant::now();

    let mut reached_head = false;

    // unlimited loop to handle all cases
    loop {
        match provider.header_by_number(block_number).unwrap() {
            None => {
                // means it should stay alive when at head
                if to_block == u64::MAX {
                    if !reached_head {
                        sync_all_states_to_db(
                            indexer_config,
                            reached_head,
                            &mut csv_writers,
                            &db_writers,
                        )
                        .await;
                        println!("synced all data to postgres, waiting for new blocks and reth-indexer will now index as they come in.");
                        let duration = start.elapsed();
                        println!("Elapsed time: {:.2?}", duration);
                        reached_head = true;
                    }

                    // as the block not been seen and its +1 on it we should make sure
                    // we do not skip a block
                    let last_seen_block = block_number - 1;

                    loop {
                        // If the db changes we need a new read tx otherwise it will see the old version - that's how MVCC works
                        provider = factory
                            .provider()
                            .expect("Failed to initialize reth provider while awaiting new blocks");

                        let latest_block_number = provider.last_block_number().unwrap();
                        info!("latest block number: {}", latest_block_number);
                        info!("last seen block number: {}", last_seen_block);

                        if latest_block_number > last_seen_block {
                            // block_number already set so break out
                            println!("new block(s) found check from: {}... last seen: {}... latest block: {}", block_number, last_seen_block, latest_block_number);
                            break;
                        }

                        // sleep for 2 seconds, mainnet blocks are 12 seconds apart but we want the indexer to be fast so worth
                        // the extra db checks on block
                        thread::sleep(Duration::from_secs(2));
                    }
                } else {
                    sync_all_states_to_db(
                        indexer_config,
                        reached_head,
                        &mut csv_writers,
                        &db_writers,
                    )
                    .await;
                    break;
                }
            }
            Some(header_tx_info) => {
                info!("checking block: {}", block_number);

                for mapping in &indexer_config.event_mappings {
                    let rpc_bloom: Bloom =
                        Bloom::from_str(&format!("{:?}", header_tx_info.logs_bloom)).unwrap();

                    if let Some(contract_address) = &mapping.filter_by_contract_addresses {
                        // check at least 1 matches bloom in mapping file
                        if !contract_address
                            .iter()
                            .any(|address| contract_in_bloom(*address, rpc_bloom))
                        {
                            continue;
                        }
                    }

                    if !mapping
                        .decode_abi_items
                        .iter()
                        .any(|item| topic_in_bloom(abi_item_to_topic_id(item), rpc_bloom))
                    {
                        continue;
                    }

                    process_block(
                        &provider,
                        &mut csv_writers,
                        &db_writers,
                        mapping,
                        rpc_bloom,
                        block_number,
                        &header_tx_info,
                    )
                    .await;
                }

                block_number += 1;
                // if we have reached the head, we want to keep going
                // if we are higher then the defined block number we write and exist
                if block_number == to_block || reached_head {
                    sync_all_states_to_db(
                        indexer_config,
                        reached_head,
                        &mut csv_writers,
                        &db_writers,
                    )
                    .await;

                    // only exit if we have reached the head as it should continue to run and wait for new blocks
                    if !reached_head {
                        break;
                    }
                }
            }
        }
    }

    println!("postgres sync is now complete");

    let duration = start.elapsed();
    println!("Elapsed time: {:.2?}", duration);
}

/// Processes a block by iterating over its transactions, filtering them based on contract addresses,
/// and invoking the `process_transaction` function for each eligible transaction.
///
/// This function performs the following steps:
///
/// 1. Retrieves the block body indices from the provided `NodeDb` for the given block number. If the
///    indices are not available, indicating that the state of the `NodeDb` is not caught up with the
///    target block number, it returns early.
/// 2. Iterates over the transaction IDs within the block based on the retrieved indices.
/// 3. Retrieves the transaction and receipt for each transaction ID from the `NodeDb`.
/// 4. Filters the receipt logs based on the contract addresses specified in the mapping, if any.
/// 5. Invokes the `process_transaction` function to decode and write the logs to CSV files and the
///    PostgreSQL database.
///
/// # Arguments
///
/// * `provider` - The reth-provider instance
/// * `csv_writers` - A mutable slice of `CsvWriter` instances representing the CSV writers for each
///   ABI item.
/// * `db_writers` - A reference to list of database writers
/// * `mapping` - A reference to the `IndexerContractMapping` containing the ABI items and other mapping
///   details.
/// * `rpc_bloom` - The bloom filter associated with the block's RPC logs.
/// * `block_number` - The block number being processed.
/// * `header_tx_info` - A reference to the `Header` containing transaction-related information.
async fn process_block<T: ReceiptProvider + TransactionsProvider + HeaderProvider + BlockReader>(
    provider: T,
    csv_writers: &mut [CsvWriter],
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
    mapping: &IndexerContractMapping,
    rpc_bloom: Bloom,
    block_number: u64,
    header_tx_info: &Header,
) {
    let block_body_indices = provider.block_body_indices(block_number).unwrap();
    if let Some(block_body_indices) = block_body_indices {
        for tx_id in block_body_indices.first_tx_num
            ..block_body_indices.first_tx_num + block_body_indices.tx_count
        {
            if let Some(transaction) = provider.transaction_by_id_no_hash(tx_id).unwrap() {
                if let Some(receipt) = provider.receipt(tx_id).unwrap() {
                    let logs: Vec<Log> =
                        if let Some(contract_address) = &mapping.filter_by_contract_addresses {
                            receipt
                                .logs
                                .iter()
                                .filter(|log| {
                                    contract_address
                                        .iter()
                                        .any(|address| address == &log.address)
                                })
                                .cloned()
                                .collect()
                        } else {
                            receipt.logs
                        };

                    if logs.is_empty() {
                        continue;
                    }

                    process_transaction(
                        csv_writers,
                        db_writers,
                        mapping,
                        rpc_bloom,
                        &logs,
                        transaction,
                        header_tx_info,
                    )
                    .await;
                }
            }
        }
    }
}

/// Processes a transaction by decoding logs and writing them to CSV files and a PostgreSQL database.
///
/// This function iterates over the `decode_abi_items` of the provided `IndexerContractMapping` and
/// performs the following steps:
///
/// 1. Checks if the topic ID of the ABI item is present in the provided RPC bloom filter. If not,
///    it skips the ABI item and proceeds to the next one.
/// 2. Searches for a corresponding CSV writer based on the ABI item's name. If found, it proceeds
///    with decoding the logs and writing them to the CSV file.
/// 3. After writing a certain number of logs, determined by the `sync_back_every_n_log` value in
///    the mapping, it syncs the CSV file to the PostgreSQL database using the COPY command.
///
/// # Arguments
///
/// * `csv_writers` - A mutable slice of `CsvWriter` instances representing the CSV writers for each
///   ABI item.
/// * `db_writers` - A reference to vector of database writers, whatever sources are in config
/// * `mapping` - A reference to the `IndexerContractMapping` containing the ABI items and other mapping
///   details.
/// * `rpc_bloom` - The bloom filter associated with the transaction's RPC logs.
/// * `logs` - A slice of `Log` instances representing the logs associated with the transaction.
/// * `transaction` - The `TransactionSignedNoHash` instance representing the transaction being processed.
/// * `header_tx_info` - A reference to the `Header` containing transaction-related information.
async fn process_transaction(
    csv_writers: &mut [CsvWriter],
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
    mapping: &IndexerContractMapping,
    rpc_bloom: Bloom,
    logs: &[Log],
    transaction: TransactionSignedNoHash,
    header_tx_info: &Header,
) {
    for abi_item in &mapping.decode_abi_items {
        let topic_id = abi_item_to_topic_id(abi_item);

        if !topic_in_bloom(topic_id, rpc_bloom) {
            continue;
        }

        if let Some(csv_writer) = csv_writers.iter_mut().find(|w| w.name == abi_item.name) {
            let decoded_logs = decode_logs(topic_id, logs, abi_item);
            if !decoded_logs.is_empty() {
                write_csv_state_record(
                    csv_writer,
                    &decoded_logs,
                    header_tx_info,
                    transaction.hash(),
                    header_tx_info.clone().seal_slow().hash,
                );

                // Calculate file size and sync back if necessary
                let kb_file_size = csv_writer.get_kb_file_size();
                let sync_back_threshold = (0.3333 * mapping.sync_back_every_n_log as f64) as u64;
                if kb_file_size >= sync_back_threshold {
                    sync_state_to_db(abi_item.name.to_lowercase(), csv_writer, db_writers).await;
                }
            }
        }
    }
}
