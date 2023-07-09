use std::path::Path;

use reth_db::{
    database::Database,
    mdbx::{Env, NoWriteMap},
    models::StoredBlockBodyIndices,
    tables,
    transaction::DbTx,
};
use reth_primitives::{Header, Receipt, TransactionSignedNoHash};

pub struct NodeDb {
    reth_db: Env<NoWriteMap>,
}

impl NodeDb {
    /// Creates a new instance of the `Database` struct.
    ///
    /// This function initializes a new `Database` with the specified database path.
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the database directory.
    ///
    /// # Returns
    ///
    /// * `Result<Database, String>` - A `Result` indicating the result of the operation.
    ///   - `Ok(Database)` if the database is successfully opened.
    ///   - `Err(String)` if an error occurs while opening the database.
    ///
    /// # Errors
    ///
    /// This function returns an error if:
    ///   - The database cannot be opened. Make sure the specified path is valid and the reth node is syncing and readable.
    pub fn new(path: &Path) -> Result<Self, String> {
        let reth_db =
            Env::<NoWriteMap>::open(path, reth_db::mdbx::EnvKind::RO, None).map_err(|_| {
                "Could not open database make sure reth node is syncing and readable".to_string()
            })?;

        Ok(Self { reth_db })
    }

    /// Retrieves the stored block body indices for a given block number.
    ///
    /// This function retrieves the stored block body indices for the specified block number from the underlying database.
    /// It returns an `Option<StoredBlockBodyIndices>`, which contains the block body indices if they exist in the database,
    /// or `None` if the block body indices are not found.
    ///
    /// # Arguments
    ///
    /// * `block_number` - The block number for which to retrieve the block body indices.
    ///
    /// # Returns
    ///
    /// * `Option<StoredBlockBodyIndices>` - The stored block body indices wrapped in an `Option`.
    ///   - `Some(StoredBlockBodyIndices)` if the block body indices exist in the database.
    ///   - `None` if the block body indices are not found.
    ///
    /// # Panics
    ///
    /// This function will panic if there is an error accessing the database or retrieving the block body indices.
    /// Make sure the database connection is established and the necessary tables exist before calling this function.
    pub fn get_block_body_indices(&self, block_number: u64) -> Option<StoredBlockBodyIndices> {
        self.reth_db
            .view(|tx| tx.get::<tables::BlockBodyIndices>(block_number))
            .unwrap()
            .unwrap()
    }

    /// Retrieves the block headers for a given block number from the reth database.
    ///
    /// # Arguments
    ///
    /// * `block_number` - The block number for which to retrieve the headers.
    ///
    /// # Returns
    ///
    /// An `Option` containing the block headers if found, or `None` if the headers are not available in the database.
    pub fn get_block_headers(&self, block_number: u64) -> Option<Header> {
        self.reth_db
            .view(|tx| tx.get::<tables::Headers>(block_number))
            .unwrap()
            .unwrap()
    }

    /// Retrieves the latest block number from the `reth_db`.
    ///
    /// This function calculates the size of the `Headers` database by accessing its statistics.
    /// It retrieves the page size, number of leaf pages, branch pages, and overflow pages,
    /// and calculates the total table size. The table size is then returned as the latest block number.
    ///
    /// # Returns
    ///
    /// The latest block number as a `u64`.
    pub fn get_latest_block_number(&self) -> u64 {
        self.reth_db
            .view(|tx| {
                let headers = tx.inner.open_db(Some(&"Headers".to_string())).unwrap();

                let stats = tx.inner.db_stat(&headers).unwrap();

                let page_size = stats.page_size() as usize;
                let leaf_pages = stats.leaf_pages();
                let branch_pages = stats.branch_pages();
                let overflow_pages = stats.overflow_pages();
                let num_pages = leaf_pages + branch_pages + overflow_pages;
                let table_size = page_size * num_pages;

                table_size as u64
            })
            .unwrap()
    }

    // Retrieves the receipt from the reth DB for the specified key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the receipt to retrieve.
    ///
    /// # Returns
    ///
    /// * `Result<Option<Receipt>, ()>` - The receipt wrapped in a `Result` indicating success or failure.
    pub fn get_receipt(&self, key: u64) -> Option<Receipt> {
        self.reth_db
            .view(|tx| tx.get::<tables::Receipts>(key))
            .unwrap()
            .unwrap()
    }

    /// Retrieves a stored transaction by key.
    ///
    /// This function retrieves a stored transaction with the specified key from the underlying database.
    /// It returns an `Option<TransactionSignedNoHash>`, which contains the transaction if it exists in the database,
    /// or `None` if the transaction is not found.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the transaction to retrieve.
    ///
    /// # Returns
    ///
    /// * `Option<TransactionSignedNoHash>` - The stored transaction wrapped in an `Option`.
    ///   - `Some(TransactionSignedNoHash)` if the transaction exists in the database.
    ///   - `None` if the transaction is not found.
    ///
    /// # Panics
    ///
    /// This function will panic if there is an error accessing the database or retrieving the transaction.
    /// Make sure the database connection is established and the necessary tables exist before calling this function.
    pub fn get_transaction(&self, key: u64) -> Option<TransactionSignedNoHash> {
        self.reth_db
            .view(|tx| tx.get::<tables::Transactions>(key))
            .unwrap()
            .unwrap()
    }
}
