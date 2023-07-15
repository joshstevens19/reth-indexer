use std::path::Path;

use reth_db::{
    database::Database,
    mdbx::{Env, NoWriteMap},
};

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

    // TODO! RETH PROVIDER HOLDS CACHE OF LATEST BLOCKS FOR NOW USE THIS TO GET LATEST BLOCK NUMBER
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
}
