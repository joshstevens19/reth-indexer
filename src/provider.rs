use std::path::Path;

use reth_db::{
    mdbx::{Env, NoWriteMap},
    models::StoredBlockBodyIndices,
    open_db_read_only,
};
use reth_primitives::{
    ChainSpecBuilder, Header, Receipt, TransactionSigned,
};
use reth_provider::{
    BlockReader, HeaderProvider, ProviderFactory, ReceiptProvider, TransactionsProvider,
};

pub fn get_reth_factory(path: &Path) -> Result<ProviderFactory<Env<NoWriteMap>>, String> {
    let db = open_db_read_only(path, None).map_err(|_| {
        "Could not open database make sure reth node is syncing and readable".to_string()
    })?;

    let spec = ChainSpecBuilder::mainnet().build();
    let factory: ProviderFactory<Env<NoWriteMap>> = ProviderFactory::new(db, spec.into());

    Ok(factory)
}

pub fn header_by_number<T: HeaderProvider>(provider: T, block_number: u64) -> Option<Header> {
    provider.header_by_number(block_number).unwrap()
}

pub fn block_body_indices<T: BlockReader>(
    provider: T,
    block_number: u64,
) -> Option<StoredBlockBodyIndices> {
    provider.block_body_indices(block_number).unwrap()
}

pub fn transaction_by_id<T: TransactionsProvider>(
    provider: T,
    tx_id: u64,
) -> Option<TransactionSigned> {
    provider.transaction_by_id(tx_id).unwrap()
}

pub fn receipt<T: ReceiptProvider>(provider: T, tx_id: u64) -> Option<Receipt> {
    provider.receipt(tx_id).unwrap()
}
