use std::path::Path;

use reth_db::{
    mdbx::{Env, NoWriteMap},
    open_db_read_only,
};
use reth_primitives::ChainSpecBuilder;
use reth_provider::ProviderFactory;

pub fn get_reth_factory(path: &Path) -> Result<ProviderFactory<Env<NoWriteMap>>, String> {
    let db = open_db_read_only(path, None).map_err(|_| {
        "Could not open database make sure reth node is syncing and readable".to_string()
    })?;

    let spec = ChainSpecBuilder::mainnet().build();
    let factory: ProviderFactory<Env<NoWriteMap>> = ProviderFactory::new(db, spec.into());

    Ok(factory)
}
