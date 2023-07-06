mod api;
mod csv;
mod decode_events;
mod indexer;
mod node_db;
mod postgres;
mod types;

use crate::indexer::sync;
use api::start_api;
use std::{fs::File, io::Read, path::Path};
use types::IndexerConfig;

// We use jemalloc for performance reasons
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Loads the indexer configuration from the "reth-indexer-config.json" file.
/// Returns the loaded `IndexerConfig` if successful.
/// Panics if the file does not exist or if there is an error reading or parsing the file.
fn load_indexer_config() -> IndexerConfig {
    let file_path = Path::new("./reth-indexer-config.json");
    let mut file = File::open(file_path).expect("Please create a reth-indexer-config.json file");

    let mut content = String::new();
    file.read_to_string(&mut content)
        .expect("Failed to read reth-indexer-config.json file");

    let config: IndexerConfig =
        serde_json::from_str(&content).expect("Failed to parse reth-indexer-config.json JSON");

    config
}

#[tokio::main]
async fn main() {
    // Initialize the logger
    env_logger::init();

    let api_only: bool = std::env::var("API").map(|_| true).unwrap_or(false);

    let indexer_config: IndexerConfig = load_indexer_config();

    if api_only {
        start_api(
            &indexer_config.event_mappings,
            &indexer_config.postgres.connection_string,
        )
        .await;
    } else {
        sync(&indexer_config).await;
    }
}
