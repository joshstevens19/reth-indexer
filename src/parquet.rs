use crate::{
    csv::CsvWriter,
    datasource::DatasourceWritable,
    types::{IndexerContractMapping, IndexerGcpBigQueryConfig},
};
use async_trait::async_trait;
use csv::ReaderBuilder;
use indexmap::IndexMap;
use phf::phf_ordered_map;
use polars::prelude::*;
use serde_json::Value;
use std::{any::Any, collections::HashMap, error::Error, fs::File};
