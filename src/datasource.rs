use crate::csv::CsvWriter;
use async_trait::async_trait;
use std::any::Any;

///
///  Common trait for writeable datasources
///  This interface will be implemented by
#[async_trait]
pub trait DatasourceWritable {
    async fn write_data(&self, table_name: &str, csv_writer: &CsvWriter);
    fn as_any(&self) -> &dyn Any;
}
