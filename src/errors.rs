use thiserror::Error;

#[derive(Error, Debug)]
pub enum IotDBError {
    #[error("IO error: {0}")]
    IO(#[from] std::io::Error),

    #[error("IoTDB thrift rpc connection error")]
    Thrift(#[from] thrift::Error),

    #[error("IoTDB Polars DataFrame error")]
    Polars(#[from] polars::error::PolarsError),
}
