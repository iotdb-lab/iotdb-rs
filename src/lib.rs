//! ![Logo](https://raw.githubusercontent.com/francis-du/iotdb-rs/main/iotdb-rs.png)
//!
//! [![Crates.io](https://img.shields.io/crates/v/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
//! [![Api Docs](https://img.shields.io/badge/Api-Doc-a94064?style=flat-square&color=%23E5531A)](https://docs.rs/iotdb)
//! [![Crates.io](https://img.shields.io/crates/d/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
//! [![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square&color=%23E5531A)](https://github.com/iotdb-lab/iotdb-rs/blob/main/LICENSE)
//! [![Rust Build](https://img.shields.io/github/workflow/status/iotdb-lab/iotdb-rs/cargo-test?label=build&style=flat-square)](https://github.com/iotdb-lab/iotdb-rs/actions?query=workflow%3Acargo-test)
//! [![Crates Publish](https://img.shields.io/github/workflow/status/iotdb-lab/iotdb-rs/cargo-publish?label=publish&style=flat-square)](https://github.com/iotdb-lab/iotdb-rs/actions?query=workflow%3Acargo-publish)
//!
//! Apache IotDB Client written in Rust
//!
//! # Overview
//!
//! IoTDB (Internet of Things Database) is a data management system for time series data, which can provide users specific services,
//! such as, data collection, storage and analysis. Due to its light weight structure, high performance and usable features together
//! with its seamless integration with the Hadoop and Spark ecology, IoTDB meets the requirements of massive dataset storage,
//! high throughput data input, and complex data analysis in the industrial IoT field.
//!
//! # How to use
//!
//! Add `iotdb` to your `Cargo.toml`
//!
//! ```toml
//! [dependencies]
//! iotdb = "0.0.6"
//! ```
//!
//! # Example
//!
//! ```rust
//! use chrono::Local;
//! use thrift::Error;
//!
//! use iotdb::common::{Compressor, DataType, Encoding};
//! use iotdb::{ConfigBuilder, Session};
//!
//! fn main() -> Result<(), Error> {
//!  let config = ConfigBuilder::new()
//!         .endpoint("127.0.0.1", "6667")
//!         .user("root")
//!         .password("root")
//!         .zone_id("UTC+8")
//!         .debug(true)
//!         .build();
//!
//!      // open session
//!     let mut session = Session::new(config).open()?;
//!     println!("time_zone: {}", session.time_zone()?);
//!     session.delete_storage_group("root.ln")?;
//!     session.set_storage_group("root.ln")?;
//!     session.create_time_series(
//!         "root.ln.wf01.wt01.temperature",
//!         DataType::INT64,
//!         Encoding::default(),
//!         Compressor::default(),
//!     )?;
//!
//!     session.create_time_series(
//!         "root.ln.wf01.wt01.humidity",
//!         DataType::INT64,
//!         Encoding::default(),
//!         Compressor::default(),
//!     )?;
//!
//!     let now = Local::now().timestamp();
//!     session.sql(
//!         format!(
//!             "INSERT INTO root.ln.wf01.wt01(timestamp,status) values({},true)",
//!             now
//!         )
//!         .as_str(),
//!     )?;
//!     session.sql(
//!         format!(
//!             "INSERT INTO root.ln.wf01.wt01(timestamp,status) values({},false)",
//!             now + 1000
//!         )
//!         .as_str(),
//!     )?;
//!     session.sql(
//!         format!(
//!             "INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values({},false,18.36)",
//!             now + 2000
//!         )
//!         .as_str(),
//!     )?;
//!     session.sql(
//!         format!(
//!             "INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values({},true,32.23)",
//!             now + 3000
//!         )
//!         .as_str(),
//!     )?;
//!    session.sql("select * from root.ln")?.show();
//!    session.close()?;
//!
//!   Ok(())
//! }
//! ```
#[macro_use]
extern crate prettytable;

pub use chrono;
pub use polars;
use std::collections::BTreeMap;
use std::net::TcpStream;
use std::str::FromStr;
pub use thrift;

use anyhow::bail;
use chrono::{Local, Utc};
use log::{debug, error, info};
use mimalloc::MiMalloc;
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
    TInputProtocol, TOutputProtocol,
};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel};

use crate::ds::DataSet;
pub use crate::ds::*;
use crate::rpc::{
    TSCancelOperationReq, TSCloseSessionReq, TSCreateMultiTimeseriesReq, TSCreateTimeseriesReq,
    TSDeleteDataReq, TSExecuteBatchStatementReq, TSExecuteStatementReq, TSExecuteStatementResp,
    TSIServiceSyncClient, TSInsertRecordReq, TSInsertRecordsReq, TSInsertStringRecordsReq,
    TSInsertTabletReq, TSInsertTabletsReq, TSOpenSessionReq, TSOpenSessionResp, TSProtocolVersion,
    TSRawDataQueryReq, TSSetTimeZoneReq, TSStatus, TTSIServiceSyncClient,
};

mod ds;
mod errors;
mod rpc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

type ClientType = TSIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>;

const SUCCESS_CODE: i32 = 200;

/// IotDB datatype enum
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DataType {
    BOOLEAN,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    TEXT,
}

impl From<&String> for DataType {
    fn from(value: &String) -> Self {
        match value.as_str() {
            "BOOLEAN" => DataType::BOOLEAN,
            "INT32" => DataType::INT32,
            "INT64" => DataType::INT64,
            "FLOAT" => DataType::FLOAT,
            "DOUBLE" => DataType::DOUBLE,
            "TEXT" => DataType::TEXT,
            _ => panic!("This '{}' data type doesn't exist", value),
        }
    }
}

impl From<&str> for DataType {
    fn from(value: &str) -> Self {
        match value {
            "BOOLEAN" => DataType::BOOLEAN,
            "INT32" => DataType::INT32,
            "INT64" => DataType::INT64,
            "FLOAT" => DataType::FLOAT,
            "DOUBLE" => DataType::DOUBLE,
            "TEXT" => DataType::TEXT,
            _ => panic!("This '{}' data type doesn't exist", value),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<i32> for DataType {
    fn into(self) -> i32 {
        match self {
            DataType::BOOLEAN => 0,
            DataType::INT32 => 1,
            DataType::INT64 => 2,
            DataType::FLOAT => 3,
            DataType::DOUBLE => 4,
            DataType::TEXT => 5,
        }
    }
}

/// IotDB encoding enum
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Encoding {
    PLAIN,
    PlainDictionary,
    RLE,
    DIFF,
    Ts2diff,
    BITMAP,
    GorillaV1,
    REGULAR,
    GORILLA,
}

impl Default for Encoding {
    fn default() -> Self {
        Encoding::PLAIN
    }
}

impl From<i32> for Encoding {
    fn from(value: i32) -> Self {
        match value {
            0 => Encoding::PLAIN,
            1 => Encoding::PlainDictionary,
            2 => Encoding::RLE,
            3 => Encoding::DIFF,
            4 => Encoding::Ts2diff,
            5 => Encoding::BITMAP,
            6 => Encoding::GorillaV1,
            7 => Encoding::REGULAR,
            8 => Encoding::GORILLA,
            _ => panic!("This '{}' encoding doesn't exist", value),
        }
    }
}

impl From<String> for Encoding {
    fn from(value: String) -> Self {
        match value.as_str() {
            "PLAIN" => Encoding::PLAIN,
            "PlainDictionary" => Encoding::PlainDictionary,
            "RLE" => Encoding::RLE,
            "DIFF" => Encoding::DIFF,
            "Ts2diff" => Encoding::Ts2diff,
            "BITMAP" => Encoding::BITMAP,
            "GorillaV1" => Encoding::GorillaV1,
            "REGULAR" => Encoding::REGULAR,
            "GORILLA" => Encoding::GORILLA,
            _ => panic!("This '{}' encoding doesn't exist", value),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<i32> for Encoding {
    fn into(self) -> i32 {
        match self {
            Encoding::PLAIN => 0,
            Encoding::PlainDictionary => 1,
            Encoding::RLE => 2,
            Encoding::DIFF => 3,
            Encoding::Ts2diff => 4,
            Encoding::BITMAP => 5,
            Encoding::GorillaV1 => 6,
            Encoding::REGULAR => 7,
            Encoding::GORILLA => 8,
        }
    }
}

/// IotDB compressor enum
#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Compressor {
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    SDT,
    PAA,
    PLA,
    LZ4,
}

impl Default for Compressor {
    fn default() -> Self {
        Compressor::SNAPPY
    }
}

impl From<i32> for Compressor {
    fn from(value: i32) -> Self {
        match value {
            0 => Compressor::UNCOMPRESSED,
            1 => Compressor::SNAPPY,
            2 => Compressor::GZIP,
            3 => Compressor::LZO,
            4 => Compressor::SDT,
            5 => Compressor::PAA,
            6 => Compressor::PLA,
            7 => Compressor::LZ4,
            _ => panic!("This '{}' compressor doesn't exist", value),
        }
    }
}

impl From<&str> for Compressor {
    fn from(value: &str) -> Self {
        match value {
            "UNCOMPRESSED" => Compressor::UNCOMPRESSED,
            "SNAPPY" => Compressor::SNAPPY,
            "GZIP" => Compressor::GZIP,
            "LZO" => Compressor::LZO,
            "SDT" => Compressor::SDT,
            "PAA" => Compressor::PAA,
            "PLA" => Compressor::PLA,
            "LZ4" => Compressor::LZ4,
            _ => panic!("This '{}' compressor doesn't exist", value),
        }
    }
}

impl From<String> for Compressor {
    fn from(value: String) -> Self {
        match value.as_str() {
            "UNCOMPRESSED" => Compressor::UNCOMPRESSED,
            "SNAPPY" => Compressor::SNAPPY,
            "GZIP" => Compressor::GZIP,
            "LZO" => Compressor::LZO,
            "SDT" => Compressor::SDT,
            "PAA" => Compressor::PAA,
            "PLA" => Compressor::PLA,
            "LZ4" => Compressor::LZ4,
            _ => panic!("This '{}' compressor doesn't exist", value),
        }
    }
}

#[allow(clippy::from_over_into)]
impl Into<i32> for Compressor {
    fn into(self) -> i32 {
        match self {
            Compressor::UNCOMPRESSED => 0,
            Compressor::SNAPPY => 1,
            Compressor::GZIP => 2,
            Compressor::LZO => 3,
            Compressor::SDT => 4,
            Compressor::PAA => 5,
            Compressor::PLA => 6,
            Compressor::LZ4 => 7,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Endpoint {
    pub host: String,
    pub port: String,
}

impl FromStr for Endpoint {
    type Err = anyhow::Error;

    fn from_str(str: &str) -> anyhow::Result<Self> {
        let host_port: Vec<&str> = str.split(':').collect();
        if host_port.is_empty() || host_port.len() != 2 {
            bail!("Endpoint format error, endpoint: '{}'", str)
            // panic!("Endpoint format error, endpoint: '{}'", str)
        } else {
            Ok(Self {
                host: String::from(host_port[0]),
                port: String::from(host_port[1]),
            })
        }
    }
}

impl Default for Endpoint {
    fn default() -> Self {
        "127.0.0.1:6667".parse::<Self>().unwrap()
    }
}

impl Endpoint {
    pub fn new(host: &str, port: &str) -> Self {
        Self {
            host: String::from(host),
            port: String::from(port),
        }
    }
}

impl ToString for Endpoint {
    fn to_string(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// IotDB Config
#[derive(Clone, Debug)]
pub struct Config {
    pub user: String,
    pub password: String,
    pub time_zone: String,
    pub timeout: i64,
    pub fetch_size: i32,
    pub endpoint: Endpoint,
    pub rpc_compaction: bool,
    pub protocol_version: TSProtocolVersion,
    pub enable_redirect_query: bool,
    pub config_map: BTreeMap<String, String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoint: Endpoint::default(),
            user: "root".to_string(),
            password: "root".to_string(),
            timeout: 3000,
            time_zone: format!("{}{}", Utc::now().offset(), Local::now().offset()),
            fetch_size: 1024,
            rpc_compaction: false,
            protocol_version: TSProtocolVersion::IOTDB_SERVICE_PROTOCOL_V3,
            enable_redirect_query: false,
            config_map: BTreeMap::new(),
        }
    }
}

pub struct ConfigBuilder(Config);

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigBuilder {
    pub fn new() -> Self {
        ConfigBuilder(Config::default())
    }

    pub fn host_port(&mut self, host: &str, port: &str) -> &mut Self {
        self.0.endpoint = Endpoint::new(host, port);
        self
    }

    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.0.endpoint = Endpoint::from_str(endpoint).unwrap();
        self
    }

    pub fn user(&mut self, user: &str) -> &mut Self {
        self.0.user = user.to_string();
        self
    }

    pub fn password(&mut self, password: &str) -> &mut Self {
        self.0.password = password.to_string();
        self
    }

    pub fn timeout(&mut self, timeout: i64) -> &mut Self {
        self.0.timeout = timeout;
        self
    }

    pub fn time_zone(&mut self, time_zone: &str) -> &mut Self {
        self.0.time_zone = time_zone.to_uppercase();
        self
    }

    pub fn fetch_size(&mut self, fetch_size: i32) -> &mut Self {
        self.0.fetch_size = fetch_size;
        self
    }

    pub fn enable_rpc_compaction(&mut self) -> &mut Self {
        self.0.rpc_compaction = true;
        self
    }

    pub fn set_protocol_version(&mut self, protocol_version: TSProtocolVersion) -> &mut Self {
        self.0.protocol_version = protocol_version;
        self
    }

    pub fn enable_redirect_query(&mut self, enable_redirect_query: bool) -> &mut Self {
        self.0.enable_redirect_query = enable_redirect_query;
        self
    }

    pub fn set_config(&mut self, key: &str, value: &str) -> &mut Self {
        self.0.config_map.insert(key.to_string(), value.to_string());
        self
    }

    pub fn set_config_map(&mut self, map: &mut BTreeMap<String, String>) -> &mut Self {
        self.0.config_map.append(map);
        self
    }

    pub fn build(&self) -> Config {
        self.0.clone()
    }
}

pub struct Session {
    client: ClientType,
    config: Config,
    session_id: i64,
    statement_id: i64,
    is_close: bool,
}

impl Session {
    // Open Session
    pub fn connect(config: Config) -> anyhow::Result<Session> {
        debug!("{:#?}", &config);
        let stream = TcpStream::connect(config.endpoint.to_string())?;
        debug!("TcpStream connect to {:?}", config.endpoint);

        let (channel_in, channel_out) = TTcpChannel::with_stream(stream).split()?;

        let (transport_in, transport_out) = (
            TFramedReadTransport::new(channel_in),
            TFramedWriteTransport::new(channel_out),
        );

        let (protocol_in, protocol_out): (Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>);
        if config.rpc_compaction {
            protocol_in = Box::new(TCompactInputProtocol::new(transport_in));
            protocol_out = Box::new(TCompactOutputProtocol::new(transport_out));
            debug!("Create TCompactProtocol client");
        } else {
            protocol_in = Box::new(TBinaryInputProtocol::new(transport_in, true));
            protocol_out = Box::new(TBinaryOutputProtocol::new(transport_out, true));
            debug!("Create TBinaryProtocol client",);
        }

        let mut client = TSIServiceSyncClient::new(protocol_in, protocol_out);

        let open_req = TSOpenSessionReq::new(
            config.protocol_version,
            config.time_zone.clone(),
            config.user.clone(),
            config.password.clone(),
            config.config_map.clone(),
        );

        let TSOpenSessionResp {
            status,
            server_protocol_version,
            session_id,
            configuration: _,
        } = client.open_session(open_req)?;
        if status.code == SUCCESS_CODE {
            if config.protocol_version != server_protocol_version {
                let msg = format!(
                    "Protocol version is different, client is {:?}, server is {:?}",
                    config.protocol_version.clone(),
                    server_protocol_version
                );
                error!("{}", msg);
                bail!(msg)
            } else {
                let statement_id = client.request_statement_id(session_id.unwrap())?;
                debug!(
                    "Open a session,session id: {}, statement id: {} ",
                    session_id.unwrap().clone(),
                    statement_id,
                );

                Ok(Session {
                    client,
                    config,
                    is_close: false,
                    session_id: session_id.unwrap(),
                    statement_id,
                })
            }
        } else {
            let msg = format!("{}", status.message.unwrap_or_else(|| "None".to_string()));
            error!("{}", msg);
            bail!(msg)
        }
    }

    pub fn is_open(&self) -> bool {
        !self.is_close
    }

    pub fn is_close(&self) -> bool {
        self.is_close
    }

    // Close Session
    pub fn close(&mut self) -> anyhow::Result<()> {
        if !self.is_close {
            let req = TSCloseSessionReq::new(self.session_id);
            let status = self.client.close_session(req)?;
            if self.is_success(&status) {
                self.is_close = true;
                debug!("Session closed");
                Ok(())
            } else {
                error!(
                    "Session closed failed, code: {}, reason: {}",
                    status.code.clone(),
                    status.message.clone().unwrap_or_else(|| "None".to_string())
                );
                bail!(status.message.unwrap_or_else(|| "None".to_string()))
            }
        } else {
            Ok(())
        }
    }

    /// Set a storage group
    pub fn set_storage_group(&mut self, storage_group: &str) -> anyhow::Result<()> {
        let status = self
            .client
            .set_storage_group(self.session_id, storage_group.to_string())?;

        if self.is_success(&status) {
            debug!(
                "Set storage group {:?}, message: {:?}",
                storage_group,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// Delete a storage group.
    pub fn delete_storage_group(&mut self, storage_group: &str) -> anyhow::Result<()> {
        debug!("Delete storage group {:?}", storage_group);
        Ok(self.delete_storage_groups(vec![storage_group.to_string()])?)
    }

    /// Delete storage groups.
    pub fn delete_storage_groups(&mut self, storage_groups: Vec<String>) -> anyhow::Result<()> {
        let status = self
            .client
            .delete_storage_groups(self.session_id, storage_groups.clone())?;
        if self.is_success(&status) {
            debug!(
                "Delete storage group(s) {:?}, message: {:?}",
                storage_groups,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// Create single time-series
    pub fn create_time_series(
        &mut self,
        ts_path: &str,
        data_type: DataType,
        encoding: Encoding,
        compressor: Compressor,
    ) -> anyhow::Result<()> {
        let req = TSCreateTimeseriesReq::new(
            self.session_id,
            ts_path.to_string(),
            data_type.into(),
            encoding.into(),
            compressor.into(),
            None,
            None,
            None,
            None,
        );

        match self.check_time_series_exists(ts_path)? {
            true => {
                info!("{} time series exists", ts_path);
                Ok(())
            }
            false => {
                let status = self.client.create_timeseries(req)?;
                if self.is_success(&status) {
                    debug!(
                        "Creat time series {:?}, message: {:?}",
                        ts_path,
                        status.message.unwrap_or_else(|| "None".to_string())
                    );
                    Ok(())
                } else {
                    error!(
                        "{}",
                        status.message.clone().unwrap_or_else(|| "None".to_string())
                    );
                    bail!(status.message.unwrap_or_else(|| "None".to_string()))
                }
            }
        }
    }

    /// Create multiple time-series
    pub fn create_multi_time_series(
        &mut self,
        ts_path_vec: Vec<String>,
        data_type_vec: Vec<i32>,
        encoding_vec: Vec<i32>,
        compressor_vec: Vec<i32>,
    ) -> anyhow::Result<()> {
        let req = TSCreateMultiTimeseriesReq::new(
            self.session_id,
            ts_path_vec.clone(),
            data_type_vec,
            encoding_vec,
            compressor_vec,
            None,
            None,
            None,
            None,
        );
        let status = self.client.create_multi_timeseries(req)?;
        if self.is_success(&status) {
            debug!(
                "Creating multiple time series {:?}, message: {:?}",
                ts_path_vec,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// Delete multiple time series
    pub fn delete_time_series(&mut self, path_vec: Vec<String>) -> anyhow::Result<()> {
        let status = self
            .client
            .delete_timeseries(self.session_id, path_vec.clone())?;
        if self.is_success(&status) {
            debug!(
                "Deleting multiple time series {:?}, message: {:?}",
                path_vec,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// Check whether a specific time-series exists
    pub fn check_time_series_exists(&mut self, path: &str) -> anyhow::Result<bool> {
        let config = self.config.clone();
        let statement = format!("SHOW TIMESERIES {}", path);
        let req = TSExecuteStatementReq::new(
            self.session_id,
            statement,
            self.statement_id,
            config.fetch_size,
            config.timeout,
            config.enable_redirect_query,
            false,
        );

        match self.client.execute_query_statement(req)? {
            TSExecuteStatementResp { query_data_set, .. } => {
                if query_data_set.is_none() {
                    Ok(false)
                } else {
                    Ok(query_data_set.unwrap().value_list.is_empty())
                }
            }
        }
    }

    /// Delete all data <= time in multiple time-series
    pub fn delete_data(&mut self, path_vec: Vec<String>, timestamp: i64) -> anyhow::Result<()> {
        let req = TSDeleteDataReq::new(self.session_id, path_vec.clone(), 0, timestamp);
        let status = self.client.delete_data(req)?;
        if self.is_success(&status) {
            debug!(
                "Delete data from {:?}, message: {:?}",
                path_vec,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// special case for inserting one row of String (TEXT) value
    pub fn insert_string_records(
        &mut self,
        device_ids: Vec<String>,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<String>>,
        is_aligned: bool,
    ) -> anyhow::Result<()> {
        let req = TSInsertStringRecordsReq::new(
            self.session_id,
            device_ids.clone(),
            measurements_list,
            values_list,
            timestamps,
            is_aligned,
        );

        let status = self.client.insert_string_records(req)?;
        if self.is_success(&status) {
            debug!(
                "Insert string records to device {:?}, message: {:?}",
                device_ids,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// Insert record
    pub fn insert_record(
        &mut self,
        device_id: &str,
        timestamp: i64,
        measurements: Vec<String>,
        values: Vec<u8>,
        is_aligned: bool,
    ) -> anyhow::Result<()> {
        let req = TSInsertRecordReq::new(
            self.session_id,
            device_id.to_string(),
            measurements,
            values,
            timestamp,
            is_aligned,
        );

        let status = self.client.insert_record(req)?;
        if self.is_success(&status) {
            debug!(
                "Insert one record to device {:?}, message: {:?}",
                device_id,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// this method NOT insert data into database and the server just return after accept the
    /// request, this method should be used to test other time cost in client
    pub fn test_insert_record(
        &mut self,
        prefix_path: &str,
        timestamp: i64,
        measurements: Vec<String>,
        values: Vec<u8>,
        is_aligned: bool,
    ) -> anyhow::Result<()> {
        let req = TSInsertRecordReq::new(
            self.session_id,
            prefix_path.to_string(),
            measurements,
            values,
            timestamp,
            is_aligned,
        );
        let status = self.client.test_insert_record(req)?;
        if self.is_success(&status) {
            debug!(
                "Testing! insert one record to prefix path {:?}, message: {:?}",
                prefix_path,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// Insert records
    pub fn insert_records(
        &mut self,
        prefix_paths: Vec<String>,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<u8>>,
        is_aligned: bool,
    ) -> anyhow::Result<()> {
        let req = TSInsertRecordsReq::new(
            self.session_id,
            prefix_paths.clone(),
            measurements_list,
            values_list,
            timestamps,
            is_aligned,
        );
        let status = self.client.insert_records(req)?;
        if self.is_success(&status) {
            debug!(
                "Insert multiple records to prefix path {:?}, message: {:?}",
                prefix_paths,
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// this method NOT insert data into database and the server just return after accept the
    /// request, this method should be used to test other time cost in client
    pub fn test_insert_records(
        &mut self,
        prefix_paths: Vec<String>,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<u8>>,
        is_aligned: bool,
    ) -> anyhow::Result<()> {
        let req = TSInsertRecordsReq::new(
            self.session_id,
            prefix_paths,
            measurements_list,
            values_list,
            timestamps,
            is_aligned,
        );
        let status = self.client.test_insert_records(req)?;
        if self.is_success(&status) {
            debug!(
                "Testing! insert multiple records, message: {:?}",
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// insert one tablet, in a tablet, for each timestamp, the number of measurements is same
    ///     for example three records in the same device can form a tablet:
    ///         timestamps,     m1,    m2,     m3
    ///                  1,  125.3,  True,  text1
    ///                  2,  111.6, False,  text2
    ///                  3,  688.6,  True,  text3
    /// Notice: The tablet should not have empty cell
    ///         The tablet itself is sorted
    /// TODO
    #[allow(clippy::too_many_arguments)]
    pub fn insert_tablet(
        &mut self,
        prefix_path: &str,
        measurements: Vec<String>,
        values: Vec<u8>,
        timestamps: Vec<u8>,
        types: Vec<i32>,
        size: i32,
        is_aligned: bool,
    ) -> anyhow::Result<()> {
        let req = TSInsertTabletReq::new(
            self.session_id,
            prefix_path.to_string(),
            measurements,
            values,
            timestamps,
            types,
            size,
            is_aligned,
        );
        let status = self.client.insert_tablet(req)?;
        if self.is_success(&status) {
            debug!(
                "Testing! insert multiple records, message: {:?}",
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// insert multiple tablets, tablets are independent to each other
    /// TODO
    #[allow(clippy::too_many_arguments)]
    pub fn insert_tablets(
        &mut self,
        prefix_paths: Vec<String>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<u8>>,
        timestamps_list: Vec<Vec<u8>>,
        types_list: Vec<Vec<i32>>,
        size_list: Vec<i32>,
        is_aligned: bool,
    ) -> anyhow::Result<()> {
        let req = TSInsertTabletsReq::new(
            self.session_id,
            prefix_paths,
            measurements_list,
            values_list,
            timestamps_list,
            types_list,
            size_list,
            is_aligned,
        );
        let status = self.client.insert_tablets(req)?;
        if self.is_success(&status) {
            debug!(
                "Testing! insert multiple records, message: {:?}",
                status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(())
        } else {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// TODO
    pub fn insert_records_of_one_device() {}

    /// TODO
    pub fn insert_records_of_one_device_sorte() {}

    /// TODO
    pub fn gen_insert_records_of_one_device_request() {}

    /// this method NOT insert data into database and the server just return after accept the
    /// request, this method should be used to test other time cost in client
    /// TODO
    pub fn test_insert_table() {}

    /// this method NOT insert data into database and the server just return after accept the
    /// request, this method should be used to test other time cost in client
    /// TODO
    pub fn test_insert_tablets() {}

    /// TODO
    pub fn gen_insert_tablet_req() {}

    /// TODO
    pub fn gen_insert_tablets_req() {}

    pub fn sql(&mut self, sql: &str) -> anyhow::Result<DataSet> {
        self.exec(sql)
    }

    /// execute query sql statement and return a DataSet
    fn exec(&mut self, statement: &str) -> anyhow::Result<DataSet> {
        debug!("Exec statement \"{}\"", statement);
        let req = TSExecuteStatementReq::new(
            self.session_id,
            statement.to_string(),
            self.statement_id,
            self.config.fetch_size,
            self.config.timeout,
            self.config.enable_redirect_query,
            false,
        );
        let resp = self.client.execute_statement(req)?;
        let status = resp.clone().status;
        let msg = status.clone().message.unwrap_or_else(|| "None".to_string());
        if self.is_success(&status) {
            debug!(
                "Execute statement {:?}, message: {:?}",
                statement,
                msg.clone()
            );
            Ok(DataSet::new(resp))
        } else {
            error!("{}", msg.clone());
            bail!(msg)
        }
    }

    /// execute batch statement and return a DataSets
    pub fn exec_batch(&mut self, statements: Vec<String>) -> anyhow::Result<()> {
        let req = TSExecuteBatchStatementReq::new(self.session_id, statements);
        let status = self.client.execute_batch_statement(req)?;
        let msg = status.clone().message.unwrap_or_else(|| "None".to_string());
        if self.is_success(&status) {
            info!("{}", msg);
            Ok(())
        } else {
            error!("{}", msg);
            bail!(msg)
        }
    }

    /// execute query sql statement and return a DataSet
    pub fn exec_query(&mut self, query: &str) -> anyhow::Result<DataSet> {
        debug!("Exec query \"{}\"", &query);
        let req = TSExecuteStatementReq::new(
            self.session_id,
            query.to_string(),
            self.statement_id,
            self.config.fetch_size,
            self.config.timeout,
            self.config.enable_redirect_query,
            false,
        );

        let resp = self.client.execute_query_statement(req)?;
        if self.is_success(&resp.status) {
            debug!(
                "Execute query {:?}, message: {:?}",
                query,
                resp.status
                    .clone()
                    .message
                    .unwrap_or_else(|| "None".to_string())
            );
            Ok(DataSet::new(resp))
        } else {
            error!(
                "Exec query failed, code: {}, reason: {}",
                resp.status.code.clone(),
                resp.status
                    .clone()
                    .message
                    .unwrap_or_else(|| "None".to_string())
            );
            bail!(resp.status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// execute update statement and return a DataSet
    pub fn exec_update(&mut self, statement: &str) -> anyhow::Result<DataSet> {
        let req = TSExecuteStatementReq::new(
            self.session_id,
            statement.to_string(),
            self.statement_id,
            self.config.fetch_size,
            self.config.timeout,
            self.config.enable_redirect_query,
            false,
        );

        let resp = self.client.execute_update_statement(req)?;
        if self.is_success(&resp.status) {
            debug!(
                "Execute update statement {:?}, message: {:?}",
                statement,
                resp.status
                    .clone()
                    .message
                    .unwrap_or_else(|| "None".to_string())
            );
            Ok(DataSet::new(resp))
        } else {
            error!(
                "{}",
                resp.status
                    .message
                    .clone()
                    .unwrap_or_else(|| "None".to_string())
            );
            bail!(resp.status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// execute row statement and return a DataSets
    pub fn exec_raw_data_query(
        &mut self,
        paths: Vec<String>,
        start_time: i64,
        end_time: i64,
    ) -> anyhow::Result<DataSet> {
        let req = TSRawDataQueryReq::new(
            self.session_id,
            paths,
            self.config.fetch_size,
            start_time,
            end_time,
            self.statement_id,
            self.config.enable_redirect_query,
            false,
        );
        let resp = self.client.execute_raw_data_query(req)?;
        if self.is_success(&resp.status) {
            Ok(DataSet::new(resp))
        } else {
            error!(
                "{}",
                resp.status
                    .message
                    .clone()
                    .unwrap_or_else(|| "None".to_string())
            );
            bail!(resp.status.message.unwrap_or_else(|| "None".to_string()))
        }
    }

    /// Set time zone
    pub fn set_time_zone(&mut self, time_zone: &str) -> anyhow::Result<()> {
        let req = TSSetTimeZoneReq::new(self.session_id, time_zone.to_string());
        let status = self.client.set_time_zone(req)?;
        if !self.is_success(&status) {
            error!(
                "{}",
                status.message.clone().unwrap_or_else(|| "None".to_string())
            );
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        } else {
            Ok(())
        }
    }

    /// Get time zone
    pub fn time_zone(&mut self) -> anyhow::Result<String> {
        let resp = self.client.get_time_zone(self.session_id)?;
        if self.is_success(&resp.status) {
            Ok(resp.time_zone)
        } else {
            error!(
                "{}",
                resp.status.message.unwrap_or_else(|| "None".to_string())
            );
            Ok(String::new())
        }
    }

    /// Verify success status of operation
    fn is_success(&self, status: &TSStatus) -> bool {
        status.code == SUCCESS_CODE
    }

    /// Cancel operation
    #[allow(dead_code)]
    fn cancel_operation(&mut self, query_id: i64) -> anyhow::Result<()> {
        let req = TSCancelOperationReq::new(self.session_id, query_id);
        let status = self.client.cancel_operation(req)?;
        if !self.is_success(&status) {
            let msg = format!("Cancel operation failed,'{:?}'", query_id);
            error!("{}", msg);
            bail!(status.message.unwrap_or_else(|| "None".to_string()))
        } else {
            Ok(())
        }
    }
}
