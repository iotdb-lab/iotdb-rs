//! ![Logo](https://raw.githubusercontent.com/francis-du/iotdb-rs/main/iotdb-rs.png)
//!
//! [![Crates.io](https://img.shields.io/crates/v/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
//! [![Api Docs](https://img.shields.io/badge/Api-Doc-a94064?style=flat-square&color=%23E5531A)](https://docs.rs/iotdb)
//! [![Crates.io](https://img.shields.io/crates/d/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
//! [![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square&color=%23E5531A)](https://github.com/francis-du/iotdb-rs/blob/main/LICENSE)
//! [![Rust Build](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-test?label=build&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-test)
//! [![Crates Publish](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-publish?label=publish&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-publish)
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
pub mod common;
pub mod dataset;
pub mod rpc;

pub use chrono;
pub use thrift;

#[macro_use]
extern crate prettytable;

use crate::common::{Compressor, DataType, Encoding, Logger};
use crate::dataset::DataSet;
use crate::rpc::{
    TSCancelOperationReq, TSCloseSessionReq, TSCreateMultiTimeseriesReq, TSCreateTimeseriesReq,
    TSDeleteDataReq, TSExecuteBatchStatementReq, TSExecuteStatementReq, TSIServiceSyncClient,
    TSInsertRecordReq, TSInsertRecordsReq, TSInsertStringRecordsReq, TSInsertTabletReq,
    TSInsertTabletsReq, TSOpenSessionReq, TSProtocolVersion, TSRawDataQueryReq, TSSetTimeZoneReq,
    TSStatus, TTSIServiceSyncClient,
};
use chrono::{Local, Utc};
use log::{debug, error, info};
use simplelog::*;
use std::collections::BTreeMap;
use std::net::TcpStream;
use std::str::FromStr;
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
    TInputProtocol, TOutputProtocol,
};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel};
use thrift::{ApplicationErrorKind, Error as ThriftError, ProtocolErrorKind};

type ClientType = TSIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>;

const SUCCESS_CODE: i32 = 200;

#[derive(Clone, Debug)]
pub struct Endpoint {
    pub host: String,
    pub port: String,
}

impl FromStr for Endpoint {
    type Err = ();

    fn from_str(str: &str) -> Result<Self, Self::Err> {
        let host_port: Vec<&str> = str.split(':').collect();
        if host_port.is_empty() || host_port.len() != 2 {
            panic!("Endpoint format error, endpoint: '{}'", str)
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
    pub log_level: LevelFilter,
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
            log_level: LevelFilter::Info,
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

    pub fn endpoint(&mut self, host: &str, port: &str) -> &mut Self {
        self.0.endpoint = Endpoint::new(host, port);
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

    pub fn log_level(&mut self, level: LevelFilter) -> &mut Self {
        self.0.log_level = level;
        self
    }

    pub fn debug(&mut self, debug: bool) -> &mut Self {
        if debug {
            self.0.log_level = LevelFilter::Debug
        }
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
    pub fn new(config: Config) -> Session {
        Logger::init(config.log_level);
        debug!("{:#?}", &config);

        let stream = TcpStream::connect(config.endpoint.to_string()).unwrap_or_else(|error| {
            panic!("{:?}, reason: {:?}", config.endpoint, error.to_string())
        });
        debug!("TcpStream connect to {:?}", config.endpoint);

        let channel = TTcpChannel::with_stream(stream);
        let (channel_in, channel_out) = channel.split().unwrap();
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

        Self {
            client: TSIServiceSyncClient::new(protocol_in, protocol_out),
            config,
            session_id: -1,
            statement_id: -1,
            is_close: true,
        }
    }

    // Open Session
    pub fn open(mut self) -> Result<Session, ThriftError> {
        let open_req = TSOpenSessionReq::new(
            self.config.protocol_version,
            self.config.time_zone.clone(),
            self.config.user.clone(),
            self.config.password.clone(),
            self.config.config_map.clone(),
        );

        match self.client.open_session(open_req) {
            Ok(resp) => {
                let status = resp.status;
                if self.is_success(&status) {
                    if self.config.protocol_version != resp.server_protocol_version {
                        self.is_close = true;
                        let msg = format!(
                            "Protocol version is different, client is {:?}, server is {:?}",
                            self.config.protocol_version.clone(),
                            resp.server_protocol_version
                        );
                        error!("{}", msg);
                        Err(thrift::new_protocol_error(
                            ProtocolErrorKind::BadVersion,
                            msg,
                        ))
                    } else {
                        self.is_close = false;
                        self.session_id = resp.session_id.unwrap();
                        self.statement_id = self.client.request_statement_id(self.session_id)?;
                        debug!(
                            "Open a session,session id: {}, statement id: {} ",
                            self.session_id.clone(),
                            self.statement_id.clone()
                        );
                        Ok(self)
                    }
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap_or_else(|| "None".to_string()),
                    ))
                }
            }
            Err(error) => {
                self.is_close = true;
                Err(error)
            }
        }
    }

    pub fn is_open(&self) -> bool {
        !self.is_close
    }

    pub fn is_close(&self) -> bool {
        self.is_close
    }

    // Close Session
    pub fn close(&mut self) -> Result<(), ThriftError> {
        if self.is_close {
            Ok(())
        } else {
            let req = TSCloseSessionReq::new(self.session_id);
            match self.client.close_session(req) {
                Ok(status) => {
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
                        Err(thrift::new_application_error(
                            ApplicationErrorKind::MissingResult,
                            status.message.unwrap_or_else(|| "None".to_string()),
                        ))
                    }
                }
                Err(error) => Err(error),
            }
        }
    }

    /// Set a storage group
    pub fn set_storage_group(&mut self, storage_group: &str) -> Result<(), ThriftError> {
        match self
            .client
            .set_storage_group(self.session_id, storage_group.to_string())
        {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Set storage group {:?}, message: {:?}",
                        storage_group,
                        status.message.unwrap_or_else(|| "None".to_string())
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap_or_else(|| "None".to_string()),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Delete a storage group.
    pub fn delete_storage_group(&mut self, storage_group: &str) -> Result<(), ThriftError> {
        debug!("Delete storage group {:?}", storage_group);
        self.delete_storage_groups(vec![storage_group.to_string()])
    }

    /// Delete storage groups.
    pub fn delete_storage_groups(
        &mut self,
        storage_groups: Vec<String>,
    ) -> Result<(), ThriftError> {
        match self
            .client
            .delete_storage_groups(self.session_id, storage_groups.clone())
        {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Delete storage group(s) {:?}, message: {:?}",
                        storage_groups,
                        status.message.unwrap_or_else(|| "None".to_string())
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap_or_else(|| "None".to_string()),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Create single time-series
    pub fn create_time_series(
        &mut self,
        ts_path: &str,
        data_type: DataType,
        encoding: Encoding,
        compressor: Compressor,
    ) -> Result<(), ThriftError> {
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

        match self.check_time_series_exists(ts_path) {
            Ok(exists) => {
                if exists {
                    Ok(())
                } else {
                    match self.client.create_timeseries(req) {
                        Ok(status) => {
                            if self.is_success(&status) {
                                debug!(
                                    "Creat time series {:?}, message: {:?}",
                                    ts_path,
                                    status.message.unwrap()
                                );
                                Ok(())
                            } else {
                                error!("{}", status.message.clone().unwrap());
                                Err(thrift::new_application_error(
                                    ApplicationErrorKind::MissingResult,
                                    status.message.unwrap(),
                                ))
                            }
                        }
                        Err(error) => Err(error),
                    }
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Create multiple time-series
    pub fn create_multi_time_series(
        &mut self,
        ts_path_vec: Vec<String>,
        data_type_vec: Vec<i32>,
        encoding_vec: Vec<i32>,
        compressor_vec: Vec<i32>,
    ) -> Result<(), ThriftError> {
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
        match self.client.create_multi_timeseries(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Creating multiple time series {:?}, message: {:?}",
                        ts_path_vec,
                        status.message.unwrap()
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Delete multiple time series
    pub fn delete_time_series(&mut self, path_vec: Vec<String>) -> Result<(), ThriftError> {
        match self
            .client
            .delete_timeseries(self.session_id, path_vec.clone())
        {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Deleting multiple time series {:?}, message: {:?}",
                        path_vec,
                        status.message.unwrap()
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Check whether a specific time-series exists
    pub fn check_time_series_exists(&mut self, path: &str) -> Result<bool, ThriftError> {
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

        match self.client.execute_query_statement(req) {
            Ok(resp) => match resp.query_data_set {
                None => Err(thrift::new_application_error(
                    ApplicationErrorKind::MissingResult,
                    resp.status.message.unwrap(),
                )),
                Some(data_set) => {
                    if data_set.value_list.is_empty() {
                        Ok(false)
                    } else {
                        Ok(true)
                    }
                }
            },
            Err(error) => Err(error),
        }
    }

    /// Delete all data <= time in multiple time-series
    pub fn delete_data(
        &mut self,
        path_vec: Vec<String>,
        timestamp: i64,
    ) -> Result<(), ThriftError> {
        let req = TSDeleteDataReq::new(self.session_id, path_vec.clone(), 0, timestamp);
        match self.client.delete_data(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Delete data from {:?}, message: {:?}",
                        path_vec,
                        status.message.unwrap()
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
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
    ) -> Result<(), ThriftError> {
        let req = TSInsertStringRecordsReq::new(
            self.session_id,
            device_ids.clone(),
            measurements_list,
            values_list,
            timestamps,
            is_aligned,
        );
        match self.client.insert_string_records(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Insert string records to device {:?}, message: {:?}",
                        device_ids,
                        status.message.unwrap()
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
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
    ) -> Result<(), ThriftError> {
        let req = TSInsertRecordReq::new(
            self.session_id,
            device_id.to_string(),
            measurements,
            values,
            timestamp,
            is_aligned,
        );
        match self.client.insert_record(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Insert one record to device {:?}, message: {:?}",
                        device_id,
                        status.message.unwrap()
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
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
    ) -> Result<(), ThriftError> {
        let req = TSInsertRecordReq::new(
            self.session_id,
            prefix_path.to_string(),
            measurements,
            values,
            timestamp,
            is_aligned,
        );
        match self.client.test_insert_record(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Testing! insert one record to prefix path {:?}, message: {:?}",
                        prefix_path,
                        status.message.unwrap()
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
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
    ) -> Result<(), ThriftError> {
        let req = TSInsertRecordsReq::new(
            self.session_id,
            prefix_paths.clone(),
            measurements_list,
            values_list,
            timestamps,
            is_aligned,
        );
        match self.client.insert_records(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Insert multiple records to prefix path {:?}, message: {:?}",
                        prefix_paths,
                        status.message.unwrap()
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
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
    ) -> Result<(), ThriftError> {
        let req = TSInsertRecordsReq::new(
            self.session_id,
            prefix_paths,
            measurements_list,
            values_list,
            timestamps,
            is_aligned,
        );
        match self.client.test_insert_records(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "Testing! insert multiple records, message: {:?}",
                        status.message.unwrap()
                    );
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
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
    ) -> Result<TSStatus, ThriftError> {
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
        self.client.insert_tablet(req)
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
    ) -> Result<TSStatus, ThriftError> {
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
        self.client.insert_tablets(req)
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

    pub fn sql(&mut self, sql: &str) -> Result<DataSet, ThriftError> {
        self.exec(sql)
    }

    /// execute query sql statement and return a DataSet
    fn exec(&mut self, statement: &str) -> Result<DataSet, ThriftError> {
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

        match self.client.execute_statement(req) {
            Ok(resp) => {
                if self.is_success(&resp.status) {
                    debug!(
                        "Execute statement {:?}, message: {:?}",
                        statement,
                        resp.status
                            .clone()
                            .message
                            .unwrap_or_else(|| "None".to_string())
                    );
                    Ok(DataSet::new(
                        statement.to_string(),
                        self.session_id,
                        self.config.fetch_size,
                        resp,
                    ))
                } else {
                    error!("{}", resp.status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        resp.status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// execute batch statement and return a DataSets
    pub fn exec_batch(&mut self, statements: Vec<String>) {
        let req = TSExecuteBatchStatementReq::new(self.session_id, statements);
        match self.client.execute_batch_statement(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    info!(
                        "Execute statements {:?}",
                        status.message.unwrap_or_else(|| "None".to_string())
                    );
                } else {
                    error!("{}", status.message.unwrap());
                }
            }
            Err(error) => error!("{}", error),
        }
    }

    /// execute query sql statement and return a DataSet
    pub fn exec_query(&mut self, query: &str) -> Result<DataSet, ThriftError> {
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

        match self.client.execute_query_statement(req) {
            Ok(resp) => {
                if self.is_success(&resp.status) {
                    debug!(
                        "Execute query {:?}, message: {:?}",
                        query,
                        resp.status
                            .clone()
                            .message
                            .unwrap_or_else(|| "None".to_string())
                    );
                    Ok(DataSet::new(
                        query.to_string(),
                        self.session_id,
                        self.config.fetch_size,
                        resp,
                    ))
                } else {
                    error!(
                        "Exec query failed, code: {}, reason: {}",
                        resp.status.code.clone(),
                        resp.status
                            .clone()
                            .message
                            .unwrap_or_else(|| "None".to_string())
                    );
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        resp.status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// execute update statement and return a DataSet
    pub fn exec_update(&mut self, statement: &str) -> Result<DataSet, ThriftError> {
        let req = TSExecuteStatementReq::new(
            self.session_id,
            statement.to_string(),
            self.statement_id,
            self.config.fetch_size,
            self.config.timeout,
            self.config.enable_redirect_query,
            false,
        );

        match self.client.execute_update_statement(req) {
            Ok(resp) => {
                if self.is_success(&resp.status) {
                    debug!(
                        "Execute update statement {:?}, message: {:?}",
                        statement,
                        resp.status
                            .clone()
                            .message
                            .unwrap_or_else(|| "None".to_string())
                    );
                    Ok(DataSet::new(
                        statement.to_string(),
                        self.session_id,
                        self.config.fetch_size,
                        resp,
                    ))
                } else {
                    error!("{}", resp.status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        resp.status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// execute row statement and return a DataSets
    pub fn exec_raw_data_query(
        &mut self,
        paths: Vec<String>,
        start_time: i64,
        end_time: i64,
    ) -> Result<DataSet, ThriftError> {
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

        match self.client.execute_raw_data_query(req) {
            Ok(resp) => {
                if self.is_success(&resp.status) {
                    Ok(DataSet::new(
                        "".to_string(),
                        self.session_id,
                        self.config.fetch_size,
                        resp,
                    ))
                } else {
                    error!("{}", resp.status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        resp.status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Set time zone
    pub fn set_time_zone(&mut self, time_zone: &str) -> Result<(), ThriftError> {
        let req = TSSetTimeZoneReq::new(self.session_id, time_zone.to_string());
        match self.client.set_time_zone(req) {
            Ok(status) => {
                if status.code == 200 {
                    Ok(())
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Get time zone
    pub fn time_zone(&mut self) -> Result<String, ThriftError> {
        match self.client.get_time_zone(self.session_id) {
            Ok(resp) => {
                if resp.status.code == 200 {
                    Ok(resp.time_zone)
                } else {
                    error!("{}", resp.status.message.unwrap());
                    Ok(String::new())
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Verify success status of operation
    fn is_success(&self, status: &TSStatus) -> bool {
        status.code == SUCCESS_CODE
    }

    /// Cancel operation
    #[allow(dead_code)]
    fn cancel_operation(&mut self, query_id: i64) -> Result<(), ThriftError> {
        let req = TSCancelOperationReq::new(self.session_id, query_id);
        match self.client.cancel_operation(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    Ok(())
                } else {
                    let msg = format!("Cancel operation failed,'{:?}'", query_id);
                    error!("{}", msg);
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        msg,
                    ))
                }
            }
            Err(error) => Err(error),
        }
    }
}
