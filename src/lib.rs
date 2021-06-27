//! ![Logo](http://iotdb.apache.org/img/logo.png)
//!
//! [![Crates.io](https://img.shields.io/crates/v/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
//! [![Api Docs](https://img.shields.io/badge/Api-Doc-a94064?style=flat-square&color=%23E5531A)](https://docs.rs/iotdb)
//! [![Crates.io](https://img.shields.io/crates/d/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
//! [![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square&color=%23E5531A)](https://github.com/francis-du/iotdb-rs/blob/main/LICENSE)
//! [![Rust Build](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-test?label=build&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-test)
//! [![Crates Publish](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-publish?label=publish&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-publish)
//!
//! (WIP) Apache IotDB Client written in Rust
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
//! iotdb = "0.0.3"
//! ```
//!
//! # Example
//!
//! ```rust
//! use thrift::Error;
//!
//! use iotdb::util::{Compressor, TSDataType, TSEncoding};
//! use iotdb::Client;
//! use iotdb::Session;
//!
//! fn main() -> Result<(), Error> {
//!    let client = Client::new("localhost", "6667")
//!         // .enable_rpc_compaction()
//!         .create()?;
//!
//!     // open session
//!     let mut session = Session::new(client);
//!
//!     session
//!         .user("root")
//!         .password("root")
//!         .zone_id("UTC+8")
//!         .open()?;
//!
//!     let storage_group = "root.ln";
//!     session.delete_storage_group(storage_group)?;
//!     session.set_storage_group(storage_group)?;
//!
//!     session.create_time_series(
//!         "root.ln.wf01.wt01.temperature",
//!         TSDataType::FLOAT,
//!         TSEncoding::RLE,
//!         Compressor::default(),
//!     )?;
//!
//!     session.create_time_series(
//!         "root.ln.wf01.wt01.humidity",
//!         TSDataType::FLOAT,
//!         TSEncoding::RLE,
//!         Compressor::default(),
//!     )?;
//!
//!     session.exec_insert("insert into root.ln.wf01.wt01(temperature, humidity) values (36,20)");
//!     session.exec_insert("insert into root.ln.wf01.wt01(temperature, humidity) values (37,26)");
//!     session.exec_insert("insert into root.ln.wf01.wt01(temperature, humidity) values (29,16)");
//!
//!     session.exec_query("SHOW STORAGE GROUP").show();
//!
//!     if session.check_time_series_exists("root.ln") {
//!        session.exec_query("SHOW TIMESERIES root.ln").show();
//!         session.exec_query("select * from root.ln").show();
//!     }
//!
//!     session
//!         .exec_update("delete from root.ln.wf01.wt01.temperature where time<=2017-11-01T16:26:00")
//!         .show();
//!
//!     session.close()?;
//!
//!     Ok(())
//! }
//! ```

pub mod errors;
pub mod rpc;
pub mod util;

use crate::rpc::{
    TSCancelOperationReq, TSCloseSessionReq, TSCreateMultiTimeseriesReq, TSCreateTimeseriesReq,
    TSDeleteDataReq, TSExecuteBatchStatementReq, TSExecuteStatementReq, TSExecuteStatementResp,
    TSIServiceSyncClient, TSInsertRecordReq, TSInsertRecordsReq, TSInsertStringRecordsReq,
    TSInsertTabletReq, TSInsertTabletsReq, TSOpenSessionReq, TSProtocolVersion, TSQueryDataSet,
    TSQueryNonAlignDataSet, TSRawDataQueryReq, TSSetTimeZoneReq, TSStatus, TTSIServiceSyncClient,
};
use crate::util::{Compressor, TSDataType, TSEncoding};
use chrono::{Local, Utc};
use log::{debug, error, trace};
use prettytable::{Cell, Row, Table};
use std::collections::{BTreeMap, HashMap};
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
    TInputProtocol, TOutputProtocol,
};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel};
use thrift::{ApplicationErrorKind, Error, ProtocolErrorKind, TransportErrorKind};

type ClientType = TSIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>;

const SUCCESS_CODE: i32 = 200;
const TIMESTAMP_STR: &'static str = "Time";
const START_INDEX: i32 = 2;
const FLAG: i32 = 0x80;

pub struct DataSet {
    statement: String,
    session_id: i64,
    fetch_size: i32,
    query_id: Option<i64>,
    columns: Option<Vec<String>>,
    data_types: Option<Vec<String>>,
    column_name_index_map: Option<BTreeMap<String, i32>>,
    ignore_timestamp: Option<bool>,
    query_data_set: Option<TSQueryDataSet>,
    non_align_query_data_set: Option<TSQueryNonAlignDataSet>,
}

impl Default for DataSet {
    fn default() -> Self {
        Self {
            statement: "".to_string(),
            session_id: -1,
            fetch_size: -1,
            query_id: None,
            columns: None,
            data_types: None,
            column_name_index_map: None,
            ignore_timestamp: None,
            query_data_set: None,
            non_align_query_data_set: None,
        }
    }
}

impl DataSet {
    pub fn new(
        statement: String,
        session_id: i64,
        fetch_size: i32,
        resp: TSExecuteStatementResp,
    ) -> DataSet {
        Self {
            statement,
            session_id,
            fetch_size,
            query_id: resp.query_id,
            columns: resp.columns,
            data_types: resp.data_type_list,
            column_name_index_map: resp.column_name_index_map,
            ignore_timestamp: resp.ignore_time_stamp,
            query_data_set: resp.query_data_set,
            non_align_query_data_set: resp.non_align_query_data_set,
        }
    }

    pub fn show(self) {
        println!("Statement => {}", self.statement);
        let mut table = Table::new();

        match self.columns {
            Some(columns) => {
                // Add Columns
                let mut cells: Vec<Cell> = vec![];
                for cell in columns {
                    cells.push(Cell::new(cell.as_str()))
                }
                table.add_row(Row::new(cells));

                // Add values rows
                match self.query_data_set {
                    Some(dataset) => {
                        let mut cells: Vec<Cell> = vec![];
                        for row in dataset.value_list {
                            match String::from_utf8(row) {
                                Ok(string) => {
                                    // TODO need to parse row record
                                    cells.push(Cell::new(string.as_str()))
                                }
                                Err(errors) => println!("Decode error: {}", errors),
                            }
                        }
                        table.add_row(Row::new(cells));
                    }
                    None => {
                        let mut cells: Vec<Cell> = vec![];
                        cells.push(Cell::new("Empty"));
                        table.add_row(Row::new(cells));
                    }
                }
            }
            None => {
                let mut cells: Vec<Cell> = vec![];
                cells.push(Cell::new("     "));
                table.add_row(Row::new(cells));
            }
        }
        table.printstd();
        print!("\n");
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    host: String,
    port: String,
    rpc_compaction: bool,
}

impl Default for Client {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: "6667".to_string(),
            rpc_compaction: false,
        }
    }
}

impl Client {
    pub fn new(host: &str, port: &str) -> Client {
        Self {
            host: host.to_string(),
            port: port.to_string(),
            rpc_compaction: Client::default().rpc_compaction,
        }
    }

    pub fn enable_rpc_compaction(&mut self) -> &mut Client {
        self.rpc_compaction = false;
        self
    }

    pub fn create(&mut self) -> thrift::Result<ClientType> {
        trace!("Create a IotDB client");

        let mut channel = TTcpChannel::new();
        channel.open(format!("{}:{}", self.host, self.port).as_str())?;
        let (channel_in, channel_out) = channel.split()?;

        let (in_protocol, out_protocol): (Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>);
        if self.rpc_compaction {
            in_protocol = Box::new(TCompactInputProtocol::new(TFramedReadTransport::new(
                channel_in,
            )));
            out_protocol = Box::new(TCompactOutputProtocol::new(TFramedWriteTransport::new(
                channel_out,
            )));
            debug!("Create a compaction client");
        } else {
            in_protocol = Box::new(TBinaryInputProtocol::new(
                TFramedReadTransport::new(channel_in),
                true,
            ));
            out_protocol = Box::new(TBinaryOutputProtocol::new(
                TFramedWriteTransport::new(channel_out),
                true,
            ));
            debug!("Create a binary client");
        }
        Ok(TSIServiceSyncClient::new(in_protocol, out_protocol))
    }
}

pub struct Session {
    user: String,
    password: String,
    zone_id: String,
    fetch_size: i32,
    session_id: i64,
    statement_id: i64,
    is_close: bool,
    protocol_version: TSProtocolVersion,
    config: BTreeMap<String, String>,
    client: ClientType,
}

impl Session {
    pub fn new(client: ClientType) -> Session {
        let tz = format!("{}{}", Utc::now().offset(), Local::now().offset());
        Self {
            user: "root".to_string(),
            password: "root".to_string(),
            zone_id: tz,
            fetch_size: 1024,
            session_id: -1,
            statement_id: -1,
            is_close: true,
            protocol_version: TSProtocolVersion::IotdbServiceProtocolV3,
            config: BTreeMap::new(),
            client,
        }
    }

    pub fn user(&mut self, user: &str) -> &mut Session {
        self.user = user.to_string();
        self
    }

    pub fn password(&mut self, password: &str) -> &mut Session {
        self.password = password.to_string();
        self
    }

    pub fn zone_id(&mut self, zone_id: &str) -> &mut Session {
        self.zone_id = zone_id.to_string();
        self
    }

    pub fn fetch_size(&mut self, fetch_size: i32) -> &mut Session {
        self.fetch_size = fetch_size;
        self
    }

    pub fn protocol_version(&mut self, user: &str) -> &mut Session {
        self.user = user.to_string();
        self
    }

    pub fn config(&mut self, key: &str, value: &str) -> &mut Session {
        self.config
            .clone()
            .insert(key.to_string(), value.to_string());
        self
    }

    pub fn config_map(&mut self, map: HashMap<&str, &str>) -> &mut Session {
        for key in map.keys() {
            self.config
                .clone()
                .insert(key.to_string(), map.get(key).unwrap().to_string());
        }
        self
    }

    // Open Session
    pub fn open(&mut self) -> Result<&mut Session, Error> {
        trace!("Open session");
        let open_req = TSOpenSessionReq::new(
            self.protocol_version.clone(),
            self.zone_id.to_string(),
            self.user.clone(),
            self.password.clone(),
            self.config.clone(),
        );

        match self.client.open_session(open_req.clone()) {
            Ok(resp) => {
                let status = resp.status;
                if self.is_success(&status) {
                    if self.protocol_version != resp.server_protocol_version {
                        let msg = format!(
                            "Protocol version is different, client is {:?},server is {:?}",
                            self.protocol_version, resp.server_protocol_version
                        );
                        error!("{}", msg.clone());
                        Err(thrift::new_protocol_error(
                            ProtocolErrorKind::BadVersion,
                            msg,
                        ))
                    } else {
                        self.session_id = resp.session_id.unwrap();
                        self.statement_id = self.client.request_statement_id(self.session_id)?;
                        self.is_close = false;
                        debug!("Session opened");
                        Ok(self)
                    }
                } else {
                    error!("{}", status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    pub fn is_open(&self) -> bool {
        !self.is_close.clone()
    }

    // Close Session
    pub fn close(&mut self) -> Result<(), Error> {
        trace!("Close session");
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
                        error!("{}", status.message.clone().unwrap());
                        Err(thrift::new_application_error(
                            ApplicationErrorKind::MissingResult,
                            status.message.unwrap(),
                        ))
                    }
                }
                Err(error) => Err(thrift::new_transport_error(
                    TransportErrorKind::Unknown,
                    error.to_string(),
                )),
            }
        }
    }

    /// Set a storage group
    pub fn set_storage_group(&mut self, storage_group: &str) -> Result<(), Error> {
        trace!("Set storage group");
        match self
            .client
            .set_storage_group(self.session_id, storage_group.to_string())
        {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "setting storage group {:?} message: {:?}",
                        storage_group,
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Delete a storage group.
    pub fn delete_storage_group(&mut self, storage_group: &str) -> Result<(), Error> {
        trace!("Delete a storage group");
        self.delete_storage_groups(vec![storage_group.to_string()])
    }

    /// Delete storage groups.
    pub fn delete_storage_groups(&mut self, storage_groups: Vec<String>) -> Result<(), Error> {
        trace!("Delete storage groups");
        match self
            .client
            .delete_storage_groups(self.session_id, storage_groups.clone())
        {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "delete storage group(s) {:?} message: {:?}",
                        storage_groups.clone(),
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Create single time-series
    pub fn create_time_series(
        &mut self,
        ts_path: &str,
        data_type: TSDataType,
        encoding: TSEncoding,
        compressor: Compressor,
    ) -> Result<(), Error> {
        trace!("Create single time series");
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
        match self.client.create_timeseries(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "creating time series {:?} message: {:?}",
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Create multiple time-series
    pub fn create_multi_time_series(
        &mut self,
        ts_path_vec: Vec<String>,
        data_type_vec: Vec<i32>,
        encoding_vec: Vec<i32>,
        compressor_vec: Vec<i32>,
    ) -> Result<(), Error> {
        trace!("Create multiple time-series");
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
                        "creating multiple time series {:?} message: {:?}",
                        ts_path_vec.clone(),
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Delete multiple time series
    pub fn delete_time_series(&mut self, path_vec: Vec<String>) -> Result<(), Error> {
        trace!("Delete multiple time-series");
        match self
            .client
            .delete_timeseries(self.session_id, path_vec.clone())
        {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "deleting multiple time series {:?} message: {:?}",
                        path_vec.clone(),
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Check whether a specific time-series exists
    pub fn check_time_series_exists(&mut self, path: &str) -> bool {
        trace!("Check time series exists");

        let statement = format!("SHOW TIMESERIES {}", path);
        let req = TSExecuteStatementReq::new(
            self.session_id,
            statement,
            self.statement_id,
            self.fetch_size,
        );

        match self.client.execute_query_statement(req) {
            Ok(resp) => match resp.query_data_set {
                None => false,
                Some(data_set) => {
                    if data_set.value_list.is_empty() {
                        false
                    } else {
                        true
                    }
                }
            },
            Err(_) => unimplemented!(),
        }
    }

    /// Delete all data <= time in multiple time-series
    pub fn delete_data(&mut self, path_vec: Vec<String>, timestamp: i64) -> Result<(), Error> {
        trace!("Delete data");
        let req = TSDeleteDataReq::new(self.session_id, path_vec.clone(), 0, timestamp);
        match self.client.delete_data(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "delete data from {:?}, message: {:?}",
                        path_vec.clone(),
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// special case for inserting one row of String (TEXT) value
    pub fn insert_string_records(
        &mut self,
        device_ids: Vec<String>,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<String>>,
    ) -> Result<(), Error> {
        let req = TSInsertStringRecordsReq::new(
            self.session_id,
            device_ids.clone(),
            measurements_list,
            values_list,
            timestamps,
        );
        match self.client.insert_string_records(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "insert string records to device {:?} message: {:?}",
                        device_ids.clone(),
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Insert record
    pub fn insert_record(
        &mut self,
        device_id: String,
        timestamp: i64,
        measurements: Vec<String>,
        values: Vec<u8>,
    ) -> Result<(), Error> {
        let req = TSInsertRecordReq::new(
            self.session_id,
            device_id.clone(),
            measurements,
            values,
            timestamp,
        );
        match self.client.insert_record(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "insert one record to device {:?} message: {:?}",
                        device_id.clone(),
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// this method NOT insert data into database and the server just return after accept the
    /// request, this method should be used to test other time cost in client
    pub fn test_insert_record(
        &mut self,
        device_id: String,
        timestamp: i64,
        measurements: Vec<String>,
        values: Vec<u8>,
    ) -> Result<(), Error> {
        let req = TSInsertRecordReq::new(
            self.session_id,
            device_id.clone(),
            measurements,
            values,
            timestamp,
        );
        match self.client.test_insert_record(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "testing! insert one record to device {:?} message: {:?}",
                        device_id.clone(),
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Insert records
    pub fn insert_records(
        &mut self,
        device_ids: Vec<String>,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<u8>>,
    ) -> Result<(), Error> {
        let req = TSInsertRecordsReq::new(
            self.session_id,
            device_ids.clone(),
            measurements_list,
            values_list,
            timestamps,
        );
        match self.client.insert_records(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "insert multiple records to devices {:?} message: {:?}",
                        device_ids.clone(),
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// this method NOT insert data into database and the server just return after accept the
    /// request, this method should be used to test other time cost in client
    pub fn test_insert_records(
        &mut self,
        device_ids: Vec<String>,
        timestamps: Vec<i64>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<u8>>,
    ) -> Result<(), Error> {
        let req = TSInsertRecordsReq::new(
            self.session_id,
            device_ids,
            measurements_list,
            values_list,
            timestamps,
        );
        match self.client.test_insert_records(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    debug!(
                        "testing! insert multiple records, message: {:?}",
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
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

    // TODO
    pub fn insert_tablet(
        &mut self,
        device_id: String,
        measurements: Vec<String>,
        values: Vec<u8>,
        timestamps: Vec<u8>,
        types: Vec<i32>,
        size: i32,
    ) -> Result<TSStatus, Error> {
        trace!("Delete data");
        let req = TSInsertTabletReq::new(
            self.session_id,
            device_id,
            measurements,
            values,
            timestamps,
            types,
            size,
        );
        self.client.insert_tablet(req)
    }

    /// insert multiple tablets, tablets are independent to each other
    /// TODO
    pub fn insert_tablets(
        &mut self,
        device_ids: Vec<String>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<u8>>,
        timestamps_list: Vec<Vec<u8>>,
        types_list: Vec<Vec<i32>>,
        size_list: Vec<i32>,
    ) -> thrift::Result<TSStatus> {
        let req = TSInsertTabletsReq::new(
            self.session_id,
            device_ids,
            measurements_list,
            values_list,
            timestamps_list,
            types_list,
            size_list,
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

    pub fn exec_insert(&mut self, statement: &str) -> DataSet {
        self.exec(statement)
    }

    /// execute query sql statement and return a DataSet
    pub fn exec_query(&mut self, query: &str) -> DataSet {
        debug!("Exec query statement \"{}\"", &query);
        let req = TSExecuteStatementReq::new(
            self.session_id,
            query.to_string(),
            self.statement_id,
            self.fetch_size,
        );

        let mut dataset: DataSet = DataSet::default();
        match self.client.execute_query_statement(req) {
            Ok(resp) => {
                if resp.status.code == 200 {
                    dataset =
                        DataSet::new(query.to_string(), self.session_id, self.fetch_size, resp)
                } else {
                    error!("{}", resp.status.message.clone().unwrap());
                }
            }
            Err(error) => error!("{}", error),
        }

        dataset
    }

    /// execute query sql statement and return a DataSet
    fn exec(&mut self, statement: &str) -> DataSet {
        debug!("Exec statement \"{}\"", &statement);
        let req = TSExecuteStatementReq::new(
            self.session_id,
            statement.to_string(),
            self.session_id,
            self.fetch_size,
        );

        let mut dataset: DataSet = DataSet::default();
        match self.client.execute_statement(req) {
            Ok(resp) => {
                if self.is_success(&resp.status) {
                    dataset = DataSet::new(
                        statement.to_string(),
                        self.session_id,
                        self.fetch_size,
                        resp,
                    )
                } else {
                    error!("{}", resp.status.message.clone().unwrap());
                }
            }
            Err(errors) => error!("{}", errors),
        }
        dataset
    }

    /// execute batch statement and return a DataSets
    pub fn exec_batch(&mut self, statements: Vec<String>) {
        let req = TSExecuteBatchStatementReq::new(self.session_id, statements);
        match self.client.execute_batch_statement(req) {
            Ok(status) => {
                if self.is_success(&status) {
                } else {
                    error!("{}", status.message.clone().unwrap());
                }
            }
            Err(errors) => error!("{}", errors),
        }
    }

    /// execute update statement and return a DataSet
    pub fn exec_update(&mut self, statement: &str) -> DataSet {
        let req = TSExecuteStatementReq::new(
            self.session_id,
            statement.to_string(),
            self.statement_id,
            self.fetch_size,
        );

        let mut dataset: DataSet = DataSet::default();
        match self.client.execute_update_statement(req) {
            Ok(resp) => {
                if self.is_success(&resp.status) {
                    dataset = DataSet::new(
                        statement.to_string(),
                        self.session_id,
                        self.fetch_size,
                        resp,
                    );
                } else {
                    error!("{}", resp.status.message.clone().unwrap());
                }
            }
            Err(errors) => error!("{}", errors),
        }

        dataset
    }

    /// execute row statement and return a DataSets
    pub fn exec_raw_data_query(
        &mut self,
        paths: Vec<String>,
        start_time: i64,
        end_time: i64,
    ) -> DataSet {
        let req = TSRawDataQueryReq::new(
            self.session_id,
            paths,
            self.fetch_size,
            start_time,
            end_time,
            self.statement_id,
        );

        let mut dataset: DataSet = DataSet::default();
        match self.client.execute_raw_data_query(req) {
            Ok(resp) => {
                if self.is_success(&resp.status) {
                    dataset = DataSet::new("".to_string(), self.session_id, self.fetch_size, resp);
                } else {
                    error!("{}", resp.status.message.clone().unwrap());
                }
            }
            Err(errors) => error!("{}", errors),
        }

        dataset
    }

    /// TODO
    fn value_to_bytes() {}

    /// Set time zone
    pub fn set_time_zone(&mut self, time_zone: &str) -> Result<(), Error> {
        trace!("Set time zone");
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
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Get time zone
    pub fn get_time_zone(&mut self) -> Result<String, Error> {
        trace!("Get time zone");
        match self.client.get_time_zone(self.session_id.clone()) {
            Ok(resp) => {
                if resp.status.code == 200 {
                    Ok(resp.time_zone)
                } else {
                    error!("{}", resp.status.message.unwrap());
                    Ok(String::new())
                }
            }
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// Verify success status of operation
    fn is_success(&self, status: &TSStatus) -> bool {
        if status.code == SUCCESS_CODE {
            true
        } else {
            false
        }
    }

    /// TODO
    fn check_sorted(timestamps: Vec<i64>) {}

    /// Cancel operation
    fn cancel_operation(&mut self, query_id: i64) -> Result<(), Error> {
        let req = TSCancelOperationReq::new(self.session_id, query_id);
        match self.client.cancel_operation(req) {
            Ok(status) => {
                if self.is_success(&status) {
                    Ok(())
                } else {
                    let msg = format!("Cancel operation failed,'{:?}'", query_id);
                    error!("{}", msg.clone());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        msg,
                    ))
                }
            }
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }
}
