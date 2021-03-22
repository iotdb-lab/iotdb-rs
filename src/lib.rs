//! ![Logo](http://iotdb.apache.org/img/logo.png)
//!
//! [![Crates.io](https://img.shields.io/crates/v/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
//! [![Api Docs](https://img.shields.io/badge/Api-Doc-a94064?style=flat-square&color=%23E5531A)](https://docs.rs/iotdb)
//! [![Crates.io](https://img.shields.io/crates/d/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
//! [![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square&color=%23E5531A)](https://github.com/francis-du/iotdb-rs/blob/main/LICENSE)
//! [![Rust Build](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-test?label=build&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-test)
//! [![Crates Publish](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-publish?label=publish&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-publish)
//!
//! (WIP) A Rust client for Apache IotDB
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
//! use iotdb::pretty;
//! use iotdb::Client;
//! use iotdb::Session;
//!
//! fn main() -> Result<(), Error> {
//!     // let client = Client::new("localhost", "6667").enable_rpc_compaction().create()?;
//!     let client = Client::new("localhost", "6667").create()?;
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
//!     let res = session.query("SHOW TIMESERIES root")?;
//!     // println!("{:#?}", res);
//!     pretty::result_set(res);
//!
//!     session.close()?;
//!
//!     Ok(())
//! }
//!
//! ```

pub mod iotdb;
pub mod pretty;
pub mod rpc;

use crate::rpc::{
    ServerProperties, TSCancelOperationReq, TSCloseSessionReq, TSCreateMultiTimeseriesReq,
    TSCreateTimeseriesReq, TSDeleteDataReq, TSExecuteStatementReq, TSExecuteStatementResp,
    TSIServiceSyncClient, TSInsertRecordReq, TSInsertRecordsOfOneDeviceReq, TSInsertRecordsReq,
    TSInsertStringRecordsReq, TSInsertTabletReq, TSInsertTabletsReq, TSOpenSessionReq,
    TSProtocolVersion, TSQueryDataSet, TSSetTimeZoneReq, TSStatus, TTSIServiceSyncClient,
};
use chrono::{Local, Utc};
use log::{debug, error, trace};
use std::collections::{BTreeMap, HashMap};
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
    TInputProtocol, TOutputProtocol,
};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel};
use thrift::{ApplicationErrorKind, Error, ProtocolErrorKind, TransportErrorKind};

type ClientType = TSIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>;

const SUCCESS_CODE: i32 = 200;

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
        let (i_chan, o_chan) = channel.split()?;

        let i_tran = TFramedReadTransport::new(i_chan);
        let o_tran = TFramedWriteTransport::new(o_chan);

        let (i_prot, o_prot): (Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>);
        if self.rpc_compaction {
            i_prot = Box::new(TCompactInputProtocol::new(i_tran));
            o_prot = Box::new(TCompactOutputProtocol::new(o_tran));
            debug!("Create a compaction client");
        } else {
            i_prot = Box::new(TBinaryInputProtocol::new(i_tran, true));
            o_prot = Box::new(TBinaryOutputProtocol::new(o_tran, true));
            debug!("Create a binary client");
        }
        Ok(TSIServiceSyncClient::new(i_prot, o_prot))
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
    pub fn open(&mut self) -> thrift::Result<&mut Session> {
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
    pub fn close(&mut self) -> thrift::Result<()> {
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
    pub fn set_storage_group(&mut self, storage_group: &str) -> thrift::Result<()> {
        trace!("Set storage group");
        match self
            .client
            .set_storage_group(self.session_id, storage_group.to_string())
        {
            Ok(status) => {
                if self.is_success(&status) {
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
    pub fn delete_storage_group(&mut self, storage_group: &str) -> thrift::Result<()> {
        trace!("Delete a storage group");
        match self
            .client
            .delete_storage_groups(self.session_id, vec![storage_group.to_string()])
        {
            Ok(status) => {
                if self.is_success(&status) {
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

    /// Delete storage groups.
    pub fn delete_storage_groups(&mut self, storage_groups: Vec<String>) -> thrift::Result<()> {
        trace!("Delete storage groups");
        match self
            .client
            .delete_storage_groups(self.session_id, storage_groups)
        {
            Ok(status) => {
                if self.is_success(&status) {
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
        ts_path: String,
        data_type: i32,
        encoding: i32,
        compressor: i32,
    ) -> thrift::Result<()> {
        trace!("Create single time-series");
        let req = TSCreateTimeseriesReq::new(
            self.session_id,
            ts_path,
            data_type,
            encoding,
            compressor,
            None,
            None,
            None,
            None,
        );
        match self.client.create_timeseries(req) {
            Ok(status) => {
                if self.is_success(&status) {
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
    ) -> thrift::Result<()> {
        trace!("Create multiple time-series");
        let req = TSCreateMultiTimeseriesReq::new(
            self.session_id,
            ts_path_vec,
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
    pub fn delete_time_series(&mut self, path_vec: Vec<String>) -> thrift::Result<()> {
        trace!("Delete multiple time-series");
        match self.client.delete_timeseries(self.session_id, path_vec) {
            Ok(status) => {
                if self.is_success(&status) {
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
                    self.cancel_operation(resp.query_id.unwrap());

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
    pub fn delete_data(&mut self, path_vec: Vec<String>, timestamp: i64) -> thrift::Result<()> {
        trace!("Delete data");
        let req = TSDeleteDataReq::new(self.session_id, path_vec, 0, timestamp);
        match self.client.delete_data(req) {
            Ok(status) => {
                if self.is_success(&status) {
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
    /// TODO
    pub fn insert_string_records(
        &mut self,
        device_ids: Vec<String>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<String>>,
        timestamps: Vec<i64>,
    ) -> thrift::Result<TSStatus> {
        let req = TSInsertStringRecordsReq::new(
            self.session_id,
            device_ids,
            measurements_list,
            values_list,
            timestamps,
        );
        self.client.insert_string_records(req)
    }

    /// Insert record
    // TODO
    pub fn insert_record(
        &mut self,
        device_id: String,
        measurements: Vec<String>,
        values: Vec<u8>,
        timestamp: i64,
    ) -> thrift::Result<TSStatus> {
        let req =
            TSInsertRecordReq::new(self.session_id, device_id, measurements, values, timestamp);
        self.client.insert_record(req)
    }

    /// Insert records
    // TODO
    pub fn insert_records(
        &mut self,
        device_ids: Vec<String>,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<u8>>,
        timestamps: Vec<i64>,
    ) -> thrift::Result<TSStatus> {
        let req = TSInsertRecordsReq::new(
            self.session_id,
            device_ids,
            measurements_list,
            values_list,
            timestamps,
        );
        self.client.insert_records(req)
    }

    /// this method NOT insert data into database and the server just return after accept the
    /// request, this method should be used to test other time cost in client
    /// TODO
    pub fn test_insert_record() {}

    /// this method NOT insert data into database and the server just return after accept the
    /// request, this method should be used to test other time cost in client
    /// TODO
    pub fn test_insert_records() {}

    /// TODO
    pub fn gen_insert_record_req() {}

    /// TODO
    pub fn gen_insert_records_req() {}

    /// insert one tablet, in a tablet, for each timestamp, the number of measurements is same
    ///     for example three records in the same device can form a tablet:
    ///         timestamps,     m1,    m2,     m3
    ///                  1,  125.3,  True,  text1
    ///                  2,  111.6, False,  text2
    ///                  3,  688.6,  True,  text3
    /// Notice: The tablet should not have empty cell
    ///         The tablet itself is sorted
    /// TODO
    pub fn insert_tablet(
        &mut self,
        device_id: String,
        measurements: Vec<String>,
        values: Vec<u8>,
        timestamps: Vec<u8>,
        types: Vec<i32>,
        size: i32,
    ) -> thrift::Result<TSStatus> {
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

    /// execute query sql statement and returns SessionDataSet
    /// TODO
    pub fn execute_query_statement() {}

    /// execute non-query sql statement
    /// TODO
    pub fn execute_non_query_statement() {}

    /// Exec Query
    /// TODO: need to delete
    pub fn query(&mut self, sql: &str) -> thrift::Result<TSExecuteStatementResp> {
        debug!("Exec query \"{}\"", &sql);
        let req = TSExecuteStatementReq::new(
            self.session_id,
            sql.to_string(),
            self.statement_id,
            self.fetch_size,
        );
        match self.client.execute_query_statement(req) {
            Ok(resp) => {
                if resp.status.code == 200 {
                    Ok(resp)
                } else {
                    error!("{}", resp.status.message.clone().unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::MissingResult,
                        resp.status.message.unwrap(),
                    ))
                }
            }
            Err(error) => Err(thrift::new_transport_error(
                TransportErrorKind::Unknown,
                error.to_string(),
            )),
        }
    }

    /// TODO
    fn value_to_bytes() {}

    /// Set time zone
    pub fn set_time_zone(&mut self, time_zone: &str) -> thrift::Result<()> {
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
    pub fn get_time_zone(&mut self) -> thrift::Result<String> {
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
            Err(_) => Ok(String::new()),
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
    pub fn cancel_operation(&mut self, query_id: i64) -> thrift::Result<()> {
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
