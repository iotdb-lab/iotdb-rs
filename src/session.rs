use std::collections::{BTreeMap, HashMap};

use chrono::{Local, Utc};
use log::{debug, error, info, trace, warn};
use thrift::protocol::{TInputProtocol, TOutputProtocol};

use crate::rpc::{
    ServerProperties, TSCancelOperationReq, TSCloseSessionReq, TSCreateMultiTimeseriesReq,
    TSCreateTimeseriesReq, TSDeleteDataReq, TSExecuteStatementReq, TSExecuteStatementResp,
    TSIServiceSyncClient, TSInsertRecordReq, TSInsertRecordsOfOneDeviceReq, TSInsertRecordsReq,
    TSInsertStringRecordsReq, TSInsertTabletReq, TSInsertTabletsReq, TSOpenSessionReq,
    TSProtocolVersion, TSSetTimeZoneReq, TSStatus, TTSIServiceSyncClient,
};
use thrift::{ApplicationErrorKind, Error, ProtocolErrorKind, TransportErrorKind};

type ClientType = TSIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>;

pub const SUCCESS_CODE: i32 = 200;

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

    // Verify success status of operation
    pub fn is_success(&self, status: &TSStatus) -> bool {
        if status.code == SUCCESS_CODE {
            true
        } else {
            false
        }
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
    // TODO
    pub fn check_time_series_exist(&mut self, path: &str) {
        trace!("Check time-series exists");
        self.query(format!("SHOW TIMESERIES {}", path).as_str());
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

    /// Insert string records
    // TODO
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

    /// Insert records of one device
    // TODO
    pub fn insert_records_of_one_device(
        &mut self,
        device_id: String,
        measurements_list: Vec<Vec<String>>,
        values_list: Vec<Vec<u8>>,
        timestamps: Vec<i64>,
    ) -> thrift::Result<TSStatus> {
        let req = TSInsertRecordsOfOneDeviceReq::new(
            self.session_id,
            device_id,
            measurements_list,
            values_list,
            timestamps,
        );
        Ok(self.client.insert_records_of_one_device(req)?)
    }

    /// Insert tablet
    // TODO
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

    /// Insert tablets
    // TODO
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

    // Exec Query
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

    /// Get properties
    pub fn get_properties(&mut self) -> thrift::Result<ServerProperties> {
        trace!("Get properties");
        Ok(self.client.get_properties()?)
    }

    /// Cancel operation
    //TODO
    pub fn cancel_operation(&mut self, query_id: i64) -> thrift::Result<TSStatus> {
        let req = TSCancelOperationReq::new(self.session_id, query_id);
        self.client.cancel_operation(req)
    }
}
