use std::collections::{BTreeMap, HashMap};

use chrono::{Local, Utc};
use log::{debug, error, info, trace, warn};
use thrift::protocol::{TInputProtocol, TOutputProtocol};

use crate::rpc::{
    ServerProperties, TSCloseSessionReq, TSDeleteDataReq, TSExecuteStatementReq,
    TSExecuteStatementResp, TSGetTimeZoneResp, TSIServiceSyncClient, TSInsertRecordReq,
    TSOpenSessionReq, TSProtocolVersion, TSSetTimeZoneReq, TSStatus, TTSIServiceSyncClient,
};
use thrift::{ApplicationError, ApplicationErrorKind, Error};

type ClientType = TSIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>;

pub struct Session {
    user: String,
    password: String,
    zone_id: String,
    fetch_size: i32,
    session_id: i64,
    statement_id: i64,
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
        trace!("open session");
        let open_req = TSOpenSessionReq::new(
            self.protocol_version.clone(),
            self.zone_id.to_string(),
            self.user.clone(),
            self.password.clone(),
            self.config.clone(),
        );

        match self.client.open_session(open_req.clone()) {
            Ok(resp) => {
                if resp.status.code == 200 {
                    self.session_id = resp.session_id.unwrap();
                    self.statement_id = self.client.request_statement_id(self.session_id)?;
                    debug!("session opened")
                } else {
                    error!("{}", resp.status.message.unwrap());
                }
            }
            Err(error) => {
                error!("Failed to open a session\n{}", error)
            }
        };

        Ok(self)
    }

    // Close Session
    pub fn close(&mut self) -> thrift::Result<()> {
        trace!("close session");
        let req = TSCloseSessionReq::new(self.session_id);
        match self.client.close_session(req) {
            Ok(status) => {
                if status.code == 200 {
                    debug!("session closed")
                } else {
                    error!("{}", status.message.unwrap())
                }
            }
            Err(error) => error!("{}", error),
        }
        Ok(())
    }

    // Exec Query
    pub fn query(&mut self, sql: &str) -> thrift::Result<TSExecuteStatementResp> {
        trace!("exec query");
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
                    error!("{}", resp.status.message.unwrap());
                    Err(thrift::new_application_error(
                        ApplicationErrorKind::Unknown,
                        "",
                    ))
                }
            }
            Err(error) => {
                error!("{}", error);
                Err(error)
            }
        }
    }

    // Get Request
    pub fn get_time_zone(&mut self) -> thrift::Result<TSGetTimeZoneResp> {
        Ok(self.client.get_time_zone(self.session_id.clone())?)
    }

    pub fn get_properties(&mut self) -> thrift::Result<ServerProperties> {
        Ok(self.client.get_properties()?)
    }

    // Set Request
    pub fn set_storage_group(&mut self, storage_group: String) -> thrift::Result<TSStatus> {
        Ok(self
            .client
            .set_storage_group(self.session_id, storage_group)?)
    }

    pub fn set_time_zone(&mut self, time_zone: String) -> thrift::Result<TSStatus> {
        let req = TSSetTimeZoneReq::new(self.session_id, time_zone);
        Ok(self.client.set_time_zone(req)?)
    }

    // Insert Request
    pub fn insert_record(
        &mut self,
        device_id: String,
        measurements: Vec<String>,
        values: Vec<u8>,
        timestamp: i64,
    ) -> thrift::Result<TSStatus> {
        let req =
            TSInsertRecordReq::new(self.session_id, device_id, measurements, values, timestamp);
        Ok(self.client.insert_record(req)?)
    }

    // Delete Request
    pub fn delete_storage_groups(
        &mut self,
        storage_groups: Vec<String>,
    ) -> thrift::Result<TSStatus> {
        Ok(self
            .client
            .delete_storage_groups(self.session_id, storage_groups)?)
    }

    pub fn delete_data(
        &mut self,
        paths: Vec<String>,
        start_time: i64,
        end_time: i64,
    ) -> thrift::Result<TSStatus> {
        let req = TSDeleteDataReq::new(self.session_id, paths, start_time, end_time);
        Ok(self.client.delete_data(req)?)
    }

    pub fn delete_timeseries(&mut self, path: Vec<String>) -> thrift::Result<TSStatus> {
        Ok(self.client.delete_timeseries(self.session_id, path)?)
    }
}
