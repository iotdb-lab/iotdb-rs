pub mod pretty;
pub mod rpc;
pub mod session;
pub mod tablet;

pub mod client {
    use log::{debug, trace};

    type ClientType = TSIServiceSyncClient<Box<dyn TInputProtocol>, Box<dyn TOutputProtocol>>;

    use crate::rpc::TSIServiceSyncClient;
    use thrift::protocol::{
        TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
        TInputProtocol, TOutputProtocol,
    };
    use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel};

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
}
