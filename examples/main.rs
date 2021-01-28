use thrift::Error;

use iotdb::client::Client;
use iotdb::pretty;
use iotdb::session::Session;
use std::collections::HashMap;

fn main() -> Result<(), Error> {
    // create client 4 ways
    // let client = Client::new("localhost", "6667").create();
    // let client = Client::new("localhost", "6667").enable_rpc_compaction().create();
    // let client = Client::default().enable_rpc_compaction().create()?;
    let client = Client::default().create()?;

    // open a session
    let mut session = Session::new(client);

    // config session
    let mut config_map = HashMap::new();
    config_map.insert("", "");

    // session
    //     .user("root")
    //     .password("root")
    //     .fetch_size(2048)
    //     .zone_id("UTC+8")
    //     .config("", "")
    //     .config_map(config_map)
    //     .open()?;

    // using default config
    session.open()?;

    let res = session.query("SHOW TIMESERIES root")?;
    println!("{:#?}", res);
    pretty::result_set(res);

    session.close()?;

    Ok(())
}
