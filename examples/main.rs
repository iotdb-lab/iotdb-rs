use thrift::Error;

use iotdb::pretty;
use iotdb::Client;
use iotdb::Session;

fn main() -> Result<(), Error> {
    // let client = Client::new("localhost", "6667").enable_rpc_compaction().create()?;
    let client = Client::new("localhost", "6667").create()?;

    // open session
    let mut session = Session::new(client);

    session
        .user("root")
        .password("root")
        .zone_id("UTC+8")
        .open()?;

    let res = session.query("SHOW TIMESERIES root")?;
    // println!("{:#?}", res);
    pretty::result_set(res);

    session.close()?;

    Ok(())
}
