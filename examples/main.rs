use thrift::Error;

use iotdb::pretty;
use iotdb::utils::TSEncoding;
use iotdb::utils::{Compressor, Field, TSDataType};
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

    session.set_storage_group("root.ln")?;
    session.create_time_series(
        "root.ln.wf01.wt01.status",
        TSDataType::BOOLEAN,
        TSEncoding::default(),
        Compressor::default(),
    )?;

    session.create_time_series(
        "root.ln.wf01.wt01.temperature",
        TSDataType::FLOAT,
        TSEncoding::RLE,
        Compressor::default(),
    )?;

    let res1 = session.query("SHOW STORAGE GROUP")?;
    println!("{:#?}", res1);
    pretty::result_set(res1);

    let res2 = session.query("SHOW TIMESERIES")?;
    println!("{:#?}", res2);
    pretty::result_set(res2);

    let res3 = session.query("SHOW TIMESERIES root.ln.wf01.wt01.status")?;
    println!("{:#?}", res3);
    pretty::result_set(res3);

    println!("{:?}", session.check_time_series_exists("root"));

    session.close()?;

    Ok(())
}
