use thrift::Error;

use iotdb::util::{Compressor, TSDataType, TSEncoding};
use iotdb::Client;
use iotdb::Session;

fn main() -> Result<(), Error> {
    let client = Client::new("localhost", "6667")
        // .enable_rpc_compaction()
        .create()?;

    // open session
    let mut session = Session::new(client);

    session
        .user("root")
        .password("root")
        .zone_id("UTC+8")
        .open()?;

    let storage_group = "root.ln";
    session.delete_storage_group(storage_group)?;
    session.set_storage_group(storage_group)?;

    session.create_time_series(
        "root.ln.wf01.wt01.temperature",
        TSDataType::FLOAT,
        TSEncoding::RLE,
        Compressor::default(),
    )?;

    session.create_time_series(
        "root.ln.wf01.wt01.humidity",
        TSDataType::FLOAT,
        TSEncoding::RLE,
        Compressor::default(),
    )?;

    session.exec_insert("insert into root.ln.wf01.wt01(temperature, humidity) values (36,20)");
    session.exec_insert("insert into root.ln.wf01.wt01(temperature, humidity) values (37,26)");
    session.exec_insert("insert into root.ln.wf01.wt01(temperature, humidity) values (29,16)");

    session.exec_query("SHOW STORAGE GROUP").show();

    if session.check_time_series_exists("root.ln") {
        session.exec_query("SHOW TIMESERIES root.ln").show();
        session.exec_query("select * from root.ln").show();
    }

    session
        .exec_update("delete from root.ln.wf01.wt01.temperature where time<=2017-11-01T16:26:00")
        .show();

    session.close()?;

    Ok(())
}
