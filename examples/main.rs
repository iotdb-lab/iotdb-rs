use thrift::Error;

use iotdb::common::{Compressor, DataType, Encoding};
use iotdb::Client;
use iotdb::Session;

fn main() -> Result<(), Error> {
    let client = Client::new("localhost", "6667")
        .log_level("debug")
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
        DataType::INT64,
        Encoding::RLE,
        Compressor::default(),
    )?;

    session.create_time_series(
        "root.ln.wf01.wt01.humidity",
        DataType::INT64,
        Encoding::RLE,
        Compressor::default(),
    )?;

    session.exec_query("SHOW STORAGE GROUP").show();

    if session.check_time_series_exists("root.ln") {
        session.exec_query("SHOW TIMESERIES root.ln").show();
        session.exec_query("select * from root.ln").show();
    }

    session.close()?;

    Ok(())
}
