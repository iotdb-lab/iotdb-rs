use thrift::Error;

use iotdb::utils::{TSEncoding, Pretty};
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

    let storage_group = "root.ln";
    session.delete_storage_group(storage_group)?;
    session.set_storage_group(storage_group)?;

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
    Pretty::new(res1).show();

    let res2 = session.query("SHOW TIMESERIES")?;
    println!("{:#?}", res2);
    Pretty::new(res2).show();

    let res3 = session.query("SHOW TIMESERIES root.ln.wf01.wt01.status")?;
    println!("{:#?}", res3);
    Pretty::new(res3).show();

    println!("{:?}", session.check_time_series_exists("root"));

    session.close()?;

    Ok(())
}
