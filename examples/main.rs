use thrift::Error;

use iotdb::common::{Compressor, DataType, Encoding};
use iotdb::{Config, Session};

fn main() -> Result<(), Error> {
    let config = Config::new()
        .endpoint("127.0.0.1", "6667")
        .user("root")
        .password("root")
        .zone_id("UTC+8")
        .debug(true)
        .build();

    // open session
    let mut session = Session::new(config).open()?;
    println!("time_zone: {}", session.time_zone()?);
    session.delete_storage_group("root.ln")?;
    session.set_storage_group("root.ln")?;
    session.create_time_series(
        "root.ln.wf01.wt01.temperature",
        DataType::INT64,
        Encoding::default(),
        Compressor::default(),
    )?;

    session.create_time_series(
        "root.ln.wf01.wt01.humidity",
        DataType::INT64,
        Encoding::default(),
        Compressor::default(),
    )?;

    session.sql("INSERT INTO root.ln.wf01.wt01(timestamp,status) values(100,true)")?;
    session.sql("INSERT INTO root.ln.wf01.wt01(timestamp,status) values(200,false)")?;
    session.sql(
        "INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(300,false,18.36)",
    )?;
    session.sql(
        "INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values(400,true,32.23)",
    )?;
    session.sql("select * from root.ln")?.show();

    session.close()?;

    Ok(())
}
