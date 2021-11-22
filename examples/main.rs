use chrono::Local;
use thrift::Error;

use iotdb::common::{Compressor, DataType, Encoding};
use iotdb::{Config, Session};

fn main() -> Result<(), Error> {
    let config = Config::new()
        .endpoint("127.0.0.1", "6667")
        .user("root")
        .password("root")
        .zone_id("UTC+8")
        // .debug(true)
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

    session.sql(
        format!(
            "INSERT INTO root.ln.wf01.wt01(timestamp,status) values({},true)",
            Local::now().timestamp()
        )
        .as_str(),
    )?;
    session.sql(
        format!(
            "INSERT INTO root.ln.wf01.wt01(timestamp,status) values({},false)",
            Local::now().timestamp()
        )
        .as_str(),
    )?;
    session.sql(
        format!(
            "INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values({},false,18.36)",
            Local::now().timestamp()
        )
        .as_str(),
    )?;
    session.sql(
        format!(
            "INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values({},true,32.23)",
            Local::now().timestamp()
        )
        .as_str(),
    )?;
    session.sql("select * from root.ln")?.show();

    session.close()?;

    Ok(())
}
