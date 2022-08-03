use chrono::Local;

use iotdb::*;

fn main() -> Result<(), anyhow::Error> {
    debug(false);

    let config = ConfigBuilder::new()
        .endpoint("119.84.128.59:6667")
        .user("root")
        .password("root")
        .time_zone("UTC+8")
        .build();

    // open session
    let mut session = Session::connect(config)?;
    println!(
        "Server version {:#?}",
        session.get_properties()?.version.as_str()
    );
    println!("Time Zone: {}", session.time_zone()?);
    session.delete_storage_group("root.ln")?;
    session.set_storage_group("root.ln")?;
    session.create_time_series(
        "root.ln.wf01.wt01.temperature",
        DataType::FLOAT,
        Encoding::default(),
        Compressor::default(),
    )?;

    session.create_time_series(
        "root.ln.wf01.wt01.status",
        DataType::BOOLEAN,
        Encoding::default(),
        Compressor::default(),
    )?;

    let now = Local::now().timestamp_millis();
    session.sql(
        format!(
            "INSERT INTO root.ln.wf01.wt01(timestamp,status) values({},true)",
            now
        )
        .as_str(),
    )?;
    session.sql(
        format!(
            "INSERT INTO root.ln.wf01.wt01(timestamp,status) values({},false)",
            now + 1000
        )
        .as_str(),
    )?;
    session.sql(
        format!(
            "INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values({},false,18.36)",
            now + 2000
        )
        .as_str(),
    )?;
    session.sql(
        format!(
            "INSERT INTO root.ln.wf01.wt01(timestamp,status,temperature) values({},true,32.23)",
            now + 3000
        )
        .as_str(),
    )?;
    session.sql("select * from root.ln")?.show();

    // DF (TODO)
    let df = session.sql("select * from root.ln")?.to_df()?;
    println!("IoTDB DF is empty: {}", df.is_empty());

    session.close()?;

    Ok(())
}

fn debug(enable: bool) {
    use simplelog::*;
    let mut log_level = LevelFilter::Info;
    if enable {
        log_level = LevelFilter::Debug;
    }
    let _ = CombinedLogger::init(vec![TermLogger::new(
        log_level,
        Default::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )]);
}
