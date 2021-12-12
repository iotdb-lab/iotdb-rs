<div align="center">

![logo](iotdb-rs.png)

<h1>iotdb-rs</h1>
<h3>Apache IotDB Client written in Rust</h3>

[![Crates.io](https://img.shields.io/crates/v/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
[![Api Docs](https://img.shields.io/badge/Api-Doc-a94064?style=flat-square&color=%23E5531A)](https://docs.rs/iotdb)
[![Crates.io](https://img.shields.io/crates/d/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square&color=%23E5531A)](https://github.com/iotdb-lab/iotdb-rs/blob/main/LICENSE)
[![Rust Build](https://img.shields.io/github/workflow/status/iotdb-lab/iotdb-rs/cargo-test?label=build&style=flat-square)](https://github.com/iotdb-lab/iotdb-rs/actions?query=workflow%3Acargo-test)
[![Crates Publish](https://img.shields.io/github/workflow/status/iotdb-lab/iotdb-rs/cargo-publish?label=publish&style=flat-square)](https://github.com/iotdb-lab/iotdb-rs/actions?query=workflow%3Acargo-publish)

</div>

---

[![Alt](https://repobeats.axiom.co/api/embed/15bcb8c6b0f3a63838c7ca62234867b58ec60b28.svg "Repobeats analytics image")](https://github.com/iotdb-lab/iotdb-rs/pulse)

## Overview

IoTDB (Internet of Things Database) is a data management system for time series data, which can provide users specific
services, such as, data collection, storage and analysis. Due to its light weight structure, high performance and usable
features together with its seamless integration with the Hadoop and Spark ecology, IoTDB meets the requirements of
massive dataset storage, high throughput data input, and complex data analysis in the industrial IoT field.

## How to use

Add `iotdb` to your `Cargo.toml`

```toml
[dependencies]
iotdb = "0.0.6"
```

## Example

```rust
use chrono::Local;
use thrift::Error;

use iotdb::common::{Compressor, DataType, Encoding};
use iotdb::{ConfigBuilder, Session};

fn main() -> Result<(), Error> {
    let config = ConfigBuilder::new()
        .endpoint("127.0.0.1", "6667")
        .user("root")
        .password("root")
        .time_zone("UTC+8")
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

    session.close()?;

    Ok(())
}
```

## Run example

1. Download the package from [here](https://archive.apache.org/dist/iotdb)

```shell
curl -O https://archive.apache.org/dist/iotdb/0.11.2/apache-iotdb-0.11.2-bin.zip
```

2. Install and start iotdb server

```shell
cd $IOTDB_HOME && sbin/start-server -c conf -rpc_port 6667
```

3. Install rust toolchain from [here](https://www.rust-lang.org/tools/install)

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

4.Run example

```shell
git clone https://github.com/francis-du/iotdb-rs.git

cargo run --example main
```

## LICENSE

[Apache License 2.0](LICENSE)