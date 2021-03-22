<div align="center">

![Logo](http://iotdb.apache.org/img/logo.png)

<h1>iotdb-rs</h1>
<h3>(WIP) Rust client for Apache IotDB</h3>

[![Crates.io](https://img.shields.io/crates/v/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
[![Api Docs](https://img.shields.io/badge/Api-Doc-a94064?style=flat-square&color=%23E5531A)](https://docs.rs/iotdb)
[![Crates.io](https://img.shields.io/crates/d/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square&color=%23E5531A)](https://github.com/francis-du/iotdb-rs/blob/main/LICENSE)
[![Rust Build](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-test?label=build&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-test)
[![Crates Publish](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-publish?label=publish&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-publish)
</div>

---

## Overview

IoTDB (Internet of Things Database) is a data management system for time series data, which can provide users specific
services, such as, data collection, storage and analysis. Due to its light weight structure, high performance and usable
features together with its seamless integration with the Hadoop and Spark ecology, IoTDB meets the requirements of
massive dataset storage, high throughput data input, and complex data analysis in the industrial IoT field.

## How to use

Add `iotdb` to your `Cargo.toml`

```toml
[dependencies]
iotdb = "0.0.3"
```

## Example

```rust
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

```

## Run example

1. Download the package from [here](https://archive.apache.org/dist/iotdb)

```shell
curl -O https://archive.apache.org/dist/iotdb/0.11.2/apache-iotdb-0.11.2-bin.zip
```

2. Install and start iotdb server

```shell
$IOTDB_HOME/sbin/start-server -c conf -rpc_port 6667
```

3. Install rust toolchain form [here](https://www.rust-lang.org/tools/install)

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

4.Run example

```shell
git clone https://github.com/francis-du/iotdb-rs.git

cargo run --example main
```

## LICENSE

[MIT LICENSE](LICENSE)