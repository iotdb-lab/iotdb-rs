<div align="center">

![Logo](http://iotdb.apache.org/img/logo.png)

<h1>iotdb-rs</h1>
<h3>(WIP) Rust client for Apache IotDB</h3>

[![Crates.io](https://img.shields.io/crates/v/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
[![Api Docs](https://img.shields.io/badge/Api-Doc-a94064?style=flat-square&color=%23E5531A)](https://docs.rs/crate/iotdb)
[![Crates.io](https://img.shields.io/crates/d/iotdb?style=flat-square&color=%23E5531A)](https://crates.io/crates/iotdb)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square&color=%23E5531A)](https://github.com/francis-du/iotdb-rs/blob/master/LICENSE)
[![Rust Build](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-test?label=build&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-test)
[![Crates Publish](https://img.shields.io/github/workflow/status/francis-du/iotdb-rs/cargo-publish?label=publish&style=flat-square)](https://github.com/francis-du/iotdb-rs/actions?query=workflow%3Acargo-publish)
</div>

---

## How to use 

Add `iotdb` to your `Cargo.toml`

```toml
[dependencies]
iotdb = "0.0.2"
```

## Example

```rust
use thrift::Error;

use iotdb::client::Client;
use iotdb::pretty;
use iotdb::session::Session;
use std::collections::HashMap;

fn main() -> Result<(), Error> {
    // create client 4 ways
    // let client = Client::new("localhost", "6667").create();
    // let client = Client::new("localhost", "6667").enable_rpc_compaction().create();
    // let client = Client::default().enable_rpc_compaction().create()?;
    let client = Client::default().create()?;

    // open a session
    let mut session = Session::new(client);

    // config session
    let mut config_map = HashMap::new();
    config_map.insert("", "");

    // session
    //     .user("root")
    //     .password("root")
    //     .fetch_size(2048)
    //     .zone_id("UTC+8")
    //     .config("", "")
    //     .config_map(config_map)
    //     .open()?;

    // using default config
    session.open()?;

    let res = session.query("SHOW TIMESERIES root")?;
    println!("{:#?}", res);
    pretty::result_set(res);

    session.close()?;

    Ok(())
}

```