use std::process::Command;

fn main() {
    fetch_rpc_file("thrift");
    Command::new("sh")
        .args(&[format!(
            "{}/code_gen.sh",
            std::env::var("CARGO_MANIFEST_DIR").unwrap()
        )])
        .output()
        .unwrap();
}

fn fetch_rpc_file(thrift_dir: &str) {
    let urls = [
        ("https://raw.githubusercontent.com/apache/iotdb/master/thrift/src/main/thrift/client.thrift", "client.thrift")
        , ("https://raw.githubusercontent.com/apache/iotdb/master/thrift-commons/src/main/thrift/common.thrift", "common.thrift")
    ];

    for url in urls {
        let out = format!("{}/{}", thrift_dir, url.1);
        match Command::new("curl")
            .args(&["-o", out.as_str(), url.0])
            .output()
        {
            Ok(_) => {
                println!("Get thrift file to {:?}", thrift_dir);
            }
            Err(error) => {
                println!("Curl is not installed \n{:?}", error);
            }
        };
    }
}
