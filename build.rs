use std::fs;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = "src";
    let thrift_dir = "thrift";
    fetch_rpc_file(thrift_dir);

    for item in (fs::read_dir(thrift_dir))? {
        let file = match item {
            Ok(f) => f,
            Err(e) => {
                println!("Error: {}", e);
                return Ok(());
            }
        };

        match file.file_name().to_str() {
            Some(input_file) => {
                let gen_file = format!("{}/{}", file.path().to_str().unwrap(), input_file);
                gen(out_dir, gen_file.as_str());
            }
            None => {}
        }
    }

    Ok(())
}

fn gen(out_dir: &str, thrift_dir: &str) {
    match Command::new("thrift")
        .args(&["-out", out_dir, "--gen", "rs", thrift_dir])
        .output()
    {
        Ok(_) => {
            println!("Gen to {:?}", out_dir);
        }
        Err(error) => {
            println!("Thrift is not installed \n{:?}", error);
        }
    };
}

fn fetch_rpc_file(thrift_dir: &str) {
    let url =
        "https://raw.githubusercontent.com/apache/iotdb/master/thrift/src/main/thrift/rpc.thrift";

    let out = format!("{}/rpc.thrift", thrift_dir);
    match Command::new("curl")
        .args(&["-o", out.as_str(), url])
        .output()
    {
        Ok(_) => {
            println!("Get rpc file to {:?}", thrift_dir);
        }
        Err(error) => {
            println!("Curl is not installed \n{:?}", error);
        }
    };
}
