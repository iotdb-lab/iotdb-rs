use std::ffi::OsString;
use std::fs;
use std::fs::ReadDir;
use std::io::Error;
use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = "src";
    let input_dir = "thrift";

    for item in (fs::read_dir(input_dir))? {
        let file = match item {
            Ok(f) => f,
            Err(e) => {
                println!("Error: {}", e);
                return Ok(());
            }
        };

        match file.file_name().to_str() {
            Some(input_file) => {
                let gen_file = format!("{}/{}", input_dir, input_file);
                gen(out_dir, gen_file.as_str());
            }
            None => {}
        }
    }

    Ok(())
}

fn gen(out_dir: &str, gen_file: &str) {
    match Command::new("thrift")
        .args(&["-out", out_dir, "--gen", "rs", gen_file])
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
