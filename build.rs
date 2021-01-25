use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_file = "thrift/rpc.thrift";
    let out_dir = "src";

    match Command::new("thrift")
        .args(&["-out", out_dir, "--gen", "rs", input_file])
        .output()
    {
        Ok(_) => {
            println!("Gen to {:?}", out_dir);
        }
        Err(error) => {
            println!("Thrift is not installed \n{:?}", error);
        }
    };
    Ok(())
}
