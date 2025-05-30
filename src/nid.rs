use std::io::Write;
use std::{fs::File, path::PathBuf};

use clap::{Args, ValueEnum, ValueHint};

use crate::cliargs::CliAction;

#[derive(Args)]
pub struct CliArgs {
    #[arg(short, long, action)]
    url: bool,
    #[arg(short, long, value_hint=ValueHint::FilePath, default_value="nid-dams.gpkg")]
    output_file: PathBuf,
}

impl CliAction for CliArgs {
    fn run(self) -> anyhow::Result<()> {
        let nid_url = "https://nid.sec.usace.army.mil/api/nation/gpkg";
        if self.url {
            println!("{nid_url}");
        } else {
            let resp = reqwest::blocking::get(nid_url).unwrap();
            if !resp.status().is_success() {
                return Err(anyhow::Error::msg(format!("HTTP Error: {}", resp.status())));
            }
            if let Some(_size) = resp.content_length() {
                if self.output_file.exists() {
                    // check for file size to not re-download it
                }
            }
            let mut file = File::create(self.output_file).unwrap();
            // TODO, make it stream (async?)
            file.write_all(&resp.bytes()?)?;
        }
        Ok(())
    }
}
