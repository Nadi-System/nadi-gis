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
        let bytes = reqwest::blocking::get(nid_url).unwrap().bytes().unwrap();
        let mut file = File::create(self.output_file).unwrap();
        file.write_all(&bytes)?;
        Ok(())
    }
}
