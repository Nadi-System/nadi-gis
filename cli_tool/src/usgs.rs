use std::io::Write;
use std::{fs::File, path::PathBuf};

use clap::{Args, ValueEnum, ValueHint};

use crate::cliargs::CliAction;

#[derive(Args)]
pub struct CliArgs {
    /// USGS Site number (separate by ',' for multiple)
    #[arg(short, long, value_delimiter = ',', required = true)]
    site_no: Vec<String>,
    /// Type of data (u/d/t/b)
    ///
    /// [upstream (u), downstream (d), tributories (t), basin (b)]
    #[arg(
        short,
        long,
        rename_all = "lower",
        default_value = "b",
        value_enum,
        hide_possible_values = true
    )]
    data: Vec<GeoInfo>,
    /// Display the url and exit (no download)
    #[arg(short, long, action)]
    url: bool,
    #[arg(short, long, value_hint=ValueHint::DirPath, default_value=".")]
    output_dir: PathBuf,
}

impl CliAction for CliArgs {
    fn run(self) -> anyhow::Result<()> {
        for site in self.site_no {
            for data in &self.data {
                if self.url {
                    println!("{}", data.usgs_url(&site));
                } else {
                    data.download(&site, &self.output_dir);
                }
            }
        }
        Ok(())
    }
}

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum)]
pub enum GeoInfo {
    #[value(alias = "u")]
    Upstream,
    #[value(alias = "d")]
    Downstream,
    #[value(alias = "t")]
    Tributories,
    #[value(alias = "b")]
    Basin,
    #[value(alias = "n")]
    NwisSite,
}

// Available data can be seen from links like this here:
// https://api.water.usgs.gov/nldi/linked-data/nwissite/USGS-03227500/navigation/UT?f=json

impl GeoInfo {
    pub fn usgs_query(&self) -> &str {
        match self {
            Self::Upstream => "navigate/UM?f=json",
            Self::NwisSite => "navigate/UT/nwissite?f=json",
            Self::Downstream => "navigate/DM?f=json",
            Self::Tributories => "navigate/UT?f=json",
            Self::Basin => "basin?f=json",
        }
    }

    pub fn filename(&self, site_no: &str) -> String {
        format!(
            "{site_no}_{}.json",
            match self {
                Self::Upstream => "upstream",
                Self::Downstream => "downstream",
                Self::Tributories => "tributaries",
                Self::Basin => "basin",
                Self::NwisSite => "nwissites",
            }
        )
    }

    pub fn usgs_url(&self, site_no: &str) -> String {
        let query = self.usgs_query();
        format!("https://api.water.usgs.gov/nldi/linked-data/nwissite/USGS-{site_no}/{query}")
    }

    pub fn download(&self, site_no: &str, dir: &PathBuf) {
        let url = self.usgs_url(site_no);
        let bytes = reqwest::blocking::get(url).unwrap().bytes().unwrap();
        if bytes.is_empty() {
            eprintln!("No data");
            return;
        }
        let filepath = dir.join(self.filename(site_no));
        let mut file = File::create(filepath).unwrap();
        file.write_all(&bytes).unwrap();
    }
}
