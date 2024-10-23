use std::path::PathBuf;

use clap::Args;
use gdal::vector::{LayerAccess, OGRFieldType};
use gdal::Dataset;

use crate::cliargs::CliAction;

#[derive(Args)]
pub struct CliArgs {
    /// Show number of features
    #[arg(short, long)]
    features: bool,
    /// Show attribute columns
    #[arg(short, long)]
    attributes: bool,
    /// GIS file with points of interest
    #[arg(value_name = "GIS_FILE")]
    file: PathBuf,
}

impl CliAction for CliArgs {
    fn run(self) -> Result<(), anyhow::Error> {
        let file_data = Dataset::open(&self.file).unwrap();
        for lyr in file_data.layers() {
            println!("{}", lyr.name());
            if self.features {
                println!("  - Features: {}", lyr.feature_count());
            }
            if self.attributes {
                println!("  - Fields:");
                lyr.defn().fields().for_each(|f| {
                    println!(
                        "    + \"{}\" ({})",
                        f.name(),
                        match f.field_type() {
                            OGRFieldType::OFTBinary => "Binary",
                            OGRFieldType::OFTDate => "Date",
                            OGRFieldType::OFTDateTime => "DateTime",
                            OGRFieldType::OFTInteger => "Interger32bit",
                            OGRFieldType::OFTInteger64 => "Integer64bit",
                            OGRFieldType::OFTInteger64List => "List<Integer64bit>",
                            OGRFieldType::OFTIntegerList => "List<Integer32bit>",
                            OGRFieldType::OFTReal => "Double",
                            OGRFieldType::OFTRealList => "List<Double>",
                            OGRFieldType::OFTString => "String",
                            OGRFieldType::OFTStringList => "List<String>",
                            OGRFieldType::OFTTime => "Time",
                            // OGRFieldType::OFTWideString => "deprecated",
                            // OGRFieldType::OFTWideStringList => "deprecated",
                            _ => "unknown",
                        }
                    )
                });
            }
        }
        Ok(())
    }
}
