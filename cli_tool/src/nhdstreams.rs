use std::path::PathBuf;

use clap::Args;
use gdal::vector::{Defn, Feature, FieldDefn, Layer, LayerAccess, LayerOptions, OGRFieldType};
use gdal::Dataset;

use crate::cliargs::CliAction;
use crate::utils::*;

#[derive(Args)]
pub struct CliArgs {
    /// print the progress
    #[arg(short, long, action)]
    verbose: bool,
    /// NHDPlus GIS file
    #[arg(value_name = "NHDPlus_FILE")]
    nhd_file: PathBuf,
    /// Output GIS file
    #[arg(value_name = "OUTPUT_FILE")]
    out_file: PathBuf,
}

impl CliAction for CliArgs {
    fn run(self) -> Result<(), anyhow::Error> {
        let file_data = Dataset::open(&self.nhd_file).unwrap();

        let mut flowlines = file_data.layer_by_name("NetworkNHDFlowline")?;
        let mut out_data = gdal_update_or_create(&self.out_file, &None, true)?;
        let mut trans = false;
        // have to use trans flag here because of borrow rule;
        // uses transaction when it can to speed up the process.
        if let Ok(mut txn) = out_data.start_transaction() {
            write_streams(&mut txn, &mut flowlines, self.verbose)?;
            txn.commit()?;
            trans = true;
        };

        if !trans {
            write_streams(&mut out_data, &mut flowlines, self.verbose)?;
        }

        Ok(())
    }
}

fn write_streams(out_data: &mut Dataset, streams: &mut Layer, verbose: bool) -> anyhow::Result<()> {
    let layer = out_data.create_layer(LayerOptions {
        name: "Streams",
        srs: streams.spatial_ref().as_ref(),
        ty: streams.defn().geometry_type(),
        ..Default::default()
    })?;

    let total = streams.feature_count();
    let mut progress = 0;
    let defn = Defn::from_layer(&layer);
    let fty = streams.defn().field_index("ftype")?;
    // QGIS shows DivergenceCode, but the file has it as
    // divergence, could be GDB's limitation in column name, try
    // the GPKG version
    let dc = streams.defn().field_index("divergence")?;
    for feat in streams.features() {
        if verbose {
            progress += 1;
            print!("\rWriting Features: {}", progress * 100 / total);
        }
        if let Ok(Some(i)) = feat.field_as_integer(dc) {
            if i > 1 {
                // removes minor branches
                continue;
            }
            if let Ok(Some(ty)) = feat.field_as_integer(fty) {
                if ty == 566 {
                    // https://hydro.nationalmap.gov/arcgis/rest/services/NHDPlus_HR/MapServer/3
                    // ftype = 428 is Pipeline; 566 is Coastline; there are more
                    // categories but removing just these
                    // EDIT: pipeline is used for some dam's release, so can't remove that
                    continue;
                }
                // InNetwork = no means the streams are isolated from the whole network, but I think it's fine to leave them be
                // MainPath was supposed to help us, but it's Unspecified for everything
                let mut ft = Feature::new(&defn)?;
                ft.set_geometry(feat.geometry().unwrap().clone())?;
                ft.create(&layer)?;
            }
        }
    }
    if verbose {
        println!();
    }
    Ok(())
}
