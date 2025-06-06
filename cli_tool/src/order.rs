use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::types::Point2D;
use anyhow::Context;
use clap::Args;
use gdal::spatial_ref::SpatialRef;
use gdal::vector::{
    Defn, Feature, FieldDefn, FieldValue, Geometry, Layer, LayerAccess, LayerOptions, OGRFieldType,
};
use gdal::{Dataset, DriverManager, DriverType};

use crate::cliargs::CliAction;
use crate::types::*;
use crate::utils::*;

#[derive(Args)]
pub struct CliArgs {
    /// Output driver [default: based on file extension]
    #[arg(short, long)]
    driver: Option<String>,
    /// Print progress
    #[arg(short, long)]
    verbose: bool,
    /// Overwrite the output file if it exists
    #[arg(short = 'O', long)]
    overwrite: bool,

    /// Streams vector file with streams network
    #[arg(value_parser=parse_layer, value_name="STREAMS_FILE[:LAYER]")]
    streams: (PathBuf, String),
    /// Output file
    #[arg(value_parser=parse_new_layer)]
    output: (PathBuf, Option<String>),
}

impl CliAction for CliArgs {
    fn run(self) -> Result<(), anyhow::Error> {
        let streams_data = Dataset::open(&self.streams.0).unwrap();
        let mut streams_lyr = streams_data.layer_by_name(&self.streams.1).unwrap();
        let points = get_endpoints(&mut streams_lyr, self.verbose)?;
        if points.is_empty() {
            eprintln!("Empty file, nothing to do.");
            return Ok(());
        }
        if self.verbose {
            println!("\nCreating HashMap from points")
        }
        let mut order: HashMap<(&Point2D, &Point2D), usize> =
            points.iter().map(|e| ((&e.0, &e.1), 0)).collect();
        if self.verbose {
            println!("\nCreating Edges")
        }
        let edges: HashMap<&Point2D, &Point2D> = points.iter().rev().map(|(s, e)| (s, e)).collect();
        if self.verbose {
            println!("\nDetecting leaf nodes")
        }
        let tips: HashSet<&Point2D> = edges.iter().map(|(&s, _)| s).collect();
        let no_tips: HashSet<&Point2D> = edges.iter().map(|(_, &e)| e).collect();
        let tips = tips.difference(&no_tips);

        let mut progress = 0;
        let total = tips.clone().count();
        for mut pt in tips {
            while let Some(out) = edges.get(pt) {
                if let Some(o) = order.get_mut(&(pt, out)) {
                    *o += 1;
                }
                pt = out;
            }
            if self.verbose {
                progress += 1;
                print!(
                    "\rCalculating Order: {}% ({} of {})",
                    progress * 100 / total,
                    progress,
                    total
                );
            }
        }

        let lyr_name = self.output.1.as_deref().unwrap_or("ordered-stream");
        let sref = streams_lyr.spatial_ref();

        let mut out_data = gdal_update_or_create(&self.output.0, &self.driver, self.overwrite)?;

        let order: Vec<i64> = points.iter().map(|(a, b)| order[&(a, b)] as i64).collect();
        let mut trans = false;
        // have to use trans flag here because of borrow rule;
        // uses transaction when it can to speed up the process.
        if let Ok(mut txn) = out_data.start_transaction() {
            write_layer(
                &order,
                &mut txn,
                &mut streams_lyr,
                lyr_name,
                sref.as_ref(),
                self.verbose,
            )?;
            txn.commit()?;
            trans = true;
        };

        if !trans {
            write_layer(
                &order,
                &mut out_data,
                &mut streams_lyr,
                lyr_name,
                sref.as_ref(),
                self.verbose,
            )?;
        }

        Ok(())
    }
}

fn write_layer(
    order: &[i64],
    out_data: &mut Dataset,
    streams_lyr: &mut Layer,
    lyr_name: &str,
    sref: Option<&SpatialRef>,
    verbose: bool,
) -> anyhow::Result<()> {
    let layer = out_data.create_layer(LayerOptions {
        name: lyr_name,
        srs: sref,
        ty: gdal_sys::OGRwkbGeometryType::wkbLineString,
        ..Default::default()
    })?;

    let fields_defn = streams_lyr
        .defn()
        .fields()
        .map(|field| (field.name(), field.field_type(), field.width()))
        .collect::<Vec<_>>();
    for fd in &fields_defn {
        let field_defn = FieldDefn::new(&fd.0, fd.1)?;
        field_defn.set_width(fd.2);
        field_defn.add_to_layer(&layer)?;
    }

    FieldDefn::new("order", OGRFieldType::OFTInteger64)?.add_to_layer(&layer)?;
    let fid = layer
        .defn()
        .field_index("order")
        .expect("Just added order field");
    let defn = Defn::from_layer(&layer);
    let total = streams_lyr.feature_count();
    let mut progress = 0;
    for (i, feat) in streams_lyr.features().enumerate() {
        let mut ft = Feature::new(&defn)?;
        ft.set_geometry(feat.geometry().unwrap().clone())?;
        // TODO: do a proper field copy
        for (j, _fd) in fields_defn.iter().enumerate() {
            if let Some(value) = feat.field(j)? {
                ft.set_field(j, &value)?;
            }
        }
        ft.set_field_integer64(fid, order[i])?;
        ft.create(&layer)?;

        if verbose {
            progress += 1;
            println!("Writing Features: {}", progress * 100 / total);
        }
    }
    Ok(())
}

pub fn get_endpoints(
    layer: &mut Layer,
    verbose: bool,
) -> Result<Vec<(Point2D, Point2D)>, anyhow::Error> {
    let total = layer.feature_count() as usize;
    layer
        .features()
        .enumerate()
        .filter_map(|(i, f)| {
            if verbose {
                print!(
                    "\rReading Geometries: {}% ({} of {})",
                    i * 100 / total,
                    i,
                    total
                );
            }
            f.geometry().map(|g1| {
                let (a, b) = if g1.geometry_name().starts_with("Multi") {
                    let g = g1.get_geometry(0);
                    (g.get_point(0), g.get_point((g.point_count() - 1) as i32))
                } else {
                    (g1.get_point(0), g1.get_point((g1.point_count() - 1) as i32))
                };
                Ok((Point2D::new3(a)?, Point2D::new3(b)?))
            })
        })
        .collect()
}
