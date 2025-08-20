use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{bail, Context};
use clap::Args;
use gdal::vector::{
    Defn, Feature, FieldDefn, FieldValue, Geometry, Layer, LayerAccess, LayerOptions, OGRFieldType,
};
use gdal::{Dataset, Driver, DriverManager, GdalOpenFlags, Metadata};

use itertools::Itertools;
use rstar::RTree;

use crate::cliargs::CliAction;
use crate::types::*;
use crate::utils::*;

#[derive(Args)]
pub struct CliArgs {
    /// Ignore spatial reference check
    #[arg(short, long, action)]
    ignore_spatial_ref: bool,
    /// Print progress
    #[arg(short, long)]
    verbose: bool,
    /// Output driver for --output [default: based on file extension]
    #[arg(short, long)]
    driver: Option<String>,
    /// Overwrite the output file if it exists
    #[arg(short = 'O', long)]
    overwrite: bool,
    /// Search Radius for the nearest point
    #[arg(short, long, default_value = "0.2")]
    radius: f64,
    /// Points file with points of interest
    #[arg(value_parser=parse_layer, value_name="POINTS_FILE[::LAYER]")]
    points: (PathBuf, String),
    /// Streams vector file with streams network
    #[arg(value_parser=parse_layer, value_name="STREAMS_FILE[::LAYER]")]
    streams: (PathBuf, String),
    /// Output GIS file for snapped geometries
    #[arg(value_parser=parse_new_layer)]
    output: (PathBuf, Option<String>),
}

impl CliAction for CliArgs {
    fn run(self) -> Result<(), anyhow::Error> {
        let points_data = Dataset::open(&self.points.0).unwrap();
        let points = points_data.layer_by_name(&self.points.1).unwrap();

        let streams_data = Dataset::open(&self.streams.0).unwrap();
        let streams = streams_data.layer_by_name(&self.streams.1).unwrap();

        if self.ignore_spatial_ref || check_spatial_ref(&points, &streams).is_ok() {
            self.snap(points, streams)?;
        }

        Ok(())
    }
}

impl CliArgs {
    fn snap(&self, mut points_lyr: Layer, mut streams_lyr: Layer) -> anyhow::Result<()> {
        let total = points_lyr.feature_count() as usize;
        if self.verbose {
            println!();
        }

        let mut out_data = gdal_update_or_create(&self.output.0, &self.driver, self.overwrite)?;
        let mut txn = out_data.start_transaction().expect("Transaction failed");

        let lyr_name = self.output.1.as_deref().unwrap_or("Snapped");
        let mut layer = txn.create_layer(LayerOptions {
            name: lyr_name,
            ty: gdal_sys::OGRwkbGeometryType::wkbPoint,
            ..Default::default()
        })?;
        let pts_defn = Defn::from_layer(&points_lyr)
            .fields()
            .map(|field| (field.name(), field.field_type(), field.width()))
            .collect::<Vec<_>>();
        for fd in &pts_defn {
            let field_defn = FieldDefn::new(&fd.0, fd.1)?;
            field_defn.set_width(fd.2);
            field_defn.add_to_layer(&layer)?;
        }
        let defn = Defn::from_layer(&layer);

        for (prog, point) in points_lyr.features().enumerate() {
            if self.verbose {
                print!(
                    "\rReading Points: {}% ({}/{})",
                    prog * 100 / total,
                    prog,
                    total
                );
            }
            if let Some(geom) = point.geometry() {
                let (x, y, _) = geom.get_point(0);
                streams_lyr.clear_spatial_filter();
                streams_lyr.set_spatial_filter_rect(
                    x - self.radius,
                    y - self.radius,
                    x + self.radius,
                    y + self.radius,
                );
                let stream_points: Vec<(f64, f64)> = streams_lyr
                    .features()
                    .filter_map(|f| f.geometry().cloned())
                    .flat_map(|g1| {
                        let mut out = Vec::new();
                        let gc = g1.geometry_count();
                        // for handling multi-geometry as well
                        if gc > 0 {
                            (0..gc)
                                .map(|j| {
                                    let g = g1.get_geometry(j);
                                    g.get_points(&mut out);
                                })
                                .collect()
                        } else {
                            g1.get_points(&mut out);
                        }
                        out
                    })
                    .map(|(x, y, _)| (x, y))
                    .collect();
                let all_points = RTree::bulk_load(stream_points);
                let snapped = match all_points.nearest_neighbor(&(x, y)) {
                    Some(p) => p,
                    None => {
                        // only happens if the tree is empty I think (doc not present)
                        eprintln!("{:?}", (x, y));
                        eprintln!("{:?}", all_points.iter().next());
                        panic!("Snap failed");
                    }
                };
                let mut geom = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbPoint)?;
                geom.add_point_2d(*snapped);
                let mut ft = Feature::new(&defn)?;
                for idx in 0..pts_defn.len() {
                    if let Some(value) = point.field(idx)? {
                        ft.set_field(idx, &value)?;
                    }
                }
                ft.set_geometry(geom)?;
                ft.create(&mut layer)?;
            }
        }
        txn.commit()?;

        if self.verbose {
            println!("\rCompleted : {}% ({}/{})", 100, total, total);
        }
        Ok(())
    }
}
