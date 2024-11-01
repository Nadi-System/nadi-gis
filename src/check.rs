use std::collections::HashSet;
use std::path::PathBuf;

use crate::cliargs::CliAction;
use crate::types::*;
use crate::utils::*;
use anyhow::Context;
use clap::Args;
use gdal::spatial_ref::SpatialRef;
use gdal::vector::{FieldValue, Geometry, Layer, LayerAccess, LayerOptions, OGRFieldType};
use gdal::{Dataset, Driver, DriverManager, DriverType, GdalOpenFlags, Metadata};

#[derive(Args)]
pub struct CliArgs {
    /// List given number of points
    #[arg(short, long, conflicts_with = "output")]
    list: Option<Option<usize>>,
    /// Output driver [default: based on file extension]
    #[arg(short, long)]
    driver: Option<String>,
    /// Overwrite the output file if it exists
    #[arg(short = 'O', long)]
    overwrite: bool,
    /// Output file
    #[arg(short, long, value_parser=parse_new_layer)]
    output: Option<(PathBuf, Option<String>)>,
    /// Print progress
    #[arg(short, long)]
    verbose: bool,
    /// Streams vector file with streams network
    #[arg(value_parser=parse_layer, value_name="STREAMS_FILE[:LAYER]")]
    streams: (PathBuf, String),
}

impl CliAction for CliArgs {
    fn run(self) -> Result<(), anyhow::Error> {
        let streams_data = Dataset::open(&self.streams.0).unwrap();
        let mut streams_lyr = streams_data.layer_by_name(&self.streams.1).unwrap();
        let streams = get_geometries(&mut streams_lyr, &None)?;
        let nodes_count = streams_lyr.feature_count() as usize;

        let mut start_nodes: HashSet<Point2D> = HashSet::with_capacity(nodes_count);
        let mut end_nodes: HashSet<Point2D> = HashSet::with_capacity(nodes_count);
        let mut branches: HashSet<Point2D> = HashSet::with_capacity(nodes_count);
        let mut confluences: HashSet<Point2D> = HashSet::with_capacity(nodes_count);
        let total = streams.len();
        let mut points = 0;
        for (i, (_name, geom)) in streams.iter().enumerate() {
            let start = Point2D::new3(geom.get_point(0))?;

            if !start_nodes.insert(start.clone()) {
                branches.insert(start);
            }

            if geom.point_count() == 1 {
                points += 1;
                continue;
            }

            let end = Point2D::new3(geom.get_point((geom.point_count() - 1) as i32))?;

            if !end_nodes.insert(end.clone()) {
                confluences.insert(end);
            }

            if self.verbose {
                println!("Reading Streams: {}% ({}/{})", i * 100 / total, i, total);
            }
        }

        let outlets: HashSet<Point2D> = end_nodes
            .difference(&start_nodes)
            .map(|p| p.clone())
            .collect();
        let origins: HashSet<Point2D> = start_nodes
            .difference(&end_nodes)
            .map(|p| p.clone())
            .collect();

        if points > 0 {
            eprintln!("Invalid Streams File: Point Geometry ({points})");
        }
        if outlets.len() != 1 {
            eprintln!(
                "Invalid Streams File: Need 1 Outlet (has {})",
                outlets.len()
            );
        }
        if !branches.is_empty() {
            eprintln!("Invalid Streams File: Branches ({})", branches.len());
        }

        let categories = [
            ("Outlet", outlets), // all the outlet points; ideally should be 1 for nadi-network
            ("Branch", branches), // any places stream branches off into multiple path downstream
            ("Confluence", confluences), // points where streams met together
            ("Origin", origins), // start point of the streams
        ];

        if let Some((filename, lyr)) = &self.output {
            let mut out_data = gdal_update_or_create(&filename, &self.driver, self.overwrite)?;
            let lyr_name = lyr.as_deref().unwrap_or("nodes");
            let sref = streams_lyr.spatial_ref();

            let mut trans = false;
            // have to use trans flag here because of borrow rule;
            // uses transaction when it can to speed up the process.
            if let Ok(mut txn) = out_data.start_transaction() {
                write_output(&categories, &mut txn, lyr_name, sref.as_ref(), self.verbose)?;
                txn.commit()?;
                trans = true;
            };

            if !trans {
                write_output(
                    &categories,
                    &mut out_data,
                    lyr_name,
                    sref.as_ref(),
                    self.verbose,
                )?;
            }
        } else {
            for (cat, list) in categories {
                println!("* {}: {}", cat, list.len());
                if let Some(total) = self.list {
                    let total = total.unwrap_or_else(|| list.len());
                    for (id, pt) in list.iter().enumerate().take(total) {
                        println!("    {} {:?}", id + 1, pt.coord2())
                    }
                }
            }
        }

        Ok(())
    }
}

fn write_output(
    categories: &[(&str, HashSet<Point2D>)],
    ds: &mut Dataset,
    lyr: &str,
    sref: Option<&SpatialRef>,
    verbose: bool,
) -> anyhow::Result<()> {
    let mut layer = ds.create_layer(LayerOptions {
        name: lyr,
        srs: sref,
        ty: gdal_sys::OGRwkbGeometryType::wkbPoint,
        ..Default::default()
    })?;
    layer.create_defn_fields(&[("category", OGRFieldType::OFTString)])?;
    let fields = ["category"];

    let total: usize = categories.iter().map(|(_, v)| v.len()).sum();
    let mut progress = 0;
    for (cat, list) in categories {
        for pt in list {
            let mut geom = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbPoint)?;
            geom.add_point_2d(pt.coord2());
            layer.create_feature_fields(
                geom,
                &fields,
                &[FieldValue::StringValue(cat.to_string())],
            )?;
            if verbose {
                progress += 1;
                println!("Writing Features: {}", progress * 100 / total);
            }
        }
    }
    Ok(())
}
