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
    /// Threshold for gap between the stream lines to assume they are connected
    #[arg(short, long, default_value = "0.0000005")]
    threshold: f64,
    /// Points file with points of interest
    #[arg(value_parser=parse_layer, value_name="POINTS_FILE[::LAYER]")]
    points: (PathBuf, String),
    /// Streams vector file with streams network
    #[arg(value_parser=parse_layer, value_name="STREAMS_FILE[::LAYER]")]
    streams: (PathBuf, String),
    /// Output GIS file for connections
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
            self.network(points, streams)?;
        }

        Ok(())
    }
}

impl CliArgs {
    fn network(&self, mut points_lyr: Layer, mut streams_lyr: Layer) -> anyhow::Result<()> {
        println!("Reading Points");
        let points: HashMap<u64, Point2D> = points_lyr
            .features()
            .filter_map(|f| f.fid().map(|i| (i, f)))
            .filter_map(|(i, f)| {
                f.geometry()
                    .map(|g| (i, Point2D::new3(g.get_point(0)).unwrap()))
            })
            .collect();
        println!("Mapping Points");
        let points_map: HashMap<Point2D, u64> =
            points.iter().map(|(k, v)| (v.clone(), *k)).collect();
        let mut connections = Vec::with_capacity(points_map.len());
        let mut outlets = Vec::with_capacity(points_map.len());
        let total = points_lyr.feature_count();
        if self.verbose {
            println!("Start Connection Seeking");
        }
        let mut prog = 0u64;
        for (fid, pt) in &points {
            if self.verbose {
                prog += 1;
                print!(
                    "\rReading Points: {}% ({}/{})",
                    prog * 100 / total,
                    prog,
                    total
                );
                std::io::stdout().flush().ok();
            }
            let point = points_lyr
                .feature(*fid)
                .expect("FID comes from this layer; should work");
            if let Some(geom) = point.geometry() {
                let (mut x, mut y, _) = geom.get_point(0);

                let mut searching = false;
                let mut iter = 0;
                loop {
                    iter += 1;
                    // find the stream points for stream closest to the point.
                    let stream_points: Vec<(f64, f64)> =
                        get_next_geom_pts(&mut streams_lyr, (x, y), self.threshold, searching);
                    if stream_points.is_empty() || stream_points.len() == 1 || iter > 10000 {
                        outlets.push((pt.coord2(), (x, y)));
                        eprintln!("Outlet: {:?}", pt.coord2());
                        break;
                    }
                    searching = true;
                    let points: Vec<Point2D> = stream_points
                        .iter()
                        .map(|s| Point2D::new2(*s).unwrap())
                        .collect();
                    // the point if exists in the geometry, skip
                    // everything before it; only relevant for the
                    // first geom; but if there is a loop, then it
                    // breaks things
                    let pt_inside = points.iter().find_position(|p| *p == pt).map(|p| p.0);
                    let points: Vec<Point2D> = if let Some(ind) = pt_inside {
                        points.into_iter().skip(ind + 1).collect()
                    } else {
                        points.into_iter().collect()
                    };
                    if let Some(out) = points.iter().find(|p| points_map.contains_key(p)) {
                        connections.push((pt.clone(), out.clone()));
                        break;
                    } else {
                        (x, y) = stream_points.into_iter().last().unwrap();
                    }
                }
            }
        }

        let mut out_data = gdal_update_or_create(&self.output.0, &self.driver, self.overwrite)?;
        let mut txn = out_data.start_transaction().expect("Transaction failed");

        let lyr_name = self.output.1.as_deref().unwrap_or("Network");
        let mut layer = txn.create_layer(LayerOptions {
            name: lyr_name,
            ty: gdal_sys::OGRwkbGeometryType::wkbLineString,
            ..Default::default()
        })?;
        let pts_defn = Defn::from_layer(&points_lyr)
            .fields()
            .map(|field| (field.name(), field.field_type(), field.width()))
            .collect::<Vec<_>>();
        for fd in &pts_defn {
            for pre in ["inp", "out"] {
                let field_defn = FieldDefn::new(&format!("{pre}_{}", fd.0), fd.1)?;
                field_defn.set_width(fd.2);
                field_defn.add_to_layer(&layer)?;
            }
        }
        let defn = Defn::from_layer(&layer);
        for (start, end) in connections {
            let mut ft = Feature::new(&defn)?;
            let mut geom = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbLineString)?;
            geom.add_point_2d(start.coord2());
            geom.add_point_2d(end.coord2());
            ft.set_geometry(geom)?;
            // inp
            if let Some(feat) = points_lyr.feature(points_map[&start]) {
                for idx in 0..pts_defn.len() {
                    if let Some(value) = feat.field(idx)? {
                        ft.set_field(idx * 2, &value)?;
                    }
                }
            }
            // out
            if let Some(feat) = points_lyr.feature(points_map[&end]) {
                for idx in 0..pts_defn.len() {
                    if let Some(value) = feat.field(idx)? {
                        ft.set_field(idx * 2 + 1, &value)?;
                    }
                }
            }
            ft.create(&mut layer)?;
        }

        let mut layer2 = txn.create_layer(LayerOptions {
            name: "Outlets",
            ty: gdal_sys::OGRwkbGeometryType::wkbLineString,
            ..Default::default()
        })?;
        let defn = Defn::from_layer(&layer2);
        for (start, end) in outlets {
            let mut ft = Feature::new(&defn)?;
            let mut geom = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbLineString)?;
            geom.add_point_2d(start);
            geom.add_point_2d(end);
            ft.set_geometry(geom)?;
            ft.create(&mut layer2)?;
        }
        txn.commit()?;

        if self.verbose {
            println!("\rCompleted : {}% ({}/{})", 100, total, total);
        }
        Ok(())
    }
}

fn get_next_geom_pts(
    layer: &mut Layer,
    coord: (f64, f64),
    radius: f64,
    starts: bool,
) -> Vec<(f64, f64)> {
    layer.clear_spatial_filter();
    layer.set_spatial_filter_rect(
        coord.0 - radius,
        coord.1 - radius,
        coord.0 + radius,
        coord.1 + radius,
    );
    let geoms: Vec<Geometry> = layer
        .features()
        .filter_map(|f| f.geometry().cloned())
        .filter(|geom| {
            (!starts) // means the geom's start point should be in the (x,y) range
                || get_geom_pts(geom)
                    .get(0)
                    .map(|(x, y)| {
                        (*x < (coord.0 + radius))
                            & (*x > (coord.0 - radius))
                            & (*y < (coord.1 + radius))
                            & (*y > (coord.1 - radius))
                    })
                    .unwrap_or_default()
        })
        .collect();
    match &geoms[..] {
        [] => Vec::new(),
        [g] => get_geom_pts(g),
        [..] => {
            // multiple streams near the point, let's take the longest one, hopefully that's the main one
            let g = geoms
                .iter()
                .max_by(|a, b| a.length().partial_cmp(&b.length()).unwrap())
                .unwrap_or(&geoms[0]);
            get_geom_pts(g)
        }
    }
}

fn get_geom_pts(geom: &Geometry) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    let gc = geom.geometry_count();
    // for handling multi-geometry as well
    if gc > 0 {
        (0..gc)
            .map(|j| {
                let g = geom.get_geometry(j);
                g.get_points(&mut out);
            })
            .collect()
    } else {
        geom.get_points(&mut out);
    }
    out.into_iter().map(|(x, y, _)| (x, y)).collect()
}
