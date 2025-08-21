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
    /// Search Radius for the output node
    #[arg(short, long, default_value = "0.5")]
    radius: f64,
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
        let points: HashMap<usize, Point2D> = points_lyr
            .features()
            .enumerate()
            .filter_map(|(i, p)| {
                p.geometry()
                    .map(|g| (i, Point2D::new3(g.get_point(0)).unwrap()))
            })
            .collect();
        println!("Mapping Points");
        let points_map: HashMap<Point2D, usize> =
            points.iter().map(|(k, v)| (v.clone(), *k)).collect();
        let mut connections = Vec::with_capacity(points_map.len());
        let total = points_lyr.feature_count() as usize;
        if self.verbose {
            println!("Start Connection Seeking");
        }
        for (prog, point) in points_lyr.features().enumerate() {
            if self.verbose {
                print!(
                    "\rReading Points: {}% ({}/{})",
                    prog * 100 / total,
                    prog,
                    total
                );
                std::io::stdout().flush().ok();
            }
            if let Some(geom) = point.geometry() {
                let (x, y, _) = geom.get_point(0);
                let pt = Point2D::new2((x, y))?;
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
                let edges: Vec<(Point2D, Point2D)> = stream_points
                    .iter()
                    .zip(stream_points[1..].iter())
                    .map(|(s, e)| (Point2D::new2(*s).unwrap(), Point2D::new2(*e).unwrap()))
                    .collect();
                let edges: HashMap<Point2D, Point2D> = edges.into_iter().rev().collect();
                let mut start = &pt;
                let mut output: Option<Point2D> = None;
                loop {
                    let end = edges.get(start);
                    if let Some(e) = end {
                        start = e;
                    } else {
                        break;
                    }
                    if points_map.contains_key(&start) {
                        output = Some(start.clone());
                        break;
                    }
                }
                if let Some(out) = output {
                    connections.push((pt, out));
                } else {
                    eprintln!("Outlet: {:?}", pt.coord2());
                }
            }
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
            for idx in 0..pts_defn.len() {
                // inp
                if let Some(value) = points_lyr
                    .feature(points_map[&start] as u64)
                    .unwrap()
                    .field(idx)?
                {
                    ft.set_field(idx * 2, &value)?;
                }
                // out
                if let Some(value) = points_lyr
                    .feature(points_map[&end] as u64)
                    .unwrap()
                    .field(idx)?
                {
                    ft.set_field(idx * 2 + 1, &value)?;
                }
            }
            ft.create(&mut layer)?;
        }
        txn.commit()?;

        if self.verbose {
            println!("\rCompleted : {}% ({}/{})", 100, total, total);
        }
        Ok(())
    }
}
