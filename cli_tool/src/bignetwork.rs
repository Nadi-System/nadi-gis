use anyhow::{bail, Context};
use clap::Args;
use gdal::vector::{
    Defn, Feature, FieldDefn, FieldValue, Geometry, Layer, LayerAccess, LayerOptions, OGRFieldType,
};
use gdal::{Dataset, Driver, DriverManager, GdalOpenFlags, Metadata};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

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
        if self.verbose {
            println!("Start Connection Seeking");
        }
        let (sender, receiver) = mpsc::channel();
        let points_to_process: Arc<Mutex<Vec<_>>> =
            Arc::new(Mutex::new(points.clone().into_iter().collect()));
        for _ in 0..10 {
            let lyr = self.streams.clone();
            let pts_map = points_map.clone();
            let pts_proc = points_to_process.clone();
            let tx = sender.clone();
            thread::spawn(move || {
                let streams_data = Dataset::open(&lyr.0).unwrap();
                let mut streams = streams_data.layer_by_name(&lyr.1).unwrap();
                loop {
                    let val = pts_proc.lock().unwrap().pop();
                    if let Some((fid, pt)) = val {
                        find_connections(&mut streams, &pts_map, fid, pt, &tx);
                    } else {
                        break;
                    }
                }
            });
        }

        let mut prog = 0u64;
        let mut total = points_lyr.feature_count();
        for msg in receiver {
            prog += 1;
            match msg.resolution {
                Resolution::Branch => {
                    total += 1;
                    find_connections(&mut streams_lyr, &points_map, msg.fid, msg.outlet, &sender);
                }
                Resolution::NotFound => {
                    eprintln!("Outlet: {:?}", msg.input);
                    outlets.push((msg.fid, msg.outlet));
                }
                Resolution::Found => {
                    connections.push((msg.fid, msg.outlet));
                }
            }
            if self.verbose {
                print!(
                    "\rProcessing Points: {}% ({}/{})",
                    prog * 100 / total,
                    prog,
                    total
                );
                std::io::stdout().flush().ok();
            }
            if prog == total {
                // without this there might be infinite loop
                break;
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
            let (st_x, st_y, _) = points_lyr
                .feature(start)
                .and_then(|f| f.geometry().map(|g| g.get_point(0)))
                .expect("FID comes from this layer; should work");
            let mut ft = Feature::new(&defn)?;
            let mut geom = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbLineString)?;
            geom.add_point_2d((st_x, st_y));
            geom.add_point_2d(end.coord2());
            ft.set_geometry(geom)?;
            // inp
            // if let Some(feat) = points_lyr.feature(points_map[&start]) {
            //     for idx in 0..pts_defn.len() {
            //         if let Some(value) = feat.field(idx)? {
            //             ft.set_field(idx * 2, &value)?;
            //         }
            //     }
            // }
            // // out
            // if let Some(feat) = points_lyr.feature(points_map[&end]) {
            //     for idx in 0..pts_defn.len() {
            //         if let Some(value) = feat.field(idx)? {
            //             ft.set_field(idx * 2 + 1, &value)?;
            //         }
            //     }
            // }
            ft.create(&mut layer)?;
        }

        let mut layer2 = txn.create_layer(LayerOptions {
            name: "Outlets",
            ty: gdal_sys::OGRwkbGeometryType::wkbLineString,
            ..Default::default()
        })?;
        let defn = Defn::from_layer(&layer2);
        for (start, end) in outlets {
            let (st_x, st_y, _) = points_lyr
                .feature(start)
                .and_then(|f| f.geometry().map(|g| g.get_point(0)))
                .expect("FID comes from this layer; should work");
            let mut ft = Feature::new(&defn)?;
            let mut geom = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbLineString)?;
            geom.add_point_2d((st_x, st_y));
            geom.add_point_2d(end.coord2());
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

/// Message to send while running network detection algorithm
struct Message {
    fid: u64,
    input: Point2D,
    outlet: Point2D,
    resolution: Resolution,
}

enum Resolution {
    /// Outlet found for this point
    Found,
    /// Outlet not found, searched upto the second point
    NotFound,
    /// The stream branches here
    Branch,
}

const MAX_ITER: usize = 10000;

fn find_connections(
    streams: &mut Layer,
    points_map: &HashMap<Point2D, u64>,
    fid: u64,
    point: Point2D,
    sender: &Sender<Message>,
) {
    let (mut x, mut y) = point.coord2();
    let mut searching = false;
    let mut iter = 0;

    loop {
        iter += 1;
        // find the stream points for stream closest to the point.
        let stream_points: Vec<Vec<(f64, f64)>> = get_next_geom_pts(streams, (x, y), searching);
        if stream_points.is_empty() || iter > MAX_ITER {
            _ = sender.send(Message {
                fid,
                input: point.clone(),
                outlet: Point2D::new2((x, y)).unwrap(),
                resolution: Resolution::NotFound,
            });
            return;
        }
        searching = true;
        let points: Vec<Point2D> = stream_points
            .iter()
            .flatten()
            .map(|s| Point2D::new2(*s).unwrap())
            .collect();
        // the point if exists in the geometry, skip
        // everything before it; only relevant for the
        // first geom; but if there is a loop, then it
        // breaks things
        let pt_inside = points.iter().find_position(|p| *p == &point).map(|p| p.0);
        let points: Vec<Point2D> = if let Some(ind) = pt_inside {
            points.into_iter().skip(ind + 1).collect()
        } else {
            points.into_iter().collect()
        };
        if let Some(out) = points.iter().find(|p| points_map.contains_key(p)) {
            _ = sender.send(Message {
                fid,
                input: point.clone(),
                outlet: out.clone(),
                resolution: Resolution::Found,
            });
            return;
        } else {
            match &stream_points[..] {
                [] => {
                    // should already be covered by if stream_points.is_empty()
                    _ = sender.send(Message {
                        fid,
                        input: point.clone(),
                        outlet: Point2D::new2((x, y)).unwrap(),
                        resolution: Resolution::NotFound,
                    });
                    return;
                }
                [pts, rest @ ..] => {
                    (x, y) = *pts.iter().last().unwrap();
                    // multiple geometries means it branches, and
                    // we'll deal with them in other threads
                    for pts in rest {
                        let (x1, y1) = pts.iter().last().unwrap();
                        if x1 != &x && y1 != &y {
                            // if they converge it's fine
                            _ = sender.send(Message {
                                fid,
                                input: point.clone(),
                                outlet: Point2D::new2((*x1, *y1)).unwrap(),
                                resolution: Resolution::Branch,
                            });
                        }
                    }
                }
            }
        }
    }
}

const EPSILON: f64 = 0.0000005;

fn get_next_geom_pts(layer: &mut Layer, coord: (f64, f64), starts: bool) -> Vec<Vec<(f64, f64)>> {
    layer.clear_spatial_filter();
    layer.set_spatial_filter_rect(
        coord.0 - EPSILON,
        coord.1 - EPSILON,
        coord.0 + EPSILON,
        coord.1 + EPSILON,
    );
    layer
        .features()
        .filter_map(|f| f.geometry().map(get_geom_pts))
        .filter(|geom| {
            (!starts) // means the geom's start point should be in the (x,y) range
                || geom
                    .get(0)
                    .map(|(x, y)| {
                        (*x < (coord.0 + EPSILON))
                            & (*x > (coord.0 - EPSILON))
                            & (*y < (coord.1 + EPSILON))
                            & (*y > (coord.1 - EPSILON))
                    })
                    .unwrap_or_default()
        })
        .collect()
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
