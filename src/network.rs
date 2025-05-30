use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use anyhow::{bail, Context};
use clap::Args;
use gdal::vector::{
    Defn, Feature, FieldValue, Geometry, Layer, LayerAccess, LayerOptions, OGRFieldType,
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
    /// Fields to use as id for Points file
    #[arg(short, long)]
    points_field: Option<String>,
    /// Output driver for --network [default: based on file extension]
    #[arg(short, long)]
    driver: Option<String>,
    /// Overwrite the network file if it exists
    #[arg(short = 'O', long)]
    overwrite: bool,
    /// Output network GIS file
    ///
    /// If given the subset of the stream network touching the points
    /// of interest will be saved in a GIS file.
    #[arg(short, long, value_parser=parse_new_layer)]
    network: Option<(PathBuf, Option<String>)>,
    /// Output network text file
    ///
    /// If given, the output will be written to the file instead of
    /// printing to stdout
    #[arg(short, long)]
    output: Option<PathBuf>,
    /// Take every nth point from the stream geometry
    ///
    /// Increase this value if your points of interest are far apart
    /// (not in the same stream segments) as it'll save memory and
    /// processing.
    #[arg(short, long, default_value = "1")]
    take: usize,
    /// Threashold distance for the snapping to streams
    #[arg(short = 'T', long)]
    threshold: Option<f64>,
    /// Only save endpoints in the network GIS file
    #[arg(short, long)]
    endpoints: bool,
    /// Print progress
    #[arg(short, long)]
    verbose: bool,
    /// if provided save the movement of point during snapping in a file
    #[arg(short, long, value_parser=parse_new_layer)]
    snap_line: Option<(PathBuf, Option<String>)>,
    /// Nodes file, if provided save the nodes of the graph as points with nodeid
    #[arg(short = 'N', long, value_parser=parse_new_layer)]
    nodes: Option<(PathBuf, Option<String>)>,
    /// Points file with points of interest
    #[arg(value_parser=parse_layer, value_name="POINTS_FILE[::LAYER]")]
    points: (PathBuf, String),
    /// Streams vector file with streams network
    #[arg(value_parser=parse_layer, value_name="STREAMS_FILE[::LAYER]")]
    streams: (PathBuf, String),
}

impl CliAction for CliArgs {
    fn run(self) -> Result<(), anyhow::Error> {
        let points_data = Dataset::open(&self.points.0).unwrap();
        let points = points_data.layer_by_name(&self.points.1).unwrap();

        let streams_data = Dataset::open(&self.streams.0).unwrap();
        let streams = streams_data.layer_by_name(&self.streams.1).unwrap();

        if self.ignore_spatial_ref || check_spatial_ref(&points, &streams).is_ok() {
            self.connections(points, streams)?;
        }

        Ok(())
    }
}

impl CliArgs {
    fn connections(&self, mut points_lyr: Layer, mut streams_lyr: Layer) -> anyhow::Result<()> {
        let points: Vec<(String, Point2D)> = self.points(&mut points_lyr)?;
        let streams = self.edges(&mut streams_lyr)?;
        if self.verbose {
            println!();
        }
        if points.is_empty() || streams.is_empty() {
            return Ok(());
        }
        let points = self.rstar(points, &streams)?;

        // if multiple points have the same nearest point in the stream network, process them here.
        let mut points_temp_dir: HashMap<&Point2D, Vec<&str>> = HashMap::new();
        for (k, v) in &points {
            if let Some(n) = points_temp_dir.get_mut(v) {
                n.push(k);
            } else {
                points_temp_dir.insert(v, vec![k]);
            }
        }

        let mut str_edges: HashMap<&str, &str> = HashMap::new();
        // if any points reach this Point2D, connect them here
        let points_nodes: HashMap<&Point2D, (&str, &str)> = points_temp_dir
            .into_iter()
            .map(|(k, mut v)| {
                v.sort();
                let n = v.len();
                if n > 1 {
                    for i in 1..n {
                        str_edges.insert(v[i - 1], v[i]);
                    }
                }
                (k, (v[0], v[n - 1]))
            })
            .collect();

        let mut points_touched_edges: HashSet<(&Point2D, &Point2D)> = HashSet::new();
        fn find_outlet<'b>(
            inp: &'b Point2D,
            points_nodes: &HashMap<&Point2D, (&str, &str)>,
            edges: &'b HashMap<Point2D, Point2D>,
            threshold: usize,
            touched: &mut HashSet<(&'b Point2D, &'b Point2D)>,
            connect_only: bool,
        ) -> Option<&'b Point2D> {
            let mut outlet = inp;
            let mut ind = 0;
            while ind < threshold {
                ind += 1;
                if let Some(v) = edges.get(&outlet) {
                    if points_nodes.contains_key(v) {
                        if connect_only {
                            touched.insert((inp, v));
                        } else {
                            touched.insert((outlet, v));
                        }
                        return Some(v);
                    } else if !connect_only {
                        touched.insert((outlet, v));
                    }
                    outlet = v;
                } else {
                    return None;
                }
            }
            None
        }

        let mut outlets = vec![];
        let mut progress = 0;
        let total = points_nodes.len();
        for pt in points_nodes.keys() {
            let outlet: Option<&Point2D> = find_outlet(
                pt,
                &points_nodes,
                &streams,
                100000,
                &mut points_touched_edges,
                self.endpoints,
            );
            if let Some(o) = outlet {
                str_edges.insert(points_nodes[pt].1, points_nodes[o].0);
            } else {
                outlets.push(pt);
            }
            if self.verbose {
                progress += 1;
                print!(
                    "\rSearching Connections: {}% ({}/{})",
                    progress * 100 / total,
                    progress,
                    total
                );
            }
        }
        if self.verbose {
            println!();
        }

        if outlets.len() > 1 {
            eprintln!("\nMultiple Outlets Found:");
            for o in outlets {
                eprintln!("{} {} -> None", points_nodes[o].1, o);
            }
        } else {
            eprintln!(
                "\nOutlet: {} {} -> None",
                points_nodes[outlets[0]].1, outlets[0]
            );
        }

        if let Some(outfile) = &self.output {
            let file = File::create(outfile)?;
            let mut writer = BufWriter::new(file);
            for (k, v) in str_edges {
                writeln!(writer, "{k} -> {v}")?;
            }
        } else {
            for (k, v) in str_edges {
                println!("{k} -> {v}");
            }
        }

        if let Some(out) = &self.network {
            let mut out_data = gdal_update_or_create(&out.0, &self.driver, self.overwrite)?;

            let save = |d: &mut Dataset| -> anyhow::Result<()> {
                let mut layer = d.create_layer(LayerOptions {
                    name: out.1.as_ref().unwrap_or(&"network".to_string()),
                    ty: gdal_sys::OGRwkbGeometryType::wkbLineString,
                    ..Default::default()
                })?;
                for (start, end) in &points_touched_edges {
                    let mut edge_geometry =
                        Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbLineString)?;
                    edge_geometry.add_point_2d(start.coord2());
                    edge_geometry.add_point_2d(end.coord2());
                    layer.create_feature(edge_geometry)?;
                }
                Ok(())
            };

            let mut trans = false;
            // have to use trans flag here because of borrow rule;
            // uses transaction when it can to speed up the process.
            if let Ok(mut txn) = out_data.start_transaction() {
                save(&mut txn)?;
                txn.commit()?;
                trans = true;
            };
            if !trans {
                save(&mut out_data)?;
            }
        }
        Ok(())
    }

    fn edges(&self, streams_lyr: &mut Layer) -> anyhow::Result<HashMap<Point2D, Point2D>> {
        let s: HashMap<Point2D, Point2D> =
            read_stream_points(streams_lyr, self.verbose, self.take)?
                .into_iter()
                .rev()
                .collect();
        Ok(s)
    }

    fn points(&self, layer: &mut Layer) -> anyhow::Result<Vec<(String, Point2D)>> {
        let total = layer.feature_count();
        let mut progress = 0;
        if self.verbose {
            println!();
        }
        // TODO take X,Y possible names as Vec<String>
        let x_field = layer.defn().field_index("lon");
        let y_field = layer.defn().field_index("lat");
        let name_field = self
            .points_field
            .as_ref()
            .and_then(|f| layer.defn().field_index(f).ok());
        layer
            .features()
            .enumerate()
            .map(|(i, f)| {
                let geom = match f.geometry() {
                    Some(g) => Point2D::new3(g.get_point(0)),
                    None => {
                        // TODO: make it check for geometry column and get this sorted out
                        let x = f.field_as_double(x_field.clone()?)?;
                        let y = f.field_as_double(y_field.clone()?)?;
                        if let (Some(x), Some(y)) = (x, y) {
                            Point2D::new2((x, y))
                        } else {
                            Err(anyhow::Error::msg("No values in lon/lat field"))
                        }
                    }
                }?;
                let name = if let Some(namef) = name_field {
                    f.field_as_string(namef)?.unwrap_or(format!("Unnamed_{i}"))
                } else {
                    i.to_string()
                };
                if self.verbose {
                    progress += 1;
                    print!(
                        "\rReading Points: {}% ({}/{})",
                        progress * 100 / total,
                        progress,
                        total
                    );
                }
                Ok((name, geom))
            })
            .collect()
    }

    fn rstar(
        &self,
        points: Vec<(String, Point2D)>,
        edges: &HashMap<Point2D, Point2D>,
    ) -> anyhow::Result<HashMap<String, Point2D>> {
        let mut points_closest: HashMap<String, Point2D> = HashMap::with_capacity(points.len());
        let mut progress: usize = 0;
        let total = points.len();
        eprintln!("Loading Points in RTree");
        let pts: HashSet<_> = edges.iter().flat_map(|(k, v)| vec![k, v]).collect();
        let pts: Vec<_> = pts.into_iter().map(|k| k.coord2()).collect();
        let all_points = RTree::bulk_load(pts);
        let sq_threshold = self.threshold.map(|t| t.powi(2));

        let mut err = HashSet::new();
        let mut snapped = Vec::with_capacity(points.len());
        for (k, p) in points {
            let place = match all_points.nearest_neighbor(&p.coord2()) {
                Some(p) => p,
                None => {
                    // only happens if the tree is empty I think (doc not present)
                    eprintln!("{:?}", p.coord2());
                    eprintln!("{:?}", all_points.iter().next());
                    err.insert(k);
                    continue;
                }
            };
            snapped.push((k.clone(), p.coord2(), *place));
            let min_pt = Point2D::new2(*place).unwrap();
            if let Some(t) = sq_threshold {
                if p.sq_dist(&min_pt) > t {
                    err.insert(k);
                    continue;
                }
            }
            points_closest.insert(k, min_pt);
            if self.verbose {
                progress += 1;
                print!(
                    "\rSnapping Points: {}% ({}/{})",
                    progress * 100 / total,
                    progress,
                    total
                );
            }
        }
        if self.verbose {
            println!();
        }
        if let Some(out) = &self.snap_line {
            let mut out_data = gdal_update_or_create(&out.0, &self.driver, self.overwrite)?;

            let save = |d: &mut Dataset| -> anyhow::Result<()> {
                let lyr_name = out.1.as_deref().unwrap_or("snap-line");
                // if layer is there and we can delete it, delete it
                delete_layer(d, lyr_name).ok();
                let mut layer = d.create_layer(LayerOptions {
                    name: lyr_name,
                    ty: gdal_sys::OGRwkbGeometryType::wkbLineString,
                    ..Default::default()
                })?;
                layer.create_defn_fields(&[
                    ("name", OGRFieldType::OFTString),
                    ("error", OGRFieldType::OFTString),
                ])?;
                let defn = Defn::from_layer(&layer);
                for (name, start, end) in &snapped {
                    let mut geom = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbLineString)?;
                    geom.add_point_2d(*start);
                    geom.add_point_2d(*end);
                    let mut ft = Feature::new(&defn)?;
                    ft.set_geometry(geom)?;
                    ft.set_field_string(0, name)?;
                    ft.set_field_string(1, if err.contains(name) { "yes" } else { "no" })?;
                    ft.create(&mut layer)?;
                }
                Ok(())
            };

            let mut trans = false;
            // have to use trans flag here because of borrow rule;
            // uses transaction when it can to speed up the process.
            if let Ok(mut txn) = out_data.start_transaction() {
                save(&mut txn)?;
                txn.commit()?;
                trans = true;
            };
            if !trans {
                save(&mut out_data)?;
            }
        }
        if !err.is_empty() {
            Err(anyhow::Error::msg(format!(
                "Errors on snapping points to streams: [{}]",
                if self.snap_line.is_none() {
                    err.into_iter().join(", ")
                } else {
                    format!("{} Nodes", err.len())
                }
            )))
        } else {
            Ok(points_closest)
        }
    }
}

fn read_stream_points(
    layer: &mut Layer,
    verbose: bool,
    take: usize,
) -> Result<Vec<(Point2D, Point2D)>, anyhow::Error> {
    let total = layer.feature_count();
    let mut progress = 0;
    if verbose {
        println!();
    }
    let mut streams: Vec<(Point2D, Point2D)> =
        Vec::with_capacity(layer.feature_count() as usize * 2);
    for f in layer.features() {
        match f.geometry() {
            Some(g) => {
                let mut pts = Vec::new();
                g.get_points(&mut pts);
                streams.append(&mut edges_from_pts(&pts, take));
            }
            None => return Err(anyhow::Error::msg("No geometry found in the layer")),
        };

        if verbose {
            progress += 1;
            print!(
                "\rReading Streams: {}% ({}/{})",
                progress * 100 / total,
                progress,
                total
            );
        }
    }
    Ok(streams)
}

fn edges_from_pts(pts: &[(f64, f64, f64)], take: usize) -> Vec<(Point2D, Point2D)> {
    let mut start = Point2D::new3(pts[0]).unwrap();
    let end = Point2D::new3(pts[pts.len() - 1]).unwrap();
    let mid = pts.len() - 2;
    if mid < take {
        vec![(start, end)]
    } else {
        // reducing the number of intermediate nodes
        let mut eds = Vec::with_capacity(mid / take + 3);
        for i in 0..(mid / take) {
            let p = Point2D::new3(pts[1 + i * take]).unwrap();
            eds.push((start, p.clone()));
            start = p;
        }
        eds.push((start, end));
        eds
    }
}
