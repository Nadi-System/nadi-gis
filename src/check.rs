use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::Context;
use clap::Args;
use gdal::vector::{FieldValue, Geometry, Layer, LayerAccess, LayerOptions, OGRFieldType};
use gdal::{Dataset, Driver, DriverManager, GdalOpenFlags, Metadata};

use crate::cliargs::CliAction;
use crate::types::*;
use crate::utils::*;

#[derive(Args)]
pub struct CliArgs {
    /// List given number of points
    #[arg(short, long, conflicts_with = "output")]
    list: Option<Option<usize>>,
    /// Output driver [default: based on file extension]
    #[arg(short, long)]
    driver: Option<String>,
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
            let driver = get_driver_by_filename(&filename, &self.driver)?;
            let mut out_data = driver.create_vector_only(&filename)?;
            // let mut txn = out_data.start_transaction()?;
            let mut layer = out_data.create_layer(LayerOptions {
                name: lyr.as_ref().unwrap_or(&"branches".to_string()),
                srs: streams_lyr.spatial_ref().as_ref(),
                ty: gdal_sys::OGRwkbGeometryType::wkbPoint,
                ..Default::default()
            })?;
            layer.create_defn_fields(&[("category", OGRFieldType::OFTString)])?;
            let fields = ["category"];

            for (cat, list) in categories {
                for pt in list {
                    let mut geom = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbPoint)?;
                    geom.add_point_2d(pt.coord2());
                    layer.create_feature_fields(
                        geom,
                        &fields,
                        &[FieldValue::StringValue(cat.to_string())],
                    )?;
                }
            }
            // txn.commit()?;
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

fn get_geometries(
    layer: &mut Layer,
    field: &Option<String>,
) -> Result<Vec<(String, Geometry)>, anyhow::Error> {
    layer
        .features()
        .enumerate()
        .map(|(i, f)| {
            let geom = match f.geometry() {
                Some(g) => g.clone(),
                None => {
                    // TODO take X,Y possible names as Vec<String>
                    let x = f.field_as_double_by_name("lon")?.unwrap();
                    let y = f.field_as_double_by_name("lat")?.unwrap();
                    let mut pt = Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbPoint)?;
                    pt.add_point((x, y, 0.0));
                    pt
                }
            };
            let name = if let Some(name) = field {
                f.field_as_string_by_name(name)?.unwrap_or("".to_string())
            } else {
                i.to_string()
            };
            Ok((name, geom.to_owned()))
        })
        .collect()
}

fn get_driver_by_filename(filename: &PathBuf, driver: &Option<String>) -> anyhow::Result<Driver> {
    let drivers =
        get_drivers_for_filename(filename.to_str().unwrap(), &GdalOpenFlags::GDAL_OF_VECTOR);

    if let Some(driver) = driver {
        drivers
            .into_iter()
            .filter(|d| d.short_name() == *driver)
            .next()
            .context(format!(
                "There is no matching vector driver {driver} for filename {filename:?}"
            ))
    } else {
        if drivers.len() > 1 {
            eprintln!(
                "Multiple drivers are compatible defaulting to the first: {:?}",
                drivers
                    .iter()
                    .map(|d| d.short_name())
                    .collect::<Vec<String>>()
            )
        }
        drivers.into_iter().next().context(format!(
            "Couldn't infer driver based on filename: {filename:?}"
        ))
    }
}

// remove once the gdal has the pull request merged
// https://github.com/georust/gdal/pull/510
fn get_drivers_for_filename(filename: &str, options: &GdalOpenFlags) -> Vec<Driver> {
    let ext = {
        let filename = filename.to_ascii_lowercase();
        let e = match filename.rsplit_once(".") {
            Some(("", _)) => "", // hidden file no ext
            Some((f, "zip")) => {
                // zip files could be zipped shp or gpkg
                if f.ends_with(".shp") {
                    "shp.zip"
                } else if f.ends_with(".gpkg") {
                    "gpkg.zip"
                } else {
                    "zip"
                }
            }
            Some((_, e)) => e, // normal file with ext
            None => "",
        };
        e.to_string()
    };

    let mut drivers: Vec<Driver> = Vec::new();
    for i in 0..DriverManager::count() {
        let d = DriverManager::get_driver(i).expect("Index for this loop should be valid");
        let mut supports = false;
        if (d.metadata_item("DCAP_CREATE", "").is_some()
            || d.metadata_item("DCAP_CREATECOPY", "").is_some())
            && ((options.contains(GdalOpenFlags::GDAL_OF_VECTOR)
                && d.metadata_item("DCAP_VECTOR", "").is_some())
                || (options.contains(GdalOpenFlags::GDAL_OF_RASTER)
                    && d.metadata_item("DCAP_RASTER", "").is_some()))
        {
            supports = true;
        } else if options.contains(GdalOpenFlags::GDAL_OF_VECTOR)
            && d.metadata_item("DCAP_VECTOR_TRANSLATE_FROM", "").is_some()
        {
            supports = true;
        }
        if !supports {
            continue;
        }

        if let Some(e) = &d.metadata_item("DMD_EXTENSION", "") {
            if *e == ext {
                drivers.push(d);
                continue;
            }
        }
        if let Some(e) = d.metadata_item("DMD_EXTENSIONS", "") {
            if e.split(" ").collect::<Vec<&str>>().contains(&ext.as_str()) {
                drivers.push(d);
                continue;
            }
        }

        if let Some(pre) = d.metadata_item("DMD_CONNECTION_PREFIX", "") {
            if filename.starts_with(&pre) {
                drivers.push(d);
            }
        }
    }

    return drivers;
}
