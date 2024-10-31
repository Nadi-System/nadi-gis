use std::collections::HashSet;
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use gdal::vector::{FieldValue, Geometry, Layer, LayerAccess, LayerOptions, OGRFieldType};
use gdal::{Dataset, Driver, DriverManager, GdalOpenFlags, Metadata};

pub fn parse_new_layer(arg: &str) -> Result<(PathBuf, Option<String>), anyhow::Error> {
    if let Some((path, layer)) = arg.split_once(':') {
        Ok((PathBuf::from(path), Some(layer.to_string())))
    } else {
        Ok((PathBuf::from(arg), None))
    }
}

pub fn parse_layer(arg: &str) -> Result<(PathBuf, String), anyhow::Error> {
    if let Some((path, layer)) = arg.split_once(':') {
        let data = Dataset::open(path)?;
        if data.layer_by_name(layer).is_err() {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Layer name {layer} doesn't exist in the file {path}"),
            )
            .into())
        } else {
            Ok((PathBuf::from(path), layer.to_string()))
        }
    } else {
        let data = Dataset::open(arg)?;
        if data.layer_count() == 1 {
            let layer = data.layer(0)?;
            Ok((PathBuf::from(&arg), layer.name()))
        } else {
            eprintln!("Provide a layer name to choose layer \"FILENAME:LAYERNAME\"");
            eprintln!("Available Layers:");
            data.layers().for_each(|l| eprintln!("  {}", l.name()));
            let layer = data.layer(0)?;
            Ok((PathBuf::from(&arg), layer.name()))
        }
    }
}

pub fn get_geometries(
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

pub fn gdal_update_or_create<P: AsRef<Path>>(
    filepath: P,
    driver: Option<String>,
    overwrite: bool,
) -> anyhow::Result<Dataset> {
    if !overwrite && filepath.as_ref().exists() {
        let open_flags = gdal::GdalOpenFlags::GDAL_OF_UPDATE;
        let op = gdal::DatasetOptions {
            open_flags,
            ..Default::default()
        };
        Ok(Dataset::open_ex(filepath, op)?)
    } else {
        let driver = if let Some(d) = &driver {
            DriverManager::get_driver_by_name(d)?
        } else {
            DriverManager::get_output_driver_for_dataset_name(&filepath, gdal::DriverType::Vector)
                .context("Driver not found for the output filename")?
        };

        Ok(driver.create_vector_only(filepath)?)
    }
}
