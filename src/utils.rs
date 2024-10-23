use std::collections::HashSet;
use std::path::PathBuf;

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
