use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod nadi_gis {
    use chrono::Datelike;
    use gdal::vector::{FieldValue, Geometry, LayerAccess, LayerOptions, OGRFieldType};
    use gdal::{Dataset, DriverManager, DriverType};
    use nadi_core::abi_stable::std_types::{RSome, RString};
    use nadi_core::anyhow::{Context, Result};
    use nadi_core::attrs::{Date, DateTime, FromAttribute, FromAttributeRelaxed, HasAttributes};
    use nadi_core::nadi_plugin::network_func;
    use nadi_core::prelude::*;
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;

    /// Load node attributes from a GIS file
    #[network_func(geometry = "GEOM", ignore = "", sanitize = true, err_no_node = false)]
    fn gis_load_attrs(
        net: &mut Network,
        file: PathBuf,
        node: String,
        layer: Option<String>,
        geometry: String,
        ignore: String,
        sanitize: bool,
        err_no_node: bool,
    ) -> Result<()> {
        let data = Dataset::open(file)?;
        let mut lyr = if let Some(lyr) = layer {
            data.layer_by_name(&lyr)
                .context("Given Layer doesn't exist")?
        } else {
            if data.layer_count() > 1 {
                eprintln!("WARN Multiple layers found, you can choose a specific layer");
                eprint!("WARN Available Layers:");
                data.layers().for_each(|l| eprint!(" {:?}", l.name()));
                eprintln!();
            }
            data.layer(0)?
        };

        let ignore: HashSet<String> = ignore.split(',').map(String::from).collect();

        for f in lyr.features() {
            let name = f.field_as_string_by_name(&node)?.unwrap_or("".to_string());
            let n = match net.node_by_name(&name) {
                Some(n) => n,
                None if err_no_node => {
                    return Err(nadi_core::anyhow::Error::msg(format!(
                        "Node {name} not found"
                    )))
                }
                None => continue,
            };
            if let Some(g) = f.geometry().and_then(|g| g.wkt().ok()) {
                n.lock().set_attr(&geometry, Attribute::String(g.into()));
            }
            let attrs = f
                .fields()
                .filter(|(f, _)| !ignore.contains(f))
                .filter_map(|(f, v)| {
                    let f = if sanitize { sanitize_key(&f) } else { f };
                    let f = RString::from(f);
                    if let Some(val) = v {
                        match val {
                            FieldValue::IntegerValue(i) => Some((f, Attribute::Integer(i as i64))),
                            FieldValue::Integer64Value(i) => Some((f, Attribute::Integer(i))),
                            FieldValue::StringValue(i) => {
                                Some((f, Attribute::String(RString::from(i))))
                            }
                            FieldValue::RealValue(i) => Some((f, Attribute::Float(i))),
                            FieldValue::DateValue(d) => Some((
                                f,
                                Attribute::Date(Date::new(
                                    d.year() as u16,
                                    d.month() as u8,
                                    d.day() as u8,
                                )),
                            )),
                            _ => None,
                        }
                    } else {
                        None
                    }
                });
            n.lock().attr_map_mut().extend(attrs);
        }
        Ok(())
    }

    /// Save GIS file of the connections
    #[network_func(layer = "network")]
    fn gis_save_connections(
        net: &mut Network,
        file: PathBuf,
        geometry: String,
        driver: Option<String>,
        layer: String,
    ) -> Result<()> {
        let driver = if let Some(d) = driver {
            gdal::DriverManager::get_driver_by_name(&d)?
        } else {
            DriverManager::get_output_driver_for_dataset_name(&file, DriverType::Vector)
                .context("Could not detect Driver for filename, try providing `driver` argument.")?
        };

        // TODO if file already exists add the layer if possible
        let mut out_data = driver.create_vector_only(&file)?;
        let mut layer = out_data.create_layer(LayerOptions {
            name: &layer,
            ty: gdal_sys::OGRwkbGeometryType::wkbLineString,
            ..Default::default()
        })?;
        layer.create_defn_fields(&[
            ("start", OGRFieldType::OFTString),
            ("end", OGRFieldType::OFTString),
        ])?;
        let fields = ["start", "end"];
        for node in net.nodes() {
            let n = node.lock();
            if let RSome(out) = n.output() {
                let start = String::try_from_attr(
                    n.attr(&geometry)
                        .context("Attribute for geometry not found")?,
                )
                .map_err(nadi_core::anyhow::Error::msg)?;
                let end = String::try_from_attr(
                    out.lock()
                        .attr(&geometry)
                        .context("Attribute for geometry not found")?,
                )
                .map_err(nadi_core::anyhow::Error::msg)?;
                let start = Geometry::from_wkt(&start)?;
                let end = Geometry::from_wkt(&end)?;

                let mut edge_geometry =
                    Geometry::empty(gdal_sys::OGRwkbGeometryType::wkbLineString)?;
                // add all points from start, (so it can be linestring
                // instead of just point); and add end's first point
                // only if it's different from last point of start
                edge_geometry.add_point(start.get_point(0));
                edge_geometry.add_point(end.get_point(0));
                layer.create_feature_fields(
                    edge_geometry,
                    &fields,
                    &[
                        FieldValue::StringValue(n.name().to_string()),
                        FieldValue::StringValue(out.lock().name().to_string()),
                    ],
                )?;
            }
        }
        Ok(())
    }

    /// Save GIS file of the nodes
    #[network_func(attrs=HashMap::new(), layer="nodes")]
    fn gis_save_nodes(
        net: &mut Network,
        file: PathBuf,
        geometry: String,
        attrs: HashMap<String, String>,
        driver: Option<String>,
        layer: String,
    ) -> Result<()> {
        let driver = if let Some(d) = driver {
            gdal::DriverManager::get_driver_by_name(&d)?
        } else {
            DriverManager::get_output_driver_for_dataset_name(&file, DriverType::Vector)
                .context("Could not detect Driver for filename, try providing `driver` argument.")?
        };

        // TODO if file already exists add the layer if possible
        let mut out_data = driver.create_vector_only(&file)?;
        let mut layer = out_data.create_layer(LayerOptions {
            name: &layer,
            ty: gdal_sys::OGRwkbGeometryType::wkbPoint,
            ..Default::default()
        })?;
        let fields: Vec<(String, (u32, Attr2FieldValue))> = attrs
            .into_iter()
            .map(|(k, v)| (k, type_name_to_field(&v)))
            .collect();
        let field_types: Vec<(&str, u32)> = fields.iter().map(|(k, v)| (k.as_str(), v.0)).collect();
        // saving shp means field names will be shortened, it'll error later, how do we fix it?
        layer.create_defn_fields(&field_types)?;
        for node in net.nodes() {
            let n = node.lock();
            let node_geom = String::try_from_attr(
                n.attr(&geometry)
                    .context("Attribute for geometry not found")?,
            )
            .map_err(nadi_core::anyhow::Error::msg)?;
            let node_geom = Geometry::from_wkt(&node_geom)?;
            let feat_fields: Vec<(&str, FieldValue)> = fields
                .iter()
                .filter_map(|(k, (_, func))| Some((k.as_str(), func(n.attr(k)?))))
                .collect();

            let field_names: Vec<&str> = feat_fields.iter().map(|(k, _)| *k).collect();
            let field_vals: Vec<FieldValue> = feat_fields.into_iter().map(|(_, v)| v).collect();
            layer.create_feature_fields(node_geom, &field_names, &field_vals)?;
        }
        Ok(())
    }

    fn sanitize_key(k: &str) -> String {
        k.replace(' ', "_")
    }

    type Attr2FieldValue = fn(&Attribute) -> FieldValue;

    fn type_name_to_field(name: &str) -> (u32, Attr2FieldValue) {
        match name {
            // This is a string that can be parsed back into correct Attribute
            "Attribute" => (OGRFieldType::OFTString, |a| {
                FieldValue::StringValue(a.to_string())
            }),
            "String" => (OGRFieldType::OFTString, |a| {
                let val: String = FromAttributeRelaxed::from_attr_relaxed(a).unwrap_or_default();
                FieldValue::StringValue(val)
            }),
            "Integer" => (OGRFieldType::OFTInteger64, |a| {
                let val: i64 = FromAttributeRelaxed::from_attr_relaxed(a).unwrap_or_default();
                FieldValue::Integer64Value(val)
            }),
            "Float" => (OGRFieldType::OFTReal, |a| {
                let val: f64 = FromAttributeRelaxed::from_attr_relaxed(a).unwrap_or_default();
                FieldValue::RealValue(val)
            }),
            "Date" => (OGRFieldType::OFTDate, |a| {
                let val: Date = FromAttributeRelaxed::from_attr_relaxed(a).unwrap_or_default();
                FieldValue::DateValue(val.into())
            }),
            // // There is no FieldValue::TimeValue
            // "Time" => (OGRFieldType::OFTTime, |a| {
            //     let val: Time = FromAttributeRelaxed::from_attr_relaxed(a).unwrap_or_default();
            //     FieldValue::TimeValue(val.into())
            // }),
            "DateTime" => (OGRFieldType::OFTDateTime, |a| {
                let val: DateTime = FromAttributeRelaxed::from_attr_relaxed(a).unwrap_or_default();
                FieldValue::DateTimeValue(val.into())
            }),
            // There are other types supported by gdal, that could exist as Attribute, but let's ignore them
            _ => panic!("Not supported. Use String, Integer, Float, Date, DateTime or Attribute"),
        }
    }
}
