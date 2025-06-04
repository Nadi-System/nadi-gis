use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod gis {
    use chrono::Datelike;
    use gdal::vector::{
        Defn, Feature, FieldValue, Geometry, LayerAccess, LayerOptions, OGRFieldType,
    };
    use gdal::{Dataset, DriverManager, DriverType};
    use nadi_core::abi_stable::std_types::{RSome, RString};
    use nadi_core::anyhow::{Context, Result};
    use nadi_core::attrs::{Date, DateTime, FromAttribute, FromAttributeRelaxed, HasAttributes};
    use nadi_core::nadi_plugin::network_func;
    use nadi_core::prelude::*;
    use std::collections::{HashMap, HashSet};
    use std::path::PathBuf;

    /// Load network from a GIS file
    ///
    /// Loads the network from a gis file containing the edges in fields
    #[network_func(ignore_null = false)]
    fn gis_load_network(
        net: &mut Network,
        /// GIS file to load (can be any format GDAL can understand)
        file: PathBuf,
        /// Field in the GIS file corresponding to the input node name
        source: String,
        /// layer of the GIS file corresponding to the output node name
        destination: String,
        /// layer of the GIS file, first one picked by default
        layer: Option<String>,
        /// Ignore feature if it has fields with null value
        ignore_null: bool,
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

        let defn = Defn::from_layer(&lyr);
        let fid_s = defn.field_index(&source)?;
        let fid_d = defn.field_index(&destination)?;
        let mut edges = Vec::with_capacity(lyr.feature_count() as usize);
        for f in lyr.features() {
            let inp_name = match f.field_as_string(fid_s)? {
                Some(n) => n,
                None if ignore_null => continue,
                None => return Err(nadi_core::anyhow::Error::msg("Null value on source field")),
            };
            let out_name = match f.field_as_string(fid_d)? {
                Some(n) => n,
                None if ignore_null => continue,
                None => return Err(nadi_core::anyhow::Error::msg("Null value on source field")),
            };
            edges.push((inp_name, out_name));
        }
        let edges_str: Vec<_> = edges
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        *net = Network::from_edges(&edges_str).map_err(nadi_core::anyhow::Error::msg)?;
        Ok(())
    }

    /// Load node attributes from a GIS file
    ///
    /// The function reads a GIS file in any format (CSV, GPKG, SHP,
    /// JSON, etc) and loads their fields as attributes to the nodes.
    #[network_func(geometry = "GEOM", ignore = "", sanitize = true, err_no_node = false)]
    fn gis_load_attrs(
        net: &mut Network,
        /// GIS file to load (can be any format GDAL can understand)
        file: PathBuf,
        /// Field in the GIS file corresponding to node name
        node: String,
        /// layer of the GIS file, first one picked by default
        layer: Option<String>,
        /// Attribute to save the GIS geometry in
        geometry: String,
        /// Field names separated by comma, to ignore
        ignore: String,
        /// sanitize the name of the fields
        sanitize: bool,
        /// Error if all nodes are not found in the GIS file
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

        let defn = Defn::from_layer(&lyr);
        let fid = defn.field_index(&node)?;
        for f in lyr.features() {
            let name = f.field_as_string(fid)?.unwrap_or("".to_string());
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
        net: &Network,
        file: PathBuf,
        geometry: String,
        driver: Option<String>,
        layer: String,
        filter: Option<Vec<bool>>,
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
        let defn = Defn::from_layer(&layer);
        let nodes: Vec<&Node> = if let Some(filt) = filter {
            net.nodes()
                .zip(filt)
                .filter(|(_, f)| *f)
                .map(|n| n.0)
                .collect()
        } else {
            net.nodes().collect()
        };
        for node in nodes {
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
                let mut ft = Feature::new(&defn)?;
                ft.set_geometry(edge_geometry)?;
                ft.set_field_string(0, n.name())?;
                ft.set_field_string(1, out.lock().name())?;
                ft.create(&mut layer)?;
            }
        }
        Ok(())
    }

    /// Save GIS file of the nodes
    #[network_func(attrs=HashMap::new(), layer="nodes")]
    fn gis_save_nodes(
        net: &Network,
        file: PathBuf,
        geometry: String,
        attrs: HashMap<String, String>,
        driver: Option<String>,
        layer: String,
        filter: Option<Vec<bool>>,
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
            .map(|(k, v)| Ok((k, type_name_to_field(&v)?)))
            .collect::<Result<_, String>>()
            .map_err(nadi_core::anyhow::Error::msg)?;
        let field_types: Vec<(&str, u32)> = fields.iter().map(|(k, v)| (k.as_str(), v.0)).collect();
        // saving shp means field names will be shortened, it'll error later, how do we fix it?
        layer.create_defn_fields(&field_types)?;
        let defn = Defn::from_layer(&layer);
        let indices: HashMap<&str, usize> = fields
            .iter()
            .filter_map(|f| Some((f.0.as_str(), defn.field_index(&f.0).ok()?)))
            .collect();
        let nodes: Vec<&Node> = if let Some(filt) = filter {
            net.nodes()
                .zip(filt)
                .filter(|(_, f)| *f)
                .map(|n| n.0)
                .collect()
        } else {
            net.nodes().collect()
        };
        for node in nodes {
            let n = node.lock();
            let node_geom = String::try_from_attr(
                n.attr(&geometry)
                    .context("Attribute for geometry not found")?,
            )
            .map_err(nadi_core::anyhow::Error::msg)?;
            let node_geom = Geometry::from_wkt(&node_geom)?;
            let mut ft = Feature::new(&defn)?;
            ft.set_geometry(node_geom)?;
            fields
                .iter()
                .filter_map(|(k, (_, func))| Some((k.as_str(), func(n.attr(k)?))))
                .try_for_each(|(k, v)| ft.set_field(indices[k], &v))?;
            ft.create(&mut layer)?;
        }
        Ok(())
    }

    fn sanitize_key(k: &str) -> String {
        k.replace(' ', "_")
    }

    type Attr2FieldValue = fn(&Attribute) -> FieldValue;

    fn type_name_to_field(name: &str) -> Result<(u32, Attr2FieldValue), String> {
        Ok(match name {
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
            t => {
                return Err(format!(
                "Type {t} Not supported. Use String, Integer, Float, Date, DateTime or Attribute"
            ))
            }
        })
    }
}
