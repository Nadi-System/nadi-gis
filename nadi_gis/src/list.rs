use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use colored::Colorize;
use gdal::vector::{FieldValue, Layer, LayerAccess};
use gdal::Dataset;
use string_template_plus::{Render, RenderOptions, Template};
use text_diff::Difference;
use toml::{Table, Value};

use crate::cliargs::CliAction;

#[derive(Args)]
pub struct CliArgs {
    /// Ignore these Fields
    #[arg(short, long, value_delimiter = ',')]
    ignore: Vec<String>,
    /// key separator
    #[arg(short, long, default_value = "::")]
    key_sep: String,
    /// variable and value separator
    #[arg(short = 'V', long, default_value = "=")]
    var_sep: String,
    /// Fields to use as id for file
    #[arg(short, long)]
    primary_key: Option<String>,
    /// sanitize key identifiers (replace space with _)
    #[arg(short, long)]
    sanitize: bool,
    /// Print the variables that have been changed
    #[arg(short, long)]
    verbose: bool,
    /// Output TOML file Template
    #[arg(short, long)]
    output: Option<String>,
    /// GIS file with points of interest
    #[arg(value_parser=parse_layer, value_name="POINTS_FILE[:LAYER]")]
    file: (PathBuf, String),
}

fn parse_layer(arg: &str) -> Result<(PathBuf, String), anyhow::Error> {
    if let Some((path, layer)) = arg.split_once(':') {
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

impl CliAction for CliArgs {
    fn run(self) -> Result<(), anyhow::Error> {
        let file_data = Dataset::open(&self.file.0).unwrap();
        let file = file_data.layer_by_name(&self.file.1).unwrap();
        if let Some(out) = &self.output {
            let templ = Template::parse_template(out)?;
            self.update_attrs(file, templ)?;
        } else {
            self.print_attrs(file, &self.primary_key)?;
        }
        Ok(())
    }
}

impl CliArgs {
    fn update_attrs(&self, mut lyr: Layer, templ: Template) -> Result<(), anyhow::Error> {
        let vars: Vec<_> = templ
            .parts()
            .iter()
            .map(|p| p.variables())
            .flatten()
            .collect();
        let mut op = RenderOptions::default();

        for (i, f) in lyr.features().enumerate() {
            let name = if let Some(name) = &self.primary_key {
                f.field_as_string_by_name(name)?.unwrap_or("".to_string())
            } else {
                i.to_string()
            };
            for v in &vars {
                op.variables.insert(
                    v.to_string(),
                    f.field_as_string_by_name(v)?.unwrap_or("".to_string()),
                );
            }

            let filename = templ.render(&op)?;

            let path: &Path = filename.as_ref();
            let mut attrs: NodeAttrs = if path.exists() {
                let contents = std::fs::read_to_string(&filename).context(format!("{path:?}"))?;
                toml::from_str(&contents).context(format!("{path:?}"))?
            } else {
                NodeAttrs::default()
            };
            let mut new_attrs: Table = f
                .fields()
                .filter(|(f, _)| !self.ignore.contains(f))
                .filter_map(|(f, v)| {
                    let f = if self.sanitize { sanitize_key(&f) } else { f };
                    if let Some(val) = v {
                        match val {
                            FieldValue::IntegerValue(i) => Some((f, Value::Integer(i as i64))),
                            FieldValue::Integer64Value(i) => Some((f, Value::Integer(i))),
                            FieldValue::StringValue(i) => Some((f, Value::String(i))),
                            FieldValue::RealValue(i) => Some((f, Value::Float(i))),
                            FieldValue::DateValue(i) => Some((f, Value::String(i.to_string()))),
                            _ => None,
                        }
                    } else {
                        None
                    }
                })
                .collect();
            if let Some(g) = f.geometry() {
                if let Ok(w) = g.wkt() {
                    new_attrs.insert("geometry_wkt".to_string(), Value::String(w));
                }
            }
            if self.verbose {
                for (k, n) in new_attrs {
                    match attrs.attr(&k) {
                        Some(v) => {
                            // temporarily for the Year Completed, take the minimum year on overlap
                            let n = match k.as_str() {
                                "Year_Completed" => {
                                    let nyr = match n {
                                        Value::Integer(i) => i,
                                        Value::Float(i) => i.floor() as i64,
                                        Value::String(s) => s.parse().unwrap(),
                                        _ => panic!("Not year"),
                                    };
                                    let oyr = match &v {
                                        &Value::Integer(i) => i,
                                        &Value::Float(i) => i.floor() as i64,
                                        &Value::String(ref s) => s.parse().unwrap(),
                                        _ => panic!("Not year"),
                                    };
                                    Value::Integer(if nyr < oyr { nyr } else { oyr })
                                }
                                _ => n,
                            };
                            if v != n {
                                self.print_changed(&name, &k, &v, &n);
                                attrs.set_attr(k, n)
                            }
                        }
                        None => {
                            self.print_new_attr(&name, &k, &n);
                            attrs.set_attr(k, n);
                        }
                    }
                }
            } else {
                attrs.extend(new_attrs);
            }
            std::fs::write(&filename, toml::to_string(&attrs)?)?;
        }

        Ok(())
    }

    fn print_attrs(&self, mut lyr: Layer, field: &Option<String>) -> Result<(), anyhow::Error> {
        for (i, f) in lyr.features().enumerate() {
            let name = if let Some(name) = field {
                f.field_as_string_by_name(name)?.unwrap_or("".to_string())
            } else {
                i.to_string()
            };
            f.fields()
                .filter(|(f, _)| !self.ignore.contains(f))
                .for_each(|(s, v)| {
                    let s = if self.sanitize { sanitize_key(&s) } else { s };
                    if let Some(val) = v {
                        match val {
                            FieldValue::Integer64Value(i) => self.print_new_attr(&name, &s, i),
                            FieldValue::StringValue(i) => {
                                self.print_new_attr(&name, &s, Value::String(i))
                            }
                            FieldValue::RealValue(i) => self.print_new_attr(&name, &s, i),
                            FieldValue::DateValue(i) => self.print_new_attr(&name, &s, i),
                            _ => (),
                        }
                    }
                });
            if let Some(g) = f.geometry() {
                if let Ok(w) = g.wkt() {
                    self.print_new_attr(&name, "geometry_wkt", Value::String(w));
                }
            }
        }
        Ok(())
    }

    fn print_changed(&self, name: &str, key: &str, val1: &Value, val2: &Value) {
        print!(">{name}{}{key}{}", self.key_sep, self.var_sep);
        match (val1, val2) {
            (&Value::String(ref o), &Value::String(ref n)) => {
                let (_, diffs) = text_diff::diff(o, n, " ");
                for d in diffs {
                    match d {
                        Difference::Same(v) => print!("{} ", v),
                        Difference::Rem(v) => print!("{} ", v.red().strikethrough()),
                        Difference::Add(v) => print!("{} ", v.green()),
                    }
                }
                println!();
            }
            _ => {
                println!(
                    "{} {}",
                    val1.to_string().red().strikethrough(),
                    val2.to_string().green()
                )
            }
        }
    }

    fn print_new_attr<T: ToString>(&self, name: &str, key: &str, val: T) {
        println!(
            "{name}{}{}{}{}",
            self.key_sep,
            key.green(),
            self.var_sep,
            val.to_string().green()
        );
    }
}

fn sanitize_key(k: &str) -> String {
    k.replace(' ', "_")
}
