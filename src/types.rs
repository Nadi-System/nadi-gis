use anyhow::Context;
use ordered_float::NotNan;
use std::collections::HashMap;

pub struct Streams(pub HashMap<Point2D, Point2D>);

pub struct Points(pub HashMap<String, Point2D>);

#[derive(Eq, PartialEq, Hash, Clone, Debug)]
pub struct Point2D {
    x: NotNan<f64>,
    y: NotNan<f64>,
}

impl Point2D {
    pub fn new2(coord: (f64, f64)) -> anyhow::Result<Self> {
        Ok(Self {
            x: NotNan::new(coord.0).context("GIS Coordinate shouldn't be NaN")?,
            y: NotNan::new(coord.1).context("GIS Coordinate shouldn't be NaN")?,
        })
    }

    pub fn new3(coord: (f64, f64, f64)) -> anyhow::Result<Self> {
        Ok(Self {
            x: NotNan::new(coord.0).context("GIS Coordinate shouldn't be NaN")?,
            y: NotNan::new(coord.1).context("GIS Coordinate shouldn't be NaN")?,
        })
    }

    pub fn coord3(&self) -> (f64, f64, f64) {
        (self.x.into_inner(), self.y.into_inner(), 0.0)
    }

    pub fn coord2(&self) -> (f64, f64) {
        (self.x.into_inner(), self.y.into_inner())
    }
}

impl std::fmt::Display for Point2D {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.x, self.y)
    }
}
