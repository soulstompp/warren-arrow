//! The geo cell of a point: a latitude band and a longitude sector, numbered
//! `band * sectors + sector`. Longitude is read round the circle, so 180 and -180 are one meridian
//! and a band's sectors run all the way round. Latitude runs from -90 to 90 and does not wrap; a
//! point at either pole lies in the end band, whatever its longitude.

/// A grid of latitude bands and longitude sectors that divide the globe evenly.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Grid {
    band: f64,
    sector: f64,
    bands: i64,
    sectors: i64,
}

impl Grid {
    /// A grid of `band_degrees`-wide bands and `sector_degrees`-wide sectors. Each must divide the
    /// globe into a whole number of pieces: 180 degrees of latitude, 360 of longitude.
    pub fn new(band_degrees: f64, sector_degrees: f64) -> Result<Grid, String> {
        let bands = 180.0 / band_degrees;
        let sectors = 360.0 / sector_degrees;
        let whole = |x: f64| x.is_finite() && x >= 1.0 && (x - x.round()).abs() < 1e-9;
        if !(whole(bands) && whole(sectors)) {
            return Err(format!(
                "a band of {band_degrees} degrees and a sector of {sector_degrees} degrees must divide 180 and 360 degrees into whole numbers of pieces"
            ));
        }
        Ok(Grid {
            band: band_degrees,
            sector: sector_degrees,
            bands: bands.round() as i64,
            sectors: sectors.round() as i64,
        })
    }

    pub fn bands(&self) -> i64 {
        self.bands
    }

    pub fn sectors(&self) -> i64 {
        self.sectors
    }

    /// The band a latitude lies in.
    pub fn band_of(&self, lat: f64) -> i64 {
        (((lat + 90.0) / self.band).floor() as i64).clamp(0, self.bands - 1)
    }

    /// The sector a longitude lies in, read round the circle.
    pub fn sector_of(&self, lon: f64) -> i64 {
        (((lon + 180.0).rem_euclid(360.0) / self.sector).floor() as i64).clamp(0, self.sectors - 1)
    }

    /// The cell a point lies in.
    pub fn cell(&self, lon: f64, lat: f64) -> Result<i64, String> {
        if !(lon.is_finite() && lat.is_finite() && (-90.0..=90.0).contains(&lat)) {
            return Err(format!(
                "({lon}, {lat}) is not a place: longitude must be finite and latitude within -90 to 90"
            ));
        }
        Ok(self.band_of(lat) * self.sectors + self.sector_of(lon))
    }

    /// The edges of a cell, in degrees: south, north, west and east. The west edge runs from -180.
    pub fn edges(&self, cell: i64) -> (f64, f64, f64, f64) {
        let (band, sector) = (cell / self.sectors, cell % self.sectors);
        let south = -90.0 + band as f64 * self.band;
        let west = -180.0 + sector as f64 * self.sector;
        (south, south + self.band, west, west + self.sector)
    }
}

use std::sync::{Arc, OnceLock};

use arrow::datatypes::Field;
use parquet::basic::LogicalType;
use parquet::geospatial::accumulator::{
    DefaultGeoStatsAccumulatorFactory, GeoStatsAccumulator, GeoStatsAccumulatorFactory,
    init_geo_stats_accumulator_factory,
};
use parquet::geospatial::bounding_box::BoundingBox;
use parquet::geospatial::statistics::GeospatialStatistics;
use parquet::schema::types::ColumnDescPtr;
use parquet_geospatial::{WkbEdges, WkbMetadata, WkbType};

use crate::Error;
use crate::value::point;

/// A field marked as a GEOGRAPHY column: WKB, spherical edges, and no CRS named, which is
/// OGC:CRS84.
pub fn geography_field(field: Field) -> Field {
    field.with_extension_type(WkbType::new(Some(WkbMetadata::new(None, Some(WkbEdges::Spherical)))))
}

/// The shortest arc of longitude holding every longitude given, as (west, east). West is greater
/// than east where the arc crosses the antimeridian, which it does only where that arc is shorter
/// than the one from the least longitude to the greatest.
pub fn arc(lons: &[f64]) -> Option<(f64, f64)> {
    let mut lons = lons.to_vec();
    lons.sort_by(f64::total_cmp);
    let (&least, &greatest) = (lons.first()?, lons.last()?);
    let (mut gap, mut after) = (0.0, 0);
    for i in 1..lons.len() {
        if lons[i] - lons[i - 1] > gap {
            gap = lons[i] - lons[i - 1];
            after = i;
        }
    }
    if 360.0 - gap < greatest - least {
        Some((lons[after], lons[after - 1]))
    } else {
        Some((least, greatest))
    }
}

/// The box of one row group's points: the shortest arc of their longitudes, and their least and
/// greatest latitudes.
#[derive(Debug, Default)]
struct Boxer {
    lons: Vec<f64>,
    lats: Option<(f64, f64)>,
    invalid: bool,
}

impl GeoStatsAccumulator for Boxer {
    fn is_valid(&self) -> bool {
        !self.invalid
    }

    fn update_wkb(&mut self, wkb: &[u8]) {
        match point(wkb) {
            Ok((lon, lat)) => {
                self.lons.push(lon);
                self.lats = Some(match self.lats {
                    Some((lo, hi)) => (lo.min(lat), hi.max(lat)),
                    None => (lat, lat),
                });
            }
            Err(_) => self.invalid = true,
        }
    }

    fn finish(&mut self) -> Option<Box<GeospatialStatistics>> {
        let done = std::mem::take(self);
        if done.invalid {
            return None;
        }
        let (west, east) = arc(&done.lons)?;
        let (south, north) = done.lats?;
        Some(Box::new(GeospatialStatistics::new(
            Some(BoundingBox::new(west, east, south, north)),
            Some(vec![1]),
        )))
    }
}

/// Boxes GEOGRAPHY columns with [`Boxer`], and leaves every other column to the parquet crate.
struct Factory;

impl GeoStatsAccumulatorFactory for Factory {
    fn new_accumulator(&self, descr: &ColumnDescPtr) -> Box<dyn GeoStatsAccumulator> {
        match descr.logical_type_ref() {
            Some(LogicalType::Geography { .. }) => Box::new(Boxer::default()),
            _ => DefaultGeoStatsAccumulatorFactory::default().new_accumulator(descr),
        }
    }
}

/// Makes the parquet crate box GEOGRAPHY columns this way, for the rest of the process. It can be
/// done only before the crate first writes a geospatial column.
pub fn install() -> Result<(), Error> {
    static INSTALLED: OnceLock<bool> = OnceLock::new();
    if *INSTALLED.get_or_init(|| init_geo_stats_accumulator_factory(Arc::new(Factory)).is_ok()) {
        Ok(())
    } else {
        Err(Error::Refused(
            "the program installed its own geospatial statistics first, so a row group's box would not be the one this writer computes"
                .into(),
        ))
    }
}
