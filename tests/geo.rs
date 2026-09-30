//! The geo cell as a key level: it wraps round the antimeridian, meets at the poles and covers the
//! globe once; rows are written in cell order; and each row group's box holds its points, crossing
//! the antimeridian where that is the shorter way round.

#![cfg(feature = "geo")]

use std::fs::File;
use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryArray, Float64Array, Int64Array, RecordBatch, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use parquet::file::reader::{FileReader, SerializedFileReader};
use warren_arrow::geo::{Grid, arc};
use warren_arrow::value::point;
use warren_arrow::write::ROWS_FILE;
use warren_arrow::{Error, Layout, Options, Written, write_table};

const LAYOUT: &str = r#"{
  "format": 2,
  "name": "places",
  "tables": [{
    "name": "place",
    "columns": [
      {"name": "place_id", "type": "bigint", "null": false},
      {"name": "place", "type": "geography", "null": false}
    ],
    "primary_key": ["place_id"],
    "keys": [{"name": "place_key", "levels": [{"cell": "place", "band": 10, "sector": 10}]}]
  }]
}"#;

fn grid() -> Grid {
    Grid::new(10.0, 10.0).unwrap()
}

/// A two-dimensional WKB point, in either byte order.
fn wkb(lon: f64, lat: f64, little: bool) -> Vec<u8> {
    let mut b = vec![u8::from(little)];
    if little {
        b.extend_from_slice(&1u32.to_le_bytes());
        b.extend_from_slice(&lon.to_le_bytes());
        b.extend_from_slice(&lat.to_le_bytes());
    } else {
        b.extend_from_slice(&1u32.to_be_bytes());
        b.extend_from_slice(&lon.to_be_bytes());
        b.extend_from_slice(&lat.to_be_bytes());
    }
    b
}

/// Every band and sector edge and middle of a 10-degree grid, points just inside the edges, the
/// antimeridian from both sides, and both poles at many longitudes.
fn points() -> Vec<(f64, f64)> {
    let mut pts = Vec::new();
    for lat in (-90..=90).step_by(5) {
        for lon in (-180..=180).step_by(5) {
            pts.push((f64::from(lon), f64::from(lat)));
        }
    }
    for lat in [-89.999, -45.001, -0.001, 0.001, 44.999, 89.999] {
        for lon in [-179.999, -0.001, 0.001, 179.999] {
            pts.push((lon, lat));
        }
    }
    for lat in [-65.0, -5.0, 45.0, 55.0, 85.0] {
        for lon in [170.0, 175.0, 179.9, 180.0, -180.0, -179.9, -175.0, -170.0] {
            pts.push((lon, lat));
        }
    }
    for lon in [-180.0, -123.4, -90.0, 0.0, 37.5, 180.0] {
        pts.push((lon, 90.0));
        pts.push((lon, -90.0));
    }
    pts
}

fn table(pts: &[(f64, f64)]) -> RecordBatch {
    let n = pts.len() as u32;
    let order: Vec<u32> = (0..n).map(|i| (i * 7919 + 13) % n).collect();
    let schema = Schema::new(vec![
        Field::new("place_id", DataType::Int64, false),
        Field::new("place", DataType::Binary, false),
    ]);
    let wkbs: Vec<Vec<u8>> = pts.iter().enumerate().map(|(i, p)| wkb(p.0, p.1, i % 3 != 0)).collect();
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from_iter_values(0..i64::from(n))),
            Arc::new(BinaryArray::from_iter_values(wkbs.iter())),
        ],
    )
    .unwrap();
    let idx = UInt32Array::from(order);
    let columns = batch.columns().iter().map(|c| take(c.as_ref(), &idx, None).unwrap()).collect();
    RecordBatch::try_new(batch.schema(), columns).unwrap()
}

#[test]
fn a_longitude_and_the_same_longitude_a_turn_away_share_a_cell() {
    for (band, sector) in [(10.0, 10.0), (30.0, 45.0), (1.0, 1.0), (0.5, 0.25)] {
        let g = Grid::new(band, sector).unwrap();
        assert_eq!(g.sector_of(180.0), g.sector_of(-180.0), "180 and -180 are one meridian");
        for (lon, lat) in points() {
            let c = g.cell(lon, lat).unwrap();
            assert_eq!(g.cell(lon + 360.0, lat).unwrap(), c, "({lon}, {lat}) and a turn east");
            assert_eq!(g.cell(lon - 360.0, lat).unwrap(), c, "({lon}, {lat}) and a turn west");
        }
    }
}

/// Whether a point lies in a cell's edges: south inclusive, north exclusive except the north end
/// band, which holds the pole; west inclusive, east exclusive, with longitude read from -180.
fn inside(g: &Grid, cell: i64, lon: f64, lat: f64) -> bool {
    let (south, north, west, east) = g.edges(cell);
    let lon = (lon + 180.0).rem_euclid(360.0) - 180.0;
    let top = cell / g.sectors() == g.bands() - 1;
    south <= lat && (lat < north || (top && lat <= north)) && west <= lon && lon < east
}

#[test]
fn every_point_lies_in_exactly_one_cell_and_the_cells_cover_the_globe() {
    let g = Grid::new(30.0, 45.0).unwrap();
    let cells = g.bands() * g.sectors();
    let mut reached = vec![false; cells as usize];
    let pts: Vec<(f64, f64)> = points()
        .into_iter()
        .chain((-90..=90).step_by(15).flat_map(|lat| {
            (-180..=180).step_by(15).map(move |lon| (f64::from(lon) + 7.5, f64::from(lat)))
        }))
        .collect();
    for (lon, lat) in pts {
        let c = g.cell(lon, lat).unwrap();
        assert!((0..cells).contains(&c), "({lon}, {lat}) is given cell {c}");
        assert!(inside(&g, c, lon, lat), "({lon}, {lat}) lies outside its cell {c}");
        let holding = (0..cells).filter(|k| inside(&g, *k, lon, lat)).count();
        assert_eq!(holding, 1, "({lon}, {lat}) lies in {holding} cells");
        reached[c as usize] = true;
    }
    assert!(reached.iter().all(|r| *r), "some cell holds none of the points");
}

#[test]
fn a_point_at_a_pole_lies_in_the_end_band_whatever_its_longitude() {
    let g = grid();
    for lon in [-540.0, -180.0, -123.4, 0.0, 45.0, 179.999, 180.0, 540.0] {
        assert_eq!(g.cell(lon, 90.0).unwrap() / g.sectors(), g.bands() - 1, "north pole at {lon}");
        assert_eq!(g.cell(lon, -90.0).unwrap() / g.sectors(), 0, "south pole at {lon}");
    }
    assert!(g.cell(0.0, 90.001).is_err() && g.cell(f64::NAN, 0.0).is_err());
}

/// One row group as written: its box as the file holds it, and its rows' points in file order.
struct Group {
    x: (f64, f64),
    y: (f64, f64),
    points: Vec<(f64, f64)>,
}

fn groups(w: &Written) -> Vec<Group> {
    let mut out = Vec::new();
    for p in &w.pieces {
        let file = w.dir.join(&p.name).join(ROWS_FILE);
        let rows = warren_arrow::read_source(&file).unwrap();
        let col = rows.column_by_name("place").unwrap();
        let pts: Vec<(f64, f64)> = (0..col.len()).map(|i| point(col.as_binary::<i32>().value(i)).unwrap()).collect();
        let reader = SerializedFileReader::new(File::open(&file).unwrap()).unwrap();
        let md = reader.metadata();
        let mut at = 0usize;
        for i in 0..md.num_row_groups() {
            let rg = md.row_group(i);
            let n = rg.num_rows() as usize;
            let bb = rg.column(1).geo_statistics().expect("a geography row group has a box");
            let bb = bb.bounding_box().expect("its box");
            out.push(Group {
                x: (bb.get_xmin(), bb.get_xmax()),
                y: (bb.get_ymin(), bb.get_ymax()),
                points: pts[at..at + n].to_vec(),
            });
            at += n;
        }
        assert_eq!(at, pts.len());
    }
    out
}

fn write(pts: &[(f64, f64)], opts: &Options) -> (tempfile::TempDir, Written) {
    let dir = tempfile::tempdir().unwrap();
    let w = write_table(&Layout::parse(LAYOUT).unwrap(), "place", &table(pts), dir.path(), opts).unwrap();
    assert_eq!(w.source, w.written);
    (dir, w)
}

/// Whether a longitude lies on a box's arc, which runs east from `x.0` to `x.1`.
fn on_arc(x: (f64, f64), lon: f64) -> bool {
    if x.0 <= x.1 {
        x.0 <= lon && lon <= x.1
    } else {
        lon >= x.0 || lon <= x.1
    }
}

/// The cells a box can hold: the bands between its latitudes, and the sectors its arc meets.
fn cells_within(g: &Grid, x: (f64, f64), y: (f64, f64)) -> impl Fn(i64) -> bool {
    let (b0, b1) = (g.band_of(y.0), g.band_of(y.1));
    let s = g.sectors();
    let width = 360.0 / s as f64;
    let from = (x.0 + 180.0).rem_euclid(360.0);
    let span = if x.0 <= x.1 { x.1 - x.0 } else { x.1 + 360.0 - x.0 };
    let steps = ((from + span) / width).floor() as i64 - (from / width).floor() as i64;
    let first = g.sector_of(x.0);
    let all = steps >= s - 1;
    move |cell| {
        let (band, sector) = (cell / s, cell % s);
        band >= b0 && band <= b1 && (all || (sector - first).rem_euclid(s) <= steps)
    }
}

#[test]
fn rows_are_written_in_cell_order_and_each_row_groups_box_holds_its_cells() {
    let g = grid();
    let (_dir, w) = write(&points(), &Options { max_row_group_rows: Some(3) });
    let gs = groups(&w);
    let cells: Vec<i64> = gs.iter().flat_map(|x| x.points.iter().map(|p| g.cell(p.0, p.1).unwrap())).collect();
    assert!(cells.windows(2).all(|p| p[0] <= p[1]), "rows are out of cell order");
    let mut narrow = 0;
    for x in &gs {
        let lats = x.points.iter().map(|p| p.1);
        let lons: Vec<f64> = x.points.iter().map(|p| p.0).collect();
        let computed = arc(&lons).unwrap();
        assert_eq!(x.x, computed, "the box read back is not the box computed");
        assert_eq!(x.y, (lats.clone().fold(f64::INFINITY, f64::min), lats.fold(f64::NEG_INFINITY, f64::max)));
        let within = cells_within(&g, x.x, x.y);
        for p in &x.points {
            assert!(on_arc(x.x, p.0) && x.y.0 <= p.1 && p.1 <= x.y.1, "{p:?} lies outside its box");
            let c = g.cell(p.0, p.1).unwrap();
            assert!(within(c), "cell {c} of {p:?} lies outside its row group's box {:?} {:?}", x.x, x.y);
        }
        narrow += usize::from((0..g.bands() * g.sectors()).filter(|c| within(*c)).count() < 10);
    }
    assert!(narrow > gs.len() / 2, "most row groups must reach only a few cells");
}

#[test]
fn a_row_group_across_the_antimeridian_gets_one_narrow_box_that_holds_it() {
    assert_eq!(arc(&[179.9, -179.9, 179.95]), Some((179.9, -179.9)));
    assert_eq!(arc(&[180.0, -180.0]), Some((180.0, -180.0)));
    assert_eq!(arc(&[-179.0, 0.0, 179.0]), Some((0.0, -179.0)), "181 degrees the long way is shorter than 358");

    let seam = [(179.9, 45.0), (-179.9, 45.0), (179.95, 46.0), (-179.95, 44.0)];
    let (_dir, w) = write(&seam, &Options::default());
    let gs = groups(&w);
    assert_eq!(gs.len(), 1);
    let x = &gs[0];
    assert!(x.x.0 > x.x.1, "the box {:?} does not cross the antimeridian", x.x);
    assert!(x.x.1 + 360.0 - x.x.0 < 1.0, "the box {:?} is not narrow", x.x);
    for p in &x.points {
        assert!(on_arc(x.x, p.0), "{p:?} lies outside the box {:?}", x.x);
    }
}

#[test]
fn a_box_never_crosses_the_antimeridian_where_the_plain_range_is_shorter() {
    assert_eq!(arc(&[-10.0, 0.0, 10.0]), Some((-10.0, 10.0)));
    assert_eq!(arc(&[-90.0, 90.0]), Some((-90.0, 90.0)), "a tie keeps the plain range");
    assert_eq!(arc(&[-60.0, 0.0, 60.0, 100.0]), Some((-60.0, 100.0)));
    assert_eq!(arc(&[5.0]), Some((5.0, 5.0)));

    let (_dir, w) = write(&[(-10.0, 1.0), (0.0, 2.0), (10.0, 3.0)], &Options::default());
    let gs = groups(&w);
    assert_eq!(gs[0].x, (-10.0, 10.0));
}

#[test]
fn a_cell_or_a_geography_the_writer_cannot_read_is_refused() {
    let why = |t: &str| match Layout::parse(t) {
        Err(Error::Refused(w)) => w,
        other => panic!("not refused: {other:?}"),
    };
    let with = |from: &str, to: &str| {
        let t = LAYOUT.replacen(from, to, 1);
        assert_ne!(t, LAYOUT, "the replacement must apply: {from}");
        t
    };
    assert!(why(&with(r#""band": 10"#, r#""band": 7"#)).contains("whole numbers of pieces"));
    assert!(why(&with(r#""sector": 10"#, r#""sector": 0"#)).contains("whole numbers of pieces"));
    assert!(why(&with(r#"{"cell": "place""#, r#"{"cell": "place_id""#)).contains("read from a geography column"));
    assert!(why(&with(r#"[{"cell": "place", "band": 10, "sector": 10}]"#, r#"["place"]"#)).contains("only through its cell"));
    assert!(why(&with(r#""primary_key": ["place_id"]"#, r#""primary_key": ["place"]"#)).contains("two spellings of one point"));
    assert!(
        why(&with(r#""primary_key": ["place_id"],"#, r#""primary_key": ["place_id"], "partitions": {"range": "place", "every": 5},"#))
            .contains("has no order of its own")
    );

    let refused = |batch: RecordBatch| {
        let dir = tempfile::tempdir().unwrap();
        match write_table(&Layout::parse(LAYOUT).unwrap(), "place", &batch, dir.path(), &Options::default()) {
            Err(Error::Refused(w)) => w,
            other => panic!("written: {other:?}"),
        }
    };
    let good = table(&[(1.0, 1.0), (2.0, 2.0)]);
    let with_places = |col: Arc<dyn Array>, dt: DataType| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("place_id", DataType::Int64, false),
                Field::new("place", dt, false),
            ])),
            vec![good.column(0).clone(), col],
        )
        .unwrap()
    };
    let far = BinaryArray::from_iter_values([wkb(1.0, 1.0, true), wkb(540.0, 1.0, true)]);
    assert!(refused(with_places(Arc::new(far), DataType::Binary)).contains("is not a place"));
    let line = BinaryArray::from_iter_values([wkb(1.0, 1.0, true), b"\x01\x02\x00\x00\x00".to_vec()]);
    assert!(refused(with_places(Arc::new(line), DataType::Binary)).contains("a two-dimensional WKB point is 21"));
    let floats = Float64Array::from(vec![1.0, 2.0]);
    assert!(refused(with_places(Arc::new(floats), DataType::Float64)).contains("which is not a geography"));
}
