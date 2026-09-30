# warren-arrow

`warren-arrow` writes tables to Parquet as a layout file lays them out. Each table is cut into
pieces, each piece holds its rows in the key's order, and a certificate beside them shows that the
rows written are the rows it was given.

```sh
warren-arrow write --layout shop.json --table buy=buy.parquet --table sets=sets.parquet --into out/
```

The layout file is JSON; [`LAYOUT.md`](LAYOUT.md) says exactly what `warren-arrow` reads and what it
refuses. A table the layout cannot hold exactly is refused with the reason, never rounded, cast
loosely or written in part.

## What it writes

For each table named on the command line, `out/<table>/` holds:

| file | what it holds |
|---|---|
| `p1/rows.parquet`, `p2/rows.parquet`, … | the rows, one file per piece, in the key's order |
| `pieces.parquet` | each piece's name, the least value of the range column it holds and the least value of the next piece's, its rows and its row groups |
| `certificate.parquet` | the row count and two sums of a hash of each row, once for the rows read and once for the rows read back from the pieces |

- The rows are ordered by the key's levels, then by the primary key, then by every other column in
  the layout's order, so no two different rows tie. The same rows given in any order are written as
  the same bytes by the same version of `warren-arrow`.
- NULL comes after every value, and text is ordered by its bytes.
- Every row group carries statistics for every column, and every file carries a page index. The
  key's leading levels, up to the first computed one, are recorded as the file's sorting columns.
- The pieces are written under `out/.<table>.partial/`, read back and certified, and only then
  renamed to `out/<table>/`. A certificate that does not match leaves the partial directory for
  inspection and makes no table directory. An existing table directory is never written over.

A source is read whole into memory. The row group size, compression (Snappy) and the length that
text statistics are truncated to are the parquet crate's defaults, because the layout file does
not name them.

## Points on the globe

Built with `--features geo`, a layout can hold a point as a `geography` column, written as a
Parquet GEOGRAPHY column (WKB, spherical edges, OGC:CRS84), and a key can order the rows by the
point's cell: the latitude band and longitude sector the point lies in, on a grid the layout
names. Longitude is read round the globe, so 180 and -180 are one meridian, and a point at either
pole lies in the end band. Each row group's box is the shortest stretch of longitude that holds
its points, so a row group of points on both sides of the antimeridian gets one narrow box that
crosses it. The cell orders the rows and is never written as a column. Built without the feature,
a layout with a `geography` column or a cell level is refused.

## Building and testing

```sh
cargo test
cargo test --features geo
cargo clippy --all-targets --all-features -- -D warnings
```

## Licence

Apache License 2.0; see [`LICENSE`](LICENSE).
