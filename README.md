# warren-arrow

`warren-arrow` writes the tables of a layout file to Parquet, each in its key's order, and proves it
wrote exactly the rows it was given.

You hand it a table's rows and a small JSON file saying how the table is laid out: its columns, its
key and how it is cut into pieces. It writes the rows sorted by the key, gives every row group and
every page statistics a reader can use, and then reads everything back and checks it. Only when the
rows read back are the rows it was given does the table appear. A table the layout cannot hold
exactly is refused with the reason, never rounded, cast loosely or written in part.

```sh
warren-arrow write --layout shop.json --table buy=buy.parquet --into out/
```

```text
buy: 365 rows in 1 pieces and 12 row groups, the same rows as the source
  p1  from -  to -  365 rows  12 row groups
```

## A layout

```json
{
  "format": 2,
  "name": "shop",
  "tables": [{
    "name": "buy",
    "columns": [
      {"name": "buy_id", "type": "bigint", "null": false},
      {"name": "placed_at", "type": "instant", "null": false}
    ],
    "primary_key": ["buy_id"],
    "keys": [{"name": "k", "levels": [{"month": "placed_at"}, "placed_at"]}]
  }]
}
```

This one keeps each purchase in month order, then by the instant. [`LAYOUT.md`](LAYOUT.md) says
exactly what a layout may hold and what is refused.

## The proof: a certificate

Beside every table, `certificate.parquet` holds the row count and two sums of a hash of each row,
once for the rows it read and once for the rows it read back from the pieces it wrote. They must be
equal, or no table is written.

- The same rows in any order give the same certificate, so it certifies the rows, not the order they
  arrived in.
- A value changed in one row changes it, even when the row count stays the same.
- The same rows given in any order are also written as the same bytes by the same version of
  `warren-arrow`.

## Why the order matters: what a reader reads

A Parquet reader decides what to read from each file's metadata before it reads a row. It can skip
a whole piece (a file), a row group inside a file, or a page inside a row group, and whatever it
keeps, it reads:
- **a piece** is skipped whole when its name rules it out;
- **a row group** is skipped when a column's least and greatest values in it rule it out;
- **a page** is skipped when the page index's least and greatest values for it rule it out.

Written in the key's order, each row group and each page holds a narrow stretch of the key, so a
question's conditions rule more of them out. A month level of the key goes further: it is written as
a column of its own and cuts a row group at each month, so a question about December keeps one row
group in twelve without reading a row. `warren-arrow kept` counts these reads for a question.

## What it writes

For each table named on the command line, `out/<table>/` holds:

| file | what it holds |
|---|---|
| `p1/rows.parquet`, `p2/rows.parquet`, … | the rows, one file per piece, in the key's order |
| `pieces.parquet` | each piece's name, the value of the range column it starts at (none for the first) and the value the next piece starts at (none for the last), its rows and its row groups |
| `certificate.parquet` | the row count and two sums of a hash of each row, for the rows read and for the rows read back |

- The rows are ordered by the key's levels, then by the primary key, then by every other column in
  the layout's order, so no two different rows tie.
- NULL comes after every value, and text is ordered by its bytes.
- Every row group carries statistics for every column, and every file carries a page index. The
  key's leading levels, up to the first cell, are recorded as the file's sorting columns.
- A month, a year or a weekday level of the key is written as a column of its own
  (`<column>_month`, `<column>_year`, `<column>_weekday`), and a key led by one ends a row group
  at each of its values.
- The pieces are written under `out/.<table>.partial/`, read back and certified, and only then
  renamed to `out/<table>/`. A certificate that does not match leaves the partial directory for
  inspection and makes no table directory. An existing table directory is never written over.

A source is read whole into memory. The compression is Snappy. The row group size and the length
that text statistics are truncated to are the parquet crate's defaults, because the layout file
does not name them.

## Reading a written table back

Each command prints JSON to standard output, and each reads only what it says it reads.

```sh
warren-arrow kept  --layout shop.json --table buy --dir out/buy --month-in placed_at=12 [--check N]
warren-arrow boxes --layout shop.json --table buy --dir out/buy --part month:placed_at
warren-arrow runs  --layout shop.json --table buy --dir out/buy --part month:placed_at
warren-arrow index --layout shop.json --table buy --dir out/buy --column placed_at --out pages.parquet
```

- **`kept`** reads only the files' footers and page indexes, and counts what a reader keeps for a
  question: the pieces, the row groups with their rows and compressed bytes, and the pages with
  their rows, each beside the whole, and the footer and page-index bytes it read to decide.
  - The conditions: a month or a weekday of an instant (`--month-in`, `--weekday-in`); a span of
    days, the first included and the last not (`--within`); pairs of values of two integer columns
    listed in a Parquet file (`--pairs-in`); and a column's values listed in a Parquet file, whole
    numbers or texts (`--in`). All of them hold.
  - A month or a weekday is seen where the key writes it as a column, or where the table's pieces
    are named by it. A condition the files do not show keeps everything.
  - Pages are counted for the first condition seen on a column.
  - With `--check N` it also reads every row group, and fails unless the kept ones hold all N rows
    the conditions keep and the skipped ones hold none: a skip never loses a row.
- **`boxes`** gives each row group's bounds on a part as a reader sees them, beside the part's
  values its rows hold: how tightly each row group holds the part.
- **`runs`** gives, per piece, a part's runs in Arrow's run-end form against its distinct values.
- **`index`** writes every page of one column, its bounds and rows beside its row group's and its
  piece's footer, as a Parquet file, from the metadata alone.

## Points on the globe

Built with `--features geo`, a layout can hold a point as a `geography` column, written as a
Parquet GEOGRAPHY column (WKB, spherical edges, OGC:CRS84), and a key can order the rows by the
point's cell: the latitude band and longitude sector the point lies in, on a grid the layout
names. Longitude is read round the globe, so 180 and -180 are one meridian, and a point at either
pole lies in the end band. Each row group's box is the shortest stretch of longitude that holds
its points, so a row group of points on both sides of the antimeridian gets one narrow box that
crosses it. The cell orders the rows and is never written as a column. Built without the feature,
a layout with a `geography` column or a cell level is refused.

## As a library

The binary is a thin layer over the crate: `Layout::parse` reads a layout, `write_table` writes and
certifies one table, `read` holds the reading tools above, and `cli` parses a written table's
arguments and a question's conditions the way the binary does, for a binary of your own.

## Building and testing

Rust 1.88 or later.

```sh
cargo test
cargo test --features geo
cargo clippy --all-targets --all-features -- -D warnings
```

## Licence

Copyright 2026 Kenneth Allen Flegal. Apache License 2.0; see [`LICENSE`](LICENSE) and
[`NOTICE`](NOTICE).
