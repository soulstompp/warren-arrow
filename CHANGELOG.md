# Changelog

## 0.1.0

The first release.

- `write` puts a layout's tables in Parquet in key order, and certifies each against its source.
- A month, year or weekday level of the key is written as its own column, a row group per value.
- `kept`, `boxes`, `runs` and `index` count what a reader keeps and skips, from the metadata alone.
- With `--features geo`, points are ordered by their cell, and a row group's box wraps the
  antimeridian the short way round.
