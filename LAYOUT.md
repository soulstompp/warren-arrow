# The layout file

A layout file describes tables as JSON: each table's columns, its primary key, the one key its
rows are ordered by, and the column its pieces are cut on. `warren-arrow` reads format 2 of the
file. It writes what the file says, or refuses the file, or a table, with the reason. Nothing is
worked out beyond what the file says.

```json
{
  "format": 2,
  "name": "shop",
  "tables": [{
    "name": "buy",
    "columns": [
      {"name": "buy_id", "type": "bigint", "null": false},
      {"name": "set_num", "type": "varchar", "length": 16, "null": false},
      {"name": "root_id", "type": "integer", "null": false},
      {"name": "placed_at", "type": "instant", "null": false},
      {"name": "qty", "type": "integer"}
    ],
    "primary_key": ["buy_id"],
    "keys": [{"name": "buy_key", "levels": ["root_id", "set_num", {"month": "placed_at"}]}],
    "partitions": {"range": "root_id", "every": 1000000}
  }]
}
```

## The file

- `format` is 2. Any other format is refused.
- `name` is the layout's name. Every name in the file (the layout, its tables, columns and keys) is
  lower-case letters, digits and underscores, starts with a letter or an underscore, and has at
  most 63 characters.
- A field the format does not name is refused, as are `hints` and a table's `pieces`, `postgres`
  and `mysql`.

## A table

Only `name` and `columns` are required.

| field | what `warren-arrow` does with it |
|---|---|
| `columns` | the columns written, in this order |
| `primary_key` | checked unique on every row; with no key, the order the rows are written in |
| `keys` | at most one key, the order the rows are written in |
| `partitions` | the column the pieces are cut on |
| `decides` | refused: a value another table decides is read through the join, never written onto the row |
| `fold` | refused: a stored fold is a SELECT, and `warren-arrow` runs no SQL; write the fold's rows as a table of their own |

A second key is refused: the rows of a file are written in one order, and another order would be
another copy of the rows.

## Columns

A column is `{"name", "type", "length", "null"}`. It may hold NULL unless it says
`"null": false`, and then a NULL in the source is refused.

| type | written as | read from | checked |
|---|---|---|---|
| `smallint` | Int16 | any integer type | each value fits |
| `integer` | Int32 | any integer type | each value fits |
| `bigint` | Int64 | any integer type | each value fits |
| `varchar`, `char` (with `length`) | Utf8 | Utf8, LargeUtf8, Utf8View | at most `length` characters |
| `text` | Utf8 | Utf8, LargeUtf8, Utf8View | |
| `instant`, a point in time | Timestamp, microseconds, UTC | a timestamp in UTC (`UTC`, `+00:00`, `Etc/UTC` or `Z`), of any unit | whole microseconds |
| `date` | Date32 | Date32; Date64 of whole days | |
| `boolean` | Boolean | Boolean | |
| `geography`, with the geo feature | Binary, as a Parquet GEOGRAPHY column | Binary, LargeBinary, BinaryView | a two-dimensional WKB point, longitude from -180 to 180, latitude from -90 to 90 |

- A `length` is given for a `varchar` or a `char`, and for no other type.
- A timestamp that names no zone is refused, because nothing says it is UTC.
- A source type with no layout type (a fraction, a list, a struct) is refused.
- The source must hold every column the layout names, and nothing else: a column the layout does
  not name is refused rather than dropped.
- A GEOGRAPHY column names spherical edges and no CRS, which is OGC:CRS84. Its WKB is written
  byte for byte as the source holds it.

## The primary key

`primary_key` lists columns, each once, each `"null": false`, and none a `geography` (two
spellings of one point are two byte strings). A row repeating another row's primary key is
refused.

## The key

`keys` holds at most one `{"name", "levels", "covers"}`.

- `levels` are the order the rows are written in. Each is one of:
  - a column, which is not a `geography`;
  - `{"month": "<column>"}` or `{"year": "<column>"}` of an `instant` (in UTC) or a `date`;
  - with the geo feature, `{"cell": "<column>", "band": <degrees>, "sector": <degrees>}` of a
    `geography`: the latitude band and longitude sector the point lies in, numbered
    `band * sectors + sector`, on bands `band` degrees high and sectors `sector` degrees wide.
    `band` must divide 180 degrees and `sector` 360 degrees into whole numbers of pieces.
    Longitude is read round the globe, so 180 and -180 are one meridian; a point at either pole
    lies in the end band.
- A month, a year or a cell orders the rows and is never written as a column.
- A level is named once.
- `covers` lists columns, which are checked to be columns. Every column of a file is written
  beside the key, so nothing else is done with them.

The rows are ordered by the levels, then by the primary key's columns, then by every other column
in the layout's order, so no two different rows tie. NULL comes after every value, and text is
ordered by its bytes.

## Partitions

`partitions` is `{"range": "<column>", "every": N}`.

- The column says `"null": false` and is not a `geography`. `every` is greater than 0.
- In the column's order, a new piece starts at rows N + 1, 2N + 1, …, wherever that row's value is
  greater than the value the piece before starts at. So a value is never split across two pieces,
  and no piece is empty.
- The pieces are named `p1`, `p2`, …, padded to one width, and each is a directory holding
  `rows.parquet`. Within a piece the rows are in the key's order.
- With no partitions, the table is one piece, `p1`.
