# The layout file

A layout file describes tables as JSON: each table's columns, its primary key, the one key its
rows are ordered by, and the column its pieces are cut on. The key's order is what lets a reader
skip rows: the narrower the stretch each row group holds, the more row groups a question rules out
from the metadata alone. `warren-arrow` reads format 2 of the file. It writes what the file says,
or refuses the file, or a table, with the reason. Nothing is worked out beyond what the file says.

A purchase names the collection row it buys, `(builder_id, row_no)`, and reaches its set through
it; nothing another table decides is written onto the purchase. Its key is its month, then its
instant, so each row group holds one month, and a question on December keeps December's row
groups alone:

```json
{
  "format": 2,
  "name": "lego",
  "tables": [{
    "name": "lego_purchases",
    "columns": [
      {"name": "purchase_id", "type": "bigint", "null": false},
      {"name": "builder_id", "type": "integer", "null": false},
      {"name": "row_no", "type": "integer", "null": false},
      {"name": "ordered_at", "type": "instant", "null": false}
    ],
    "primary_key": ["purchase_id"],
    "keys": [{"name": "lego_purchases_by_month", "levels": [{"month": "ordered_at"}, "ordered_at"]}]
  }]
}
```

## The file

- `format` is 2. Any other format is refused.
- `name` is the layout's name. Every name in the file (the layout, its tables, columns and keys) is
  lower-case letters, digits and underscores, starts with a letter or an underscore, and has at
  most 63 characters.
- A field the format does not name is refused.

## A table

Only `name` and `columns` are required.

| field | what `warren-arrow` does with it |
|---|---|
| `columns` | the columns written, in this order |
| `primary_key` | checked unique on every row; with no key, the order the rows are written in |
| `keys` | at most one key, the order the rows are written in |
| `partitions` | the column the pieces are cut on |

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
  - `{"month": "<column>"}`, `{"year": "<column>"}` or `{"weekday": "<column>"}` of an
    `instant` (in UTC) or a `date`. A weekday is 1 for Monday to 7 for Sunday;
  - with the geo feature, `{"cell": "<column>", "band": <degrees>, "sector": <degrees>}` of a
    `geography`: the latitude band and longitude sector the point lies in, numbered
    `band * sectors + sector`, on bands `band` degrees high and sectors `sector` degrees wide.
    `band` must divide 180 degrees and `sector` 360 degrees into whole numbers of pieces.
    Longitude is read round the globe, so 180 and -180 are one meridian; a point at either pole
    lies in the end band.
- A month, a year or a weekday orders the rows and is written as a `smallint` column named
  `<column>_month`, `<column>_year` or `<column>_weekday`, after the layout's own columns, so a
  reader sees its values in each row group's and each page's statistics. A table with a column of
  that name is refused. The certificate reads only the layout's own columns.
- A key whose first level is a month, a year or a weekday ends a row group wherever that level's
  value changes, so each row group holds one value, and its statistics name it.
- A cell orders the rows and is never written as a column.
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

## Spellings on trial

Format 2 does not have these. They are read so that they can be tried, and may change or go.

- **A part stored under a name of its own**: the level `{"month" | "year" | "weekday":
  "<column>", "store": "<name>"}` is a month, a year or a weekday level written as the column
  `<name>` in place of `<column>_<part>`. `<name>` is not one of the table's columns.
- **A piece per part**: `partitions` may instead be `{"phase": {"month" | "year" | "weekday":
  "<column>"}}`, a piece for each value, its directory named `<column>_<part>=<value>`. The column
  says `"null": false`. The part is in the directory's name and not in the rows.

`warren-arrow write --row-group-per-block` ends a row group wherever the key's first level changes
value when that level is a column, as a key led by a month, a year or a weekday always does.
