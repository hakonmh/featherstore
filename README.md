# FeatherStore

[![Documentation Status](https://readthedocs.org/projects/featherstore/badge/?version=stable)](https://featherstore.readthedocs.io/en/stable/)
[![Tests (Ubuntu)](https://img.shields.io/github/actions/workflow/status/hakonmh/featherstore/ubuntu-test.yml?label=tests%20ubuntu)](https://github.com/hakonmh/featherstore/actions/workflows/ubuntu-test.yml)
[![Tests (macOS, Windows)](https://img.shields.io/github/actions/workflow/status/hakonmh/featherstore/macos-windows-test.yml?label=tests%20macos%2Fwindows)](https://github.com/hakonmh/featherstore/actions/workflows/macos-windows-test.yml)
[![PyPI version](https://img.shields.io/pypi/v/FeatherStore?color=blue)](https://pypi.org/project/FeatherStore/)
[![Dev Status](https://img.shields.io/pypi/status/featherstore?color=important)](https://pypi.org/project/FeatherStore/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/hakonmh/featherstore/blob/master/LICENSE)

A fast, local datastore for Pandas DataFrames, Polars DataFrames, and PyArrow Tables,
built on partitioned [Feather files](https://arrow.apache.org/docs/python/feather.html).

## Why FeatherStore?

The usual way to persist a DataFrame is to write the whole thing to one file and
read the whole thing back. That works well until the table grows and you only
need last week's rows, two columns, or to add today's data.

FeatherStore splits each table into Feather partitions sorted by a unique index.
That lets most operations touch only the data they need:

* Read a subset of rows (by label or index range) and columns (by name or pattern)
* Append new rows by rewriting only the last partition
* Insert, update, and drop rows by rewriting only the partitions they fall in
* Read column names, index, and shape without loading the data

It runs on a laptop with no server to start: a database is just a directory.

**Good fit:** time series that grow every day, keyed tables you slice by index,
notebooks and scripts that need persistent tables without setting up a database.

**Not a good fit:**

* Ad-hoc SQL or joins across many tables (use DuckDB)
* Several processes writing to the same table (there is no locking)
* Small tables you always read in full (a single Pickle or Feather file is simpler and often faster)
* Frequent column inserts, drops, or type changes on large tables (these rewrite every partition)

## Installation

```bash
pip install featherstore
```

or

```bash
uv add featherstore
```

FeatherStore requires Python 3.11 or newer. pandas, Polars, and PyArrow are
installed as dependencies, including Polars if you only use pandas.

## Quick example

A database is a directory, stores group related tables, and each table is
stored as a set of Feather partitions. `create_database` creates the directory
and connects to it:

```python
import pandas as pd
import featherstore as fs

fs.create_database("weather_db")
store = fs.create_store("weather")

df = pd.DataFrame(
    {"temperature": [2.1, 1.4, 0.8, 3.2], "rainfall": [4.2, 0.0, 0.0, 0.3]},
    index=pd.DatetimeIndex(
        ["2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"], name="date"
    ),
)
store.write_table("bergen", df)
```

`append_table` adds rows whose index values come after the stored data. Only the
last partition is rewritten:

```python
new_day = pd.DataFrame(
    {"temperature": [1.9], "rainfall": [2.6]},
    index=pd.DatetimeIndex(["2024-01-06"], name="date"),
)
store.append_table("bergen", new_day)
```

`select_table` returns a `Table` with methods for inserting, updating, and
dropping data. Rows are inserted at their sorted index position:

```python
table = store.select_table("bergen")

late_day = pd.DataFrame(
    {"temperature": [-0.3], "rainfall": [1.1]},
    index=pd.DatetimeIndex(["2024-01-03"], name="date"),
)
table.insert_rows(late_day)

correction = pd.DataFrame(
    {"rainfall": [3.8]},
    index=pd.DatetimeIndex(["2024-01-01"], name="date"),
)
table.update(correction)
print(table.read_pandas())
```

```text
            temperature  rainfall
date
2024-01-01          2.1       3.8
2024-01-02          1.4       0.0
2024-01-03         -0.3       1.1
2024-01-04          0.8       0.0
2024-01-05          3.2       0.3
2024-01-06          1.9       2.6
```

Reads can load only the rows and columns you need:

```python
print(store.read_pandas("bergen", rows={"after": "2024-01-04"}, cols=["rainfall", "temperature"]))
```

```text
            rainfall  temperature
date
2024-01-04       0.0          0.8
2024-01-05       0.3          3.2
2024-01-06       2.6          1.9
```

Use `store.read_polars()` or `store.read_arrow()` to get the same data as a Polars
DataFrame or PyArrow Table, and `table.drop(rows=...)` or `table.drop(cols=...)` to
remove data. The
[10 minutes to FeatherStore](https://featherstore.readthedocs.io/en/stable/user_guide/10min.html)
guide covers these and more, and
[Gotchas](https://featherstore.readthedocs.io/en/stable/user_guide/gotchas.html)
lists behavior that tends to surprise new users.

## Performance

Numbers below are for a table with 10 million rows and 60 columns of mixed types
(about 6.4 GB as CSV), written from and read into pandas:

* **Full write:** 1.3 seconds with FeatherStore, compared with 2.5 s for Feather,
  3.0 s for Pickle, 11.5 s for Parquet, and 42 s for DuckDB.
* **Full read:** 1.4 seconds with FeatherStore, compared with 1.5 s for Pickle,
  6.2 s for Parquet, and 21 s for DuckDB. A single Feather file is faster at 1.0 s.
* **Range query:** reading a quarter of the rows by index range takes 0.34 s,
  about a quarter of the time of a full read.

See the
[benchmarks](https://featherstore.readthedocs.io/en/stable/Benchmarks.html) for
more details and the benchmark code.

## Project status

FeatherStore is in alpha and maintained by one person in their spare time.
The API may change between 0.x releases; breaking changes are listed in the
[changelog](https://github.com/hakonmh/featherstore/blob/master/CHANGELOG.md).

Bug reports, questions, and pull requests are welcome on
[GitHub Issues](https://github.com/hakonmh/featherstore/issues).

## Development

The project uses [uv](https://docs.astral.sh/uv/) and
[Task](https://taskfile.dev/):

```bash
git clone https://github.com/hakonmh/featherstore.git
cd featherstore
uv sync --all-extras
uv run pytest
```

Run `task --list` to see the other tasks, such as the full test suite, linting,
and building the docs.

## Links

* [Documentation](https://featherstore.readthedocs.io/en/stable/)
* [Changelog](https://github.com/hakonmh/featherstore/blob/master/CHANGELOG.md)
* [Issues](https://github.com/hakonmh/featherstore/issues)
* License: [MIT](https://github.com/hakonmh/featherstore/blob/master/LICENSE)
