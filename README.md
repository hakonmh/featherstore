
# FeatherStore

[![Documentation Status](https://readthedocs.org/projects/featherstore/badge/?version=latest)](https://featherstore.readthedocs.io/en/latest/index.html)
[![Test Status](https://img.shields.io/github/actions/workflow/status/hakonmh/featherstore/macos-windows-test.yml)](https://github.com/hakonmh/featherstore/actions)
[![PyPI version](https://img.shields.io/pypi/v/FeatherStore?color=blue)](https://pypi.org/project/FeatherStore/)
[![Dev Status](https://img.shields.io/pypi/status/featherstore?color=important)](https://pypi.org/project/FeatherStore/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/hakonmh/featherstore/blob/master/LICENSE)

## High-performance datastore built on Apache Arrow and Feather

FeatherStore is a high-performance datastore for Pandas DataFrames, Polars DataFrames,
and PyArrow Tables. Data is stored as partitioned
[Feather files](https://arrow.apache.org/docs/python/feather.html), so FeatherStore can
run several operations on stored tables while loading only the data each operation needs:

* Partial reads
* Appends
* Row and column inserts (Pandas, Polars, or PyArrow)
* Updates (Pandas, Polars, or PyArrow)
* Drops
* Metadata reads (column names, index, table dimensions, and more)
* Column type changes

A database is a directory on disk. Stores group related tables, and each table is
stored as partitioned Feather files.

For a full walkthrough, see the
[getting started guide](https://featherstore.readthedocs.io/en/stable/Quickstart.html).

## Using FeatherStore

```python
import pandas as pd
import featherstore as fs

fs.create_database("path/to/db")
store = fs.create_store("weather")

df = pd.DataFrame(
    {
        "temperature": [2.1, 1.4, 0.8, 3.2],
        "humidity": [88, 91, 79, 74],
        "rainfall": [4.2, 0.0, 0.0, 0.3],
    },
    index=pd.DatetimeIndex(
        ["2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"],
        name="date",
    ),
)
store.write_table("bergen", df)

print(store.read_pandas("bergen"))
#             temperature  humidity  rainfall
# date
# 2024-01-01          2.1        88       4.2
# 2024-01-02          1.4        91       0.0
# 2024-01-04          0.8        79       0.0
# 2024-01-05          3.2        74       0.3

# Append without loading the full table
new_day = pd.DataFrame(
    {"temperature": [1.9], "humidity": [83], "rainfall": [2.6]},
    index=pd.DatetimeIndex(["2024-01-06"], name="date"),
)
store.append_table("bergen", new_day)

# Insert, update, and drop with a Table object
table = store.select_table("bergen")
delayed = pd.DataFrame(
    {"temperature": [-0.3], "humidity": [85], "rainfall": [1.1]},
    index=pd.DatetimeIndex(["2024-01-03"], name="date"),
)
table.insert(delayed)  # matching column names -> inserts rows

# Query only the partitions and columns you need
print(store.read_pandas("bergen", rows={"after": "2024-01-04"}, cols=["rainfall", "temperature"]))
#             rainfall  temperature
# date
# 2024-01-04       0.0          0.8
# 2024-01-05       0.3          3.2
# 2024-01-06       2.6          1.9
```

Tables can also be read with `store.read_polars()` and `store.read_arrow()`.

## Performance

FeatherStore is fast on both small and large tables.
See the full comparison in the [documentation](https://featherstore.readthedocs.io/en/stable/Benchmarks.html).

## Installation

Install FeatherStore with pip or uv:

```
pip install featherstore
uv add featherstore
```

Or from source:

```
pip install git+https://github.com/hakonmh/featherstore.git
uv add git+https://github.com/hakonmh/featherstore.git
```

## Requirements

FeatherStore 0.3.0 requires:

* Python >= 3.11
* pandas >= 2.2.0
* polars[timezone] >= 1.21.0
* pyarrow >= 14.0.0

These are installed automatically with `pip install featherstore` or `uv add featherstore`.

## Documentation

See the [documentation](https://featherstore.readthedocs.io/en/stable/index.html) for the full API and feature set.
