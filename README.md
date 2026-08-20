
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

For more information, see the
[documentation](https://featherstore.readthedocs.io/en/stable/Quickstart.html).

## Using FeatherStore

```python
>>> # Create a Pandas DataFrame
import pandas as pd
from numpy.random import randn
import featherstore as fs

dates = pd.date_range("2021-01-01", periods=5)
df = pd.DataFrame(randn(5, 4), index=dates, columns=list("ABCD"))

                   A         B         C         D
2021-01-01  0.402138 -0.016436 -0.565256  0.520086
2021-01-02 -1.071026 -0.326358 -0.692681  1.188319
2021-01-03  0.777777 -0.665146  1.017527 -0.064830
2021-01-04 -0.835711 -0.575801 -0.650543 -0.411509
2021-01-05 -0.649335 -0.830602  1.191749  0.396745

>>> # Create a database folder at the given path
fs.create_database('path/to/db')
fs.connect('path/to/db')
fs.database_exists('path/to/db')  # True
# Create a store
fs.create_store('example_store')
# List existing stores in the current database
fs.list_stores()

['example_store']

>>> # Connect to the store
store = fs.Store('example_store')
# Save the table to the store; partition_size is the size of each partition in bytes
PARTITION_SIZE = 128  # bytes
store.write_table('example_table', df, partition_size=PARTITION_SIZE)
# List existing tables in the current store
store.list_tables()

['example_table']

>>> # FeatherStore can read tables as Arrow Tables, Pandas DataFrames, or Polars DataFrames
store.read_pandas('example_table')
# store.read_arrow('example_table') for Arrow Tables
# store.read_polars('example_table') for Polars DataFrames

                   A         B         C         D
2021-01-01  0.402138 -0.016436 -0.565256  0.520086
2021-01-02 -1.071026 -0.326358 -0.692681  1.188319
2021-01-03  0.777777 -0.665146  1.017527 -0.064830
2021-01-04 -0.835711 -0.575801 -0.650543 -0.411509
2021-01-05 -0.649335 -0.830602  1.191749  0.396745

>>> # FeatherStore can append data without loading the full table
new_dates = pd.date_range("2021-01-06", periods=1)
df1 = pd.DataFrame(randn(1, 4), index=new_dates, columns=list("ABCD"))
store.append_table('example_table', df1)

>>> # Insert rows or columns with Table.insert(), which dispatches based on column names.
# Table.update() and the insert methods also accept Polars DataFrames and PyArrow Tables.
table = store.select_table('example_table')
new_rows = pd.DataFrame(randn(1, 4), index=[pd.Timestamp("2021-01-07")], columns=list("ABCD"))
table.insert(new_rows)  # matching column names -> inserts rows

new_col = pd.DataFrame({'E': randn(7)}, index=table.read_pandas().index)
table.insert(new_col, idx=4)  # new column name -> inserts column at position 4

>>> # Query parts of the data
store.read_pandas('example_table', rows={'after': '2021-01-05'}, cols=['D', 'A'])

                   D         A
2021-01-05  0.396745 -0.649335
2021-01-06  0.606950  0.408125

```

## Performance

FeatherStore is fast on both small and large tables.
See the full comparison in the [documentation](https://featherstore.readthedocs.io/en/stable/Benchmarks.html).

## Installation

Install FeatherStore with `$ pip install featherstore`, or from source with
`$ pip install git+https://github.com/hakonmh/featherstore.git`.

## Requirements

FeatherStore 0.3.0 requires:

* Python >= 3.11
* pandas >= 2.2.0
* polars[timezone] >= 1.21.0
* pyarrow >= 14.0.0

These are installed automatically with `pip install featherstore`.

## Documentation

See the [documentation](https://featherstore.readthedocs.io/en/stable/index.html) for the full API and feature set.
