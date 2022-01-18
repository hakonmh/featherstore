
# FeatherStore
[![Documentation Status](https://readthedocs.org/projects/featherstore/badge/?version=latest)](https://featherstore.readthedocs.io/en/latest/index.html)
[![PyPI version](https://img.shields.io/pypi/v/FeatherStore?color=blue)](https://pypi.org/project/FeatherStore/)
[![Dev Status](https://img.shields.io/pypi/status/featherstore?color=important)](https://pypi.org/project/FeatherStore/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://github.com/hakonmh/featherstore/blob/master/LICENSE)

## High performance datastore built upon Apache Arrow & Feather

FeatherStore is a fast datastore for storing Pandas DataFrames, Pandas Series, Polars
DataFrames and PyArrow Tables as partitioned [Feather Files](https://arrow.apache.org/docs/python/feather.html).
FeatherStore supports several operations on stored tables that can be done without loading
in the full data:
* Predicate filtering when reading
* Append data
* Read metadata (column names, index, table shape, etc)
* Insert data
* Update data
* Drop data
* Changing column types

To learn more, read the [User Guide](https://featherstore.readthedocs.io/en/latest/Quickstart.html).

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
# Creates a data store
fs.create_store('example_store')
# List existing stores in current database
fs.list_stores()

['example_store']

>>> # Connect to store
store = fs.Store('example_table')
# Saves table to store, partition size defines the size of each partition in bytes
PARTITION_SIZE = 128  # bytes
store.write_table('example_table', df, partition_size=PARTITION_SIZE)
# Lists existing tables in current store
store.list_tables()

['example_table']

>>> # FeatherStore can read tables as Arrow Tables, Pandas DataFrames or Polars DataFrames
store.read_pandas('example_table')
# store.read_arrow('example_table') for reading to Arrow Tables
# store.read_polars('example_table') for reading to Polars DataFrames

                   A         B         C         D
2021-01-01  1.122392  0.265080  0.908843 -0.546288
2021-01-02 -2.189536  0.593536  0.428618  1.159518
2021-01-03  1.344019  0.723140  1.266272 -0.707655
2021-01-04 -1.755134 -0.399792 -0.229055  0.733093
2021-01-05 -0.871126  1.192000  0.425984  0.275433

>>> # FeatherStore supports appending data without loading in the full table
new_dates = pd.date_range("2021-01-06", periods=1)
df1 = pd.DataFrame(randn(1, 4), index=new_dates, columns=list("ABCD"))
store.append_table('example_table', df1)
# It also supports querying parts of the data
store.read_pandas('example_table', rows=['after', '2021-01-05'], cols=['D', 'A'])

                   D         A
2021-01-05  0.275433 -0.871126
2021-01-06  0.606950  0.408125

```

## Installation
FeatherStore can be installed by using `$ pip install featherstore`

## Requirements:
* Python >= 3.8
* Arrow
* Pandas
* Polars
* Numpy

## Documentation
Want to know about all the features FeatherStore support? [Read the docs!](https://featherstore.readthedocs.io/en/latest/index.html)
