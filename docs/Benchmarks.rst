Benchmarks
==========

In this benchmark we'll compare how well FeatherStore, Feather, Parquet, CSV,
PyStore and DuckDB perform when reading and writing Pandas DataFrames.

All functions take the same Pandas DataFrame, write it to the hard drive, and
load it back into Pandas again.

The benchmark ran on the following computer:

* CPU: Intel© Core™ i5-11600
* RAM: 48 GB DDR4 (3200 MHz)
* SSD: 1 TB M.2 NVMe (3470/3000 Read/Write MBps)
* GPU: NVIDIA GeForce GTX 1060 6GB (Not used during the benchmark)

The times quoted below are the fastest query times seen during a series of
runs.

First Dataset
-------------

This dataset is made up of 1 billion fields of random data in the shape 10
million rows and 100 columns (Approx. 19.3 gb of data when stored as CSV).
The data consists of strings, integers, floats and datetimes with 25 columns
of each data type.

**Write table benchmark (Table size: 10 000 000, 100):**

- Write FeatherStore:  3 716.43 ms
- Write Feather:       3 581.44 ms
- Write Parquet:       8 038.80 ms
- Write CSV:         750 304.05 ms
- Write PyStore:      24 074.08 ms
- Write DuckDB:       13 147.43 ms

**Read table benchmark (Table size: 10 000 000, 100):**

- Read FeatherStore:  2 007.28 ms
- Read Feather:       6 516.06 ms
- Read Parquet:       6 486.78 ms
- Read CSV:         101 269.68 ms
- Read PyStore:      13 514.46 ms
- Read DuckDB:       18 228.74 ms

Second Dataset
--------------

The second dataset is made up of 94.6 million fields of random data in the
shape 15.8 million rows and 6 columns. (Approx. 2.0 gb of data when stored
as CSV). The data consists of one datetime column and 5 float columns.

The benchmark provides a rough estimate of the time it takes to save and load
intraday OHLCV asset data (15.8 million records is equal to 30 years of
1 minute data in a 24/7 market).

**Write table benchmark (Table size: 15 768 000, 6):**

- Write FeatherStore:  518.08 ms
- Write Feather:       389.08 ms
- Write Parquet:       827.67 ms
- Write CSV:        78 259.50 ms
- Write PyStore:     2 347.50 ms
- Write DuckDB:      1 814.55 ms

**Read table benchmark (Table size: 15 768 000, 6):**

- Read FeatherStore:  172.16 ms
- Read Feather:       396.35 ms
- Read Parquet:       697.16 ms
- Read CSV:        12 389.58 ms
- Read PyStore:       994.11 ms
- Read DuckDB:      1 304.28 ms

Internal Benchmarks
-------------------

In addition to supporting reading and writing Pandas DataFrames, FeatherStore
also supports reading and writing Polars DataFrames and PyArrow Tables.
These two data structures use the Apache Arrow Columnar Format as a memory
model, allowing reads and writes without serializing and deserializing to and
from Pandas.

We will benchmark using the first dataset again, comparing reading and writing
the dataset as a Pandas DataFrame, a Polars DataFrame, and a PyArrow Table
using FeatherStore.

**Write table benchmark (Table size: 10 000 000, 100):**

- Write Pandas:  3 734.78 ms
- Write Polars:  3 359.98 ms
- Write Arrow:   3 374.49 ms

**Read table benchmark (Table size: 10 000 000, 100):**

- Read Pandas:  1 377.60 ms
- Read Polars:    241.79 ms
- Read Arrow:      16.60 ms
