Changelog
=========

0.2.0
-----

**Warning**: This update causes some API-breaking changes:

* Changed metadata backend, which breaks tables written in earlier versions
* Changed filtering for `cols` from `cols=['like', <pattern>]` to `cols={'like': <pattern>}`
* Changed filtering for `rows` from `rows=[<keyword>, <value>]` to `rows={<keyword>: <value>}`
* Changed `store.store_name` to `store.name`
* Changed `table.exists` to `table.exists()`

Enhancements:

* Restoring snapshots no longer overwrites existing store or tables by default
  * Added `errors` parameter to adjust this behavior
* Added `table.name`
* Added `is_connected()`
* Added `store_exists()`
* Added `database_exists()`
* Added `table.partition_size` read only property
* Added `table.repartition(new_partition_size)` for re-partitioning a table to `new_partition_size`
* Dropping both `rows` and `cols` in `table.drop()` at the same time is now supported
* Added support for `numpy` datatypes in `table.astype()`
* `rows` and `cols` arguments now supports more sequence types than just lists
* `before`, `after`, and `between` is no longer invalid index values
* `like` is no longer a invalid column name
* `table.read(rows=[...])` now raises an exception when `rows` are not found in the table
* Improved `table.insert()` performance
* Made some exceptions messages clearer

Bugfixes:

* `connect(<Database>)` now correctly switches connection to `<Database>` instead of staying connected to the old database
* Fixed `append` not working properly with default index
* Fixed `read_pandas` not working with binary columns
* Fixed `read_pandas` not working with large string columns
* Fixed `read_pandas` not working with date32 and date64 columns
* Fixed a bug causing `insert` to sometimes delete a partition
* Fixed `Table.shape` not working
* Fixed `store.rename` not working
* Fixed predicate filtering keeping one row to many in special cases
* Fixed being able to write tables with non-string column names
* Fixed performance bottleneck when making hidden files on windows

Other:

* Updated dependency requirements to:
  * `polars[timezone]=0.14.11`
  * `pyarrow>=7.0.0`

0.1.1
-----

* Fixed behavior of `Table.append()` when using the default index

0.1.0
-----

* Added snapshots
* Added `Table.astype()`
* Added `Table.reorder_columns()` as a synonym to `Table.columns = values`
* Added `Store.drop()` as a synonym to `store.drop_store()`
* Added option to set partition_size=-1 to disable partitioning
* Added performance comparison to docs
* Added a simple performance benchmark script to benchmark reads and writes
* Performance improvements

0.0.5
-----

* Added `Table.add_columns()`
* Added `Table.rename_columns()`
* You can now use `Table.columns = values` to reorganize columns
* Improved performance of all write operations
* Changed minimum PyArrow requirement from 4.0.0 to 5.0.0

0.0.4
-----

**Warning**: This update causes some API-breaking changes:

* `store.table` renamed to `store.select_table`
* `list_tables` and `list_store` argument `like` now uses `?` as single-character
  wildcard instead of `_`.
* Removed `read_table_metadata` and `read_partition_metadata`

Enhancements:

* Added `Table.update()`
* Added `Table.insert()`
* Added `Table.drop()`, `Table.drop_rows()` and `Table.drop_columns()`
* Added `Table.shape` and `Table.exists`
* Removed msgpack dependency
* Performance improvements

0.0.3
-----

* Added missing dependencies
* Updated docs

0.0.2
-----

**Warning**: This update causes some API-breaking changes:

* `Table.read()` and `Store.read_table()` has now been renamed to `read_arrow()`
* The parameter `new_name` in `rename_%()` functions and methods have been changed to `to`

Enhancements:

* Improved `read_polars()` performance
* Index column now appears first when reading data as Arrow or Polars
* `read_pandas()` now converts to Pandas Series where it is possible
* Improved performance of all metadata handling

0.0.1
-----

Initial release.
