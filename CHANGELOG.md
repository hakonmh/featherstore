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

Other changes:
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

Other changes:
* Improved `read_polars()` performance
* Index column now appears first when reading data as Arrow or Polars
* `read_pandas()` now converts to Pandas Series where it is possible
* Improved performance of all metadata handling

0.0.1
-----
Initial release.
