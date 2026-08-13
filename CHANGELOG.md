Changelog
=========

0.3.0
-----

**Warning**: This update causes some API-breaking changes:

* Raised minimum Python version from 3.8 to 3.11
* Raised dependency floors to `pandas>=2.2.0`, `polars[timezone]>=1.21.0`,
  and `pyarrow>=14.0.0` (upper bounds removed)
* Renamed `Table.insert()` to `Table.insert_rows()` and
  `Table.add_columns()` to `Table.insert_columns()`
* Domain errors now raise custom exceptions from
  ``featherstore.exceptions`` instead of built-in types such as
  ``IndexError``, ``FileNotFoundError``, ``FileExistsError``, ``ValueError``,
  ``TypeError``, ``ConnectionError``, ``ConnectionRefusedError``,
  ``PermissionError``, and ``OSError``
* Import exception types via ``from featherstore.exceptions import ...``;
  they are not re-exported from the top-level ``featherstore`` package
* Argument-shape validation (invalid types, missing parameters) still raises
  ``TypeError``, ``ValueError``, and ``AttributeError``

Enhancements:

* ``Table.update()``, ``Table.insert_rows()``, ``Table.insert_columns()``, and
  ``Table.insert()`` accept Pandas DataFrame/Series, Polars DataFrame, and
  PyArrow Table input (Polars Series is not supported for these edit APIs)
* Re-added `Table.insert()` as a convenience method that dispatches to
  `insert_rows()` or `insert_columns()` based on the number of columns in
  the input data
* `Table.insert_columns()` and `Table.insert()` accept a sequence of column
  positions for `idx` (one position per new column)
* ``Table.insert_rows()``, ``Table.insert_columns()``, and ``Table.insert()``
  accept a ``warnings=`` parameter (``"warn"`` or ``"ignore"``, default
  ``"warn"``) to control unsorted-index sorting warnings
* Partition writes use Arrow IPC with atomic replace (avoids corrupting
  memory-mapped files on overwrite)
* Broadened supported index types (decimal, float, uint, binary, duration, and
  more) with stricter validation in `_raise_if` and `_table_utils`
* Added a hierarchical exception tree rooted at ``FeatherStoreError``,
  with category bases (``TableError``, ``StoreError``, ``ColumnError``,
  ``RowError``, ``IndexSchemaError``, ``DatabaseConnectionError``,
  ``SnapshotError``, ``PathError``) and specific leaf exceptions such as
  ``ColumnNotFoundError``, ``RowNotFoundError``, and
  ``TableAlreadyExistsError``
* Improved domain error messages to include offending values where useful
  (for example, missing column names and row indices)
* Export `database_exists()` from the top-level `featherstore` package
* Pandas metadata generation updated for pandas 2.2 / 3.x string and float
  dtypes (version-gated string metadata constants); sorting uses Arrow
  `sort_indices` so categoricals keep unused categories
* Raised minimum Polars version to 1.21.0 (first release with `n_unique()`
  support for decimal index dtypes)
* Documentation updated for 0.3.0 (`rename_store(..., to=...)`,
  `table.exists()`, `insert()`, `insert_rows()`, `insert_columns()`,
  multi-backend edit APIs, `warnings=`, and dependency requirements)
* Added an Exceptions page to the API reference with a nested hierarchy list
  and per-class documentation
* Added ``Raises`` sections to public method and function docstrings for
  ``Table``, ``Store``, ``connection``, and ``snapshot``
* Migrated packaging to `pyproject.toml`; removed `setup.py`,
  `requirements.txt`, `pytest.ini`, and `.flake8` (pytest and flake8 config
  now live in `pyproject.toml`)
* CI and Read the Docs install via `pip install -e ".[dev]"`; PyPI publish
  builds with `python -m build`
* Snapshot extraction passes `filter='data'` on Python 3.12+
* Added Taskfile tasks for `uv sync`, docs, lint, and tests
* Refactored `benchmarks/`: renamed `external`/`internal` to
  `format_comparison`/`table_operations`, applied clean-code structure
  (shared helpers, explicit imports, PascalCase benchmark classes), and added
  module docstrings
* Added Taskfile benchmark tasks: `bench:format-comparison`,
  `bench:table-operations`, and `bench:log`
* Added ``tests/test_exceptions.py`` for hierarchy and message coverage
* Added e2e workflow test and shared fixtures for astype, expected-table, and
  hardcoded-table scenarios
* Replaced test-suite star imports with explicit fixture imports; added `__all__`
  to `tests/fixtures`
* Applied ruff formatting across `featherstore`, `tests`, and `benchmarks`
* Internal benchmark logs now write to `.dev/bmarks`

Bugfixes:

* Fixed store and table names such as ``..``, ``.``, and names containing
  path separators escaping the database directory; delete operations now
  resolve paths before checking they lie inside the database
* Fixed silent data loss on repeated mid-table ``insert_rows`` caused by
  lossy partition-ID round-trips (fractional IDs collapsed when generating
  new mid-gap partition names)
* Fixed intermittent `PermissionError` (WinError 32) on Windows when deleting
  partition files during bulk cleanup (e.g. `drop_table()`); file removal now
  uses runtime-probed POSIX delete semantics on Windows 10+ (including
  Windows 11 and later) via ``SetFileInformationByHandle``, falling back to
  ``os.remove`` only when the API or filesystem reports the operation as
  unsupported, with the existing short retry loop kept as secondary protection
* Fixed snapshot restore with ``errors="ignore"`` leaving orphan partition
  files or tables behind
* Fixed ``astype`` not refreshing ``index_dtype`` metadata when casting the
  index
* Fixed stale ``num_columns`` / ``shape`` after inserting or dropping columns
* Fixed ``reorder_columns`` / ``columns`` setter dropping the index name from
  table metadata
* Fixed ``rename_table`` allowing the forbidden ``.metadata`` name
* Fixed ``database_exists`` not expanding ``~`` in paths
* Fixed ``Indexer.keyword`` raising ``AttributeError`` for non-keyword dict
  keys
* Fixed SQL ``LIKE`` patterns treating regex metacharacters as special and
  crashing on empty patterns
* Fixed snapshot restore accepting invalid ``errors`` values
* Fixed ``connect()`` replacing the live connection before validation, so a
  failed connect or reconnect left ``is_connected()`` raising
  ``AttributeError`` and made ``disconnect()`` unusable
* Fixed ``insert_rows`` keeping ``has_default_index`` when new ids are
  consecutive with each other but leave a gap after the last stored value,
  which caused later reads to drop the index and replace it with ``0..n-1``
* Fixed ``astype`` leaving ``has_default_index`` set after casting the index
  to a non-default type, so later reads dropped the converted index values
* Fixed ``Table.rename_table`` leaving Metadata pointing at the old path, so
  a later read on the same instance raised ``FileNotFoundError``

0.2.1
-----

* Fixed PermissionError when trying to modify a previously opened file on windows
* Changed minimum PyArrow requirement from 7.0.0 to 8.0.0

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
