0.0.4
-----
**Warning**: This update causes some API-breaking changes:
* `list_tables` and `list_store` argument `like` now uses `?` as single character
  wildcard instead of `_`.

Other changes:
* Added `Table.insert()`


0.0.3
-----
* Added missing dependencies
* Updated docs

0.0.2
-----

**Warning**: This update causes some API-breaking changes:
* `Table.read()` and `Store.read_table()` has now been renamed to `read_arrow()`
* The parameter `new_name` in `rename_*()` functions and methods have been changed to `to`

Other changes:
* Improved `read_polars()` performance
* Index column now appears first when reading data as Arrow or Polars
* `read_pandas()` now converts to Pandas Series where it is possible
* Improved performance of all metadata handling

0.0.1
-----

Initial release.