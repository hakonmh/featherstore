"""Public re-exports and shared constants for the test fixture package."""

from os.path import join

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

from ._utils import get_index_name
from .assertions import assert_df_equals, assert_store_table_equal, assert_table_equals
from .cast_table import change_dtype, to_arrow_dtype
from .convert_table import convert_expected, convert_table
from .edit_table import (
    insert_column_names_at,
    regenerate_values,
    shuffle_cols,
    sort_table,
    update_table,
)
from .make_table import (
    continuous_datetime_index,
    continuous_string_index,
    default_index,
    fake_default_index,
    make_table,
    sorted_binary_index,
    sorted_date32_index,
    sorted_datetime_index,
    sorted_decimal_index,
    sorted_float_index,
    sorted_large_string_index,
    sorted_string_index,
    sorted_time32_index,
    sorted_timedelta_index,
    sorted_uint_index,
    unsorted_int_index,
    unsorted_string_index,
)
from .misc import get_partition_size
from .partitions import (
    assert_partition_bounds_are_ordered,
    assert_partition_metadata_matches_files,
    partition_layout,
    partition_names,
    pruned_partitions,
)
from .split_table import split_table

DB_PATH = join("tests", "_db")
STORE_NAME = "test_store"
TABLE_NAME = "table_name"
TABLE_PATH = join(DB_PATH, STORE_NAME, TABLE_NAME)
MD_NAME = "db"

__all__ = [
    "DB_PATH",
    "DEFAULT_ARROW_INDEX_NAME",
    "MD_NAME",
    "STORE_NAME",
    "TABLE_NAME",
    "TABLE_PATH",
    "assert_df_equals",
    "assert_partition_bounds_are_ordered",
    "assert_partition_metadata_matches_files",
    "assert_store_table_equal",
    "assert_table_equals",
    "change_dtype",
    "continuous_datetime_index",
    "continuous_string_index",
    "convert_expected",
    "convert_table",
    "default_index",
    "fake_default_index",
    "get_index_name",
    "get_partition_size",
    "insert_column_names_at",
    "make_table",
    "partition_layout",
    "partition_names",
    "pruned_partitions",
    "regenerate_values",
    "shuffle_cols",
    "sort_table",
    "sorted_binary_index",
    "sorted_date32_index",
    "sorted_datetime_index",
    "sorted_decimal_index",
    "sorted_float_index",
    "sorted_large_string_index",
    "sorted_string_index",
    "sorted_time32_index",
    "sorted_timedelta_index",
    "sorted_uint_index",
    "split_table",
    "to_arrow_dtype",
    "unsorted_int_index",
    "unsorted_string_index",
    "update_table",
]
