from os.path import join
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME, DB_MARKER_NAME

from .cast_table import change_dtype, to_arrow_dtype
from .convert_table import convert_table
from .expected_table import merge_rows, insert_columns_expected, insert_column_names_at
from .make_table import (
    make_table,
    default_index,
    fake_default_index,
    sorted_string_index,
    sorted_datetime_index,
    continuous_datetime_index,
    continuous_string_index,
    unsorted_int_index,
    unsorted_string_index,
    unsorted_datetime_index,
    sorted_timedelta_index,
    sorted_time32_index,
    sorted_date32_index,
    sorted_float_index,
    sorted_uint_index,
    sorted_decimal_index,
    sorted_binary_index,
    sorted_large_string_index,
)
from .misc import (
    shuffle_cols,
    sort_table,
    get_partition_size,
    format_arrow_table,
    df_has_default_index,
    drop_default_index_if_exists,
)
from .split_table import split_table
from ._utils import get_index_name
from .update_values import update_values
from .assertions import assert_table_equals, assert_df_equals, assert_store_table_equal
from .hardcoded_table import (
    make_hardcoded_table,
    build_e2e_operations,
    shuffle_e2e_operations,
    apply_operations_to_expected,
)

DB_PATH = join("tests", "_db")
STORE_NAME = "test_store"
TABLE_NAME = "table_name"
TABLE_PATH = join(DB_PATH, STORE_NAME, TABLE_NAME)
MD_NAME = "db"

__all__ = [
    "DEFAULT_ARROW_INDEX_NAME",
    "DB_MARKER_NAME",
    "change_dtype",
    "to_arrow_dtype",
    "convert_table",
    "merge_rows",
    "insert_columns_expected",
    "insert_column_names_at",
    "make_table",
    "default_index",
    "fake_default_index",
    "sorted_string_index",
    "sorted_datetime_index",
    "continuous_datetime_index",
    "continuous_string_index",
    "unsorted_int_index",
    "unsorted_string_index",
    "unsorted_datetime_index",
    "sorted_timedelta_index",
    "sorted_time32_index",
    "sorted_date32_index",
    "sorted_float_index",
    "sorted_uint_index",
    "sorted_decimal_index",
    "sorted_binary_index",
    "sorted_large_string_index",
    "shuffle_cols",
    "sort_table",
    "get_partition_size",
    "format_arrow_table",
    "df_has_default_index",
    "drop_default_index_if_exists",
    "split_table",
    "get_index_name",
    "update_values",
    "assert_table_equals",
    "assert_df_equals",
    "assert_store_table_equal",
    "make_hardcoded_table",
    "build_e2e_operations",
    "shuffle_e2e_operations",
    "apply_operations_to_expected",
    "DB_PATH",
    "STORE_NAME",
    "TABLE_NAME",
    "TABLE_PATH",
    "MD_NAME",
]
