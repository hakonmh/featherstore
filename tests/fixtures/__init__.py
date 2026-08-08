from os.path import join

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

from ._utils import get_index_name
from .assertions import assert_df_equals, assert_store_table_equal, assert_table_equals
from .cast_table import change_dtype, to_arrow_dtype
from .convert_table import convert_table
from .expected_table import (
    convert_expected,
    insert_column_names_at,
    merge_rows,
    update_table,
)
from .hardcoded_table import (
    apply_operations_to_expected,
    build_e2e_operations,
    make_hardcoded_table,
    shuffle_e2e_operations,
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
from .misc import (
    get_partition_size,
    shuffle_cols,
    sort_table,
)
from .split_table import split_table
from .update_values import replace_values

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
    "apply_operations_to_expected",
    "assert_df_equals",
    "assert_store_table_equal",
    "assert_table_equals",
    "build_e2e_operations",
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
    "make_hardcoded_table",
    "make_table",
    "merge_rows",
    "replace_values",
    "shuffle_cols",
    "shuffle_e2e_operations",
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
