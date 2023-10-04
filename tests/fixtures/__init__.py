from os.path import join
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME, DB_MARKER_NAME

from .convert_table import convert_table
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
from .assertions import assert_table_equals, assert_df_equals

DB_PATH = join('tests', '_db')
STORE_NAME = "test_store"
TABLE_NAME = "table_name"
TABLE_PATH = join(DB_PATH, STORE_NAME, TABLE_NAME)
MD_NAME = 'db'
