import pandas as pd
import pyarrow as pa

from featherstore._metadata import Metadata
from featherstore._table.common import (
    _rows_dtype_matches_index,
    format_cols
)


def can_drop_rows_from_table(rows, table_path, table_exists):
    if not table_exists:
        raise FileNotFoundError("Table doesn't exist")

    is_valid_row_format = isinstance(rows, (list, pd.Index))
    if not is_valid_row_format:
        raise TypeError("'rows' must be either List or pd.Index")

    index_dtype = Metadata(table_path, "table")["index_dtype"]
    if rows and not _rows_dtype_matches_index(rows, index_dtype):
        raise TypeError("'rows' dtype doesn't match table index")


def get_adjacent_partition_name(partition_names, table_path):
    all_partition_names = Metadata(table_path, 'partition').keys()
    index_first_partition = all_partition_names.index(partition_names[0])
    index_last_partition = all_partition_names.index(partition_names[-1])
    if index_first_partition > 0:
        idx = index_first_partition - 1
        partition_names.append(all_partition_names[idx])
    elif index_last_partition < (len(all_partition_names) - 1):
        idx = index_last_partition + 1
        partition_names.append(all_partition_names[idx])
    return sorted(partition_names)


def drop_rows_from_data(df, rows, index_col_name):
    index = df[index_col_name]
    _check_if_rows_isin_index(index, rows)
    mask = _make_arrow_filter_mask(index, rows)
    mask = pa.compute.invert(mask)
    df = df.filter(mask)
    return df


def _check_if_rows_isin_index(index, rows):
    keyword = str(rows[0]).lower()
    if keyword not in ("before", "after", "between"):
        row_array = pa.array(rows)
        mask = pa.compute.is_in(row_array, value_set=index)

        rows_not_in_old_df = not pa.compute.min(mask).as_py()
        if rows_not_in_old_df:
            raise ValueError(f"Some rows not in stored table")


def _make_arrow_filter_mask(index, rows):
    keyword = str(rows[0]).lower()
    if keyword == "before":
        mask = pa.compute.greater_equal(rows[1], index)
    elif keyword == "after":
        mask = pa.compute.less_equal(rows[1], index)
    elif keyword == "between":
        lower_bound = pa.compute.less_equal(rows[1], index)
        higher_bound = pa.compute.greater_equal(rows[2], index)
        mask = pa.compute.and_(lower_bound, higher_bound)
    else:  # When a list of rows is provided
        mask = pa.compute.is_in(index, value_set=pa.array(rows))
    return mask


# ------ drop_columns ------

def can_drop_cols_from_table(cols, table_path, table_exists):
    table_data = Metadata(table_path, 'table')

    if not table_exists:
        raise FileNotFoundError("Table doesn't exist")

    is_valid_col_format = isinstance(cols, list)
    if not is_valid_col_format:
        raise TypeError("'cols' must be of type List")

    col_elements_are_str = all(isinstance(item, str) for item in cols)
    if not col_elements_are_str:
        raise TypeError("Elements in 'cols' must be of type str")

    index_name = table_data["index_name"]
    if index_name in cols:
        raise ValueError("Can't drop index column")

    stored_columns = table_data["columns"]
    stored_columns.remove(index_name)
    dropped_cols = format_cols(cols, stored_columns)

    some_cols_not_in_stored_cols = set(dropped_cols) - set(stored_columns)
    if some_cols_not_in_stored_cols:
        raise IndexError("Trying to drop a column not found in table")

    trying_to_drop_all_cols = not bool(set(stored_columns) - set(dropped_cols))
    if trying_to_drop_all_cols:
        raise IndexError("Can't drop all columns. To drop full table, use 'drop_table()'")


def drop_cols_from_data(df, cols):
    return df.drop(cols)
