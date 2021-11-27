import pyarrow as pa

from featherstore.connection import Connection
from featherstore._metadata import Metadata
from featherstore._table import _raise_if
from featherstore._table.common import filter_table_cols


def can_drop_rows_from_table(rows, table_path):
    Connection.is_connected()
    _raise_if.table_not_exists(table_path)
    _raise_if.rows_argument_is_not_supported_dtype(rows)
    _raise_if.rows_argument_items_dtype_not_same_as_index(rows, table_path)


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


# ----------------- drop_columns ------------------


def can_drop_cols_from_table(cols, table_path):
    Connection.is_connected()
    _raise_if.table_not_exists(table_path)
    _raise_if.cols_argument_is_not_supported_dtype(cols)
    _raise_if.cols_argument_items_is_not_str(cols)

    check_if = CheckDropCols(cols, table_path)
    check_if.trying_to_drop_index()
    check_if.cols_are_in_stored_data()
    check_if.trying_to_drop_all_cols()


class CheckDropCols:

    def __init__(self, cols, table_path):
        self.cols = cols

        table_data = Metadata(table_path, 'table')
        self._index_name = table_data["index_name"]
        stored_cols = table_data["columns"]

        stored_cols.remove(self._index_name)
        self._stored_columns = set(stored_cols)
        dropped_cols = filter_table_cols(cols, stored_cols)
        self._dropped_columns = set(dropped_cols)

    def trying_to_drop_index(self):
        if self._index_name in self.cols:
            raise ValueError("Can't drop index column")

    def cols_are_in_stored_data(self):
        some_cols_not_in_stored_cols = bool(self._dropped_columns - self._stored_columns)
        if some_cols_not_in_stored_cols:
            raise IndexError("Trying to drop a column not found in table")

    def trying_to_drop_all_cols(self):
        trying_to_drop_all_cols = not bool(self._stored_columns - self._dropped_columns)
        if trying_to_drop_all_cols:
            raise IndexError("Can't drop all columns. To drop full table, use 'drop_table()'")


def drop_cols_from_data(df, cols):
    return df.drop(cols)
