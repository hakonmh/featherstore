import bisect

import pyarrow as pa

from featherstore.connection import Connection
from featherstore._metadata import Metadata
from featherstore._table import _raise_if
from featherstore._table import common


def can_drop_rows_from_table(rows, table_path):
    Connection.is_connected()
    _raise_if.table_not_exists(table_path)
    _raise_if.rows_argument_is_not_supported_dtype(rows)
    _raise_if.rows_argument_items_dtype_not_same_as_index(rows, table_path)


def get_adjacent_partition_name(partition_names, table_path):
    """Fetches an extra partition name so we can use that partition
    when combining small partitions.
    """
    all_partition_names = Metadata(table_path, 'partition').keys()
    first_partition = partition_names[0]
    last_partition = partition_names[-1]

    partition_before = _get_partition_before_if_exists(first_partition, all_partition_names)
    partition_after = _get_partition_after_if_exists(last_partition, all_partition_names)

    if partition_before:
        partition_names = _insert_adjacent_partition(partition_before, to=partition_names)
    elif partition_after:
        partition_names = _insert_adjacent_partition(partition_after, to=partition_names)

    return partition_names


def _get_partition_before_if_exists(partition, all_partitions):
    partition_index = all_partitions.index(partition)
    is_not_first_partition = partition_index > 0
    if is_not_first_partition:
        new_partition_idx = partition_index - 1
        return all_partitions[new_partition_idx]


def _get_partition_after_if_exists(partition, all_partitions):
    partition_index = all_partitions.index(partition)
    last_partition_index = len(all_partitions) - 1
    is_not_last_partition = partition_index < last_partition_index
    if is_not_last_partition:
        new_partition_idx = partition_index + 1
        return all_partitions[new_partition_idx]


def _insert_adjacent_partition(adj_partition, *, to):
    partition_names = to.copy()
    bisect.insort(partition_names, adj_partition)
    return partition_names


def drop_rows_from_data(df, rows, index_col_name):
    # TODO: Rework
    index = df[index_col_name]
    _raise_if_rows_are_not_in_index(index, rows)
    mask = _make_arrow_filter_mask(index, rows)  # This should be changed
    mask = pa.compute.invert(mask)
    df = df.filter(mask)
    return df


def _raise_if_rows_are_not_in_index(index, rows):
    keyword = str(rows[0]).lower()
    if keyword not in ("before", "after", "between"):
        row_array = pa.array(rows)
        mask = pa.compute.is_in(row_array, value_set=index)

        rows_not_in_old_df = not pa.compute.min(mask).as_py()
        if rows_not_in_old_df:
            raise ValueError(f"Some rows not in stored table")


def _make_arrow_filter_mask(index, rows):
    # TODO: Drop?
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

    raise_if = CheckDropCols(cols, table_path)
    raise_if.trying_to_drop_index()
    raise_if.cols_are_not_in_stored_data()
    raise_if.trying_to_drop_all_cols()


class CheckDropCols:
    # TODO: Improve

    def __init__(self, cols, table_path):
        table_data = Metadata(table_path, 'table')
        self._index_name = table_data["index_name"]
        stored_cols = table_data["columns"]

        self.cols = cols
        stored_cols.remove(self._index_name)
        self._stored_cols = set(stored_cols)
        self._dropped_cols = set(self._get_dropped_cols())

    def _get_dropped_cols(self):
        dropped_cols = common.filter_cols_if_like_provided(self.cols, self._stored_cols)
        return dropped_cols

    def trying_to_drop_index(self):
        if self._index_name in self.cols:
            raise ValueError("Can't drop index column")

    def cols_are_not_in_stored_data(self):
        some_cols_not_in_stored_cols = bool(self._dropped_cols - self._stored_cols)
        if some_cols_not_in_stored_cols:
            raise IndexError("Trying to drop a column not found in table")

    def trying_to_drop_all_cols(self):
        trying_to_drop_all_cols = not bool(self._stored_cols - self._dropped_cols)
        if trying_to_drop_all_cols:
            raise IndexError("Can't drop all columns. To drop full table, use 'drop_table()'")


def drop_cols_from_data(df, cols):
    return df.drop(cols)
