import os
import bisect

import pyarrow as pa

from featherstore.connection import Connection
from featherstore._metadata import Metadata
from featherstore._table import _raise_if
from featherstore._table import common
from featherstore._table import _table_utils
from featherstore._table.read import get_partition_names as _get_partition_names


def can_drop_rows_from_table(rows, table_path):
    Connection.is_connected()
    _raise_if.table_not_exists(table_path)
    _raise_if.rows_argument_is_not_supported_dtype(rows)
    _raise_if.rows_argument_items_dtype_not_same_as_index(rows, table_path)


def get_partition_names(rows, table_path):
    names = _get_partition_names(rows, table_path)
    names = _get_adjacent_partition_name(names, table_path)
    return names


def _get_adjacent_partition_name(partition_names, table_path):
    """Fetches an extra partition name so we can use that partition
    when combining small partitions.
    """
    all_partition_names = Metadata(table_path, 'partition').keys()
    first_partition = partition_names[0]
    last_partition = partition_names[-1]

    partition_before = _get_partition_before_if_exists(first_partition,
                                                       all_partition_names)
    partition_after = _get_partition_after_if_exists(last_partition,
                                                     all_partition_names)

    if partition_before:
        partition_names = _insert_adjacent_partition(partition_before,
                                                     to=partition_names)
    elif partition_after:
        partition_names = _insert_adjacent_partition(partition_after,
                                                     to=partition_names)

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


def drop_rows_from_data(df, rows, index_name):
    index_col = df.select([index_name])
    rows_to_drop = _table_utils.filter_arrow_table(index_col, rows, index_name)
    rows_array = rows_to_drop[index_name]
    _raise_if_rows_not_in_index(rows_array)
    df = _drop_rows(df, rows_array, index_name)
    _raise_if_all_rows_is_dropped(df)
    return df


def _drop_rows(df, rows, index_name):
    index = df[index_name]
    mask = pa.compute.is_in(index, value_set=rows)
    mask = pa.compute.invert(mask)
    df = df.filter(mask)
    return df


def _raise_if_rows_not_in_index(rows):
    if rows.null_count > 0:
        raise ValueError(f"Some rows not in stored table")


def _raise_if_all_rows_is_dropped(df):
    if not df:
        raise IndexError("Can't drop all rows from stored table")


# ----------------- drop_columns ------------------


def can_drop_cols_from_table(cols, table_path):
    Connection.is_connected()
    _raise_if.table_not_exists(table_path)
    _raise_if.cols_argument_is_not_supported_dtype(cols)
    _raise_if.cols_argument_items_is_not_str(cols)

    raise_if = CheckDropCols(cols, table_path)
    raise_if.trying_to_drop_index_col()
    raise_if.cols_are_not_in_stored_data()
    raise_if.trying_to_drop_all_cols()


class CheckDropCols:

    def __init__(self, cols, table_path):
        self._table_data = Metadata(table_path, 'table')
        self._index_name = self._table_data["index_name"]
        self.cols = cols

        self._stored_cols = self._get_stored_cols()
        self._dropped_cols = self._get_dropped_cols()

    def _get_stored_cols(self):
        stored_cols = self._table_data["columns"]
        stored_cols.remove(self._index_name)
        return set(stored_cols)

    def _get_dropped_cols(self):
        dropped_cols = common.filter_cols_if_like_provided(self.cols, self._stored_cols)
        return set(dropped_cols)

    def trying_to_drop_index_col(self):
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


def create_partitions(df, rows_per_partition, partition_names):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    partition_names = partition_names[:len(partitions)]
    partitions = _table_utils.assign_ids_to_partitions(partitions, partition_names)
    return partitions


def get_partitions_to_drop(df, partition_names):
    names = [x for x in partition_names if x not in df.keys()]
    return names


def drop_partitions(table_path, partitions):
    for partition in partitions:
        _delete_partition(table_path, partition)
        _delete_partition_metadata(table_path, partition)


def _delete_partition(table_path, partition_name):
    partition_path = os.path.join(table_path, f'{partition_name}.feather')
    try:
        os.remove(partition_path)
    except PermissionError as e:
        try:
            os.system(f'cmd /k "del /f /q /a {e.filename}"')
        except Exception:
            raise PermissionError('File still opened by memory-map')


def _delete_partition_metadata(table_path, partition_name):
    partition_data = Metadata(table_path, 'partition')
    partition_names = partition_data.keys()
    partition_names = partition_names.remove(partition_name)
    del partition_data[partition_name]
