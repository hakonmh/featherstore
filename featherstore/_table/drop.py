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
    Connection._raise_if_not_connected()
    _raise_if.table_not_exists(table_path)
    _raise_if.rows_argument_is_not_collection(rows)
    _raise_if_rows_argument_is_empty(rows)
    _raise_if.rows_argument_items_dtype_not_same_as_index(rows, table_path)


def _raise_if_rows_argument_is_empty(rows):
    if rows is not None:
        rows_is_empty = len(rows) == 0
        if rows_is_empty:
            raise IndexError(f"No rows in sequence, rows should be None or a"
                             "sequence with elements")


def get_partition_names(rows, table_path):
    partition_names = _get_partition_names(rows, table_path)
    partition_names = _get_adjacent_partition_name(partition_names, table_path)
    return partition_names


def _get_adjacent_partition_name(partitions_selected, table_path):
    """Fetches an extra partition name so we can use that partition when
    combining small partitions.
    """
    all_partition_names = Metadata(table_path, 'partition').keys()

    partition_before_first = _table_utils.get_previous_item(item=partitions_selected[0],
                                                            sequence=all_partition_names)
    partition_after_last = _table_utils.get_next_item(item=partitions_selected[-1],
                                                      sequence=all_partition_names)

    if partition_before_first:
        partition_names = _insert_adjacent_partition(partition_before_first,
                                                     to=partitions_selected)
    elif partition_after_last:
        partition_names = _insert_adjacent_partition(partition_after_last,
                                                     to=partitions_selected)
    else:
        partition_names = partitions_selected

    return partition_names


def _insert_adjacent_partition(adj_partition, *, to):
    partition_names = to.copy()
    bisect.insort(partition_names, adj_partition)
    return partition_names


def drop_rows_from_data(df, rows, index_name):
    index_col = df.select([index_name])
    rows_to_drop = _table_utils.filter_arrow_table(index_col, rows, index_name)[0]
    df = _drop_rows(df, rows_to_drop, index_name)
    _raise_if_all_rows_is_dropped(df)
    return df


def _drop_rows(df, rows, index_name):
    index = df[index_name]
    mask = pa.compute.is_in(index, value_set=rows)
    mask = pa.compute.invert(mask)
    df = df.filter(mask)
    return df


def _raise_if_all_rows_is_dropped(df):
    if not df:
        raise IndexError("Can't drop all rows from stored table")


def has_still_default_index(rows, table_metadata, partition_metadata):
    has_default_index = table_metadata["has_default_index"]
    if not has_default_index:
        return False

    if rows[0] == 'before':
        is_still_def_idx = _idx_still_default_after_dropping_rows_before(rows, partition_metadata)
    elif rows[0] == 'after':
        is_still_def_idx = _idx_still_default_after_dropping_rows_after(rows, partition_metadata)
    elif rows[0] == 'between':
        is_still_def_idx = _idx_still_default_after_dropping_rows_between(rows, partition_metadata)
    else:
        is_still_def_idx = _idx_still_default_after_dropping_rows_list(rows, partition_metadata)
    return is_still_def_idx


def _idx_still_default_after_dropping_rows_before(rows, partition_metadata):
    first_stored_value = _table_utils.get_first_stored_index_value(partition_metadata)
    first_row_value = rows[-1]
    no_values_are_removed = first_row_value < first_stored_value
    if no_values_are_removed:
        _has_still_default_index = True
    else:
        _has_still_default_index = False
    return _has_still_default_index


def _idx_still_default_after_dropping_rows_after(rows, partition_metadata):
    return True


def _idx_still_default_after_dropping_rows_between(rows, partition_metadata):
    first_stored_value = _table_utils.get_first_stored_index_value(partition_metadata)
    last_stored_value = _table_utils.get_last_stored_index_value(partition_metadata)
    after = rows[1]
    before = rows[2]
    start_after_table_end = after > last_stored_value
    end_after_table_start = before < first_stored_value

    no_values_are_removed = end_after_table_start or start_after_table_end
    values_removed_only_from_end_of_table = before > last_stored_value
    if no_values_are_removed or values_removed_only_from_end_of_table:
        _has_still_default_index = True
    else:
        _has_still_default_index = False
    return _has_still_default_index


def _idx_still_default_after_dropping_rows_list(rows, partition_metadata):
    last_stored_value = _table_utils.get_last_stored_index_value(partition_metadata)
    rows = sorted(rows)
    last_row_value = rows[-1]

    last_row_removed = last_row_value == last_stored_value
    rows_are_continuous = all(a + 1 == b for a, b in zip(rows, rows[1:]))
    values_removed_only_from_end_of_table = last_row_removed and rows_are_continuous
    if values_removed_only_from_end_of_table:
        _has_still_default_index = True
    else:
        _has_still_default_index = False
    return _has_still_default_index


# ----------------- drop_columns ------------------


def can_drop_cols_from_table(cols, table_path):
    Connection._raise_if_not_connected()
    _raise_if.table_not_exists(table_path)
    _raise_if.cols_argument_is_not_collection(cols)
    _raise_if.cols_argument_items_is_not_str(cols)
    _raise_if_cols_argument_is_empty(cols)

    raise_if = CheckDropCols(cols, table_path)
    raise_if.trying_to_drop_index_col()
    raise_if.cols_are_not_in_stored_data()
    raise_if.trying_to_drop_all_cols()


def _raise_if_cols_argument_is_empty(cols):
    if cols is not None:
        cols_is_empty = len(cols) == 0
        if cols_is_empty:
            raise IndexError("No cols in sequence, cols should be None or a"
                             "sequence with elements")


class CheckDropCols:

    def __init__(self, cols, table_path):
        self._table_data = Metadata(table_path, 'table')
        self._index_name = self._table_data["index_name"]
        self.cols = common.format_cols_arg_if_provided(cols)

        self._stored_cols = self._get_stored_cols()
        self._dropped_cols = self._get_dropped_cols()

    def _get_stored_cols(self):
        stored_cols = self._table_data["columns"]
        stored_cols.remove(self._index_name)
        return set(stored_cols)

    def _get_dropped_cols(self):
        if common.like_is_provided(self.cols):
            cols_to_drop = common.get_cols_like_pattern(self.cols, self._stored_cols)
        else:
            cols_to_drop = self.cols
        return set(cols_to_drop)

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
