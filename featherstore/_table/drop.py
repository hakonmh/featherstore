import os
import bisect

import pyarrow as pa

from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._table import _table_utils
from featherstore._table.read import get_partition_names as _get_partition_names
from featherstore._table._indexers import ColIndexer, RowIndexer


def can_drop_rows_from_table(table, rows):
    Connection._raise_if_not_connected()
    _raise_if.table_not_exists(table)
    _raise_if.rows_argument_is_not_collection(rows)

    rows = RowIndexer(rows)
    _raise_if.rows_items_not_all_same_type(rows)
    _raise_if.rows_argument_items_type_not_same_as_index(rows, table._table_data)


def get_partition_names(table, rows):
    partition_names = _get_partition_names(table, rows)
    partition_names = _get_adjacent_partition_name(table, partition_names)
    return partition_names


def _get_adjacent_partition_name(table, partitions_selected):
    """Fetches an extra partition name so we can use that partition when
    combining small partitions.
    """
    all_partition_names = table._partition_data.keys()

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


def has_still_default_index(table, rows):
    has_default_index = table._table_data["has_default_index"]
    if not has_default_index:
        return False

    partition_metadata = table._partition_data
    if rows.keyword == 'before':
        is_still_def_idx = _idx_still_default_after_dropping_rows_before(rows, partition_metadata)
    elif rows.keyword == 'after':
        is_still_def_idx = True
    elif rows.keyword == 'between':
        is_still_def_idx = _idx_still_default_after_dropping_rows_between(rows, partition_metadata)
    elif rows:
        is_still_def_idx = _idx_still_default_after_dropping_rows_list(rows, partition_metadata)
    else:
        is_still_def_idx = True

    return is_still_def_idx


def _idx_still_default_after_dropping_rows_before(rows, partition_metadata):
    first_stored_value = _table_utils.get_first_stored_index_value(partition_metadata)
    before = rows[0]
    no_values_are_removed = before < first_stored_value
    if no_values_are_removed:
        _has_still_default_index = True
    else:
        _has_still_default_index = False
    return _has_still_default_index


def _idx_still_default_after_dropping_rows_between(rows, partition_metadata):
    first_stored_value = _table_utils.get_first_stored_index_value(partition_metadata)
    last_stored_value = _table_utils.get_last_stored_index_value(partition_metadata)
    after = rows[0]
    before = rows[1]
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


def can_drop_cols_from_table(table, cols):
    Connection._raise_if_not_connected()
    _raise_if.table_not_exists(table)
    _raise_if.cols_argument_is_not_collection(cols)

    raise_if = CheckDropCols(cols, table)
    raise_if.items_not_str()
    raise_if.index_is_dropped()
    raise_if.cols_are_not_in_stored_data()
    raise_if.all_rows_are_dropped()


class CheckDropCols:

    def __init__(self, cols, table):
        self._table_path = table._table_path
        self._table_data = table._table_data
        self._cols = ColIndexer(cols)

        self._stored_cols = self._get_stored_cols()
        self._dropped_cols = self._get_dropped_cols()

    def _get_stored_cols(self):
        stored_cols = self._table_data["columns"]
        index_name = self._table_data["index_name"]
        stored_cols.remove(index_name)
        return set(stored_cols)

    def _get_dropped_cols(self):
        dropped_cols = self._cols.like(self._stored_cols)
        return set(dropped_cols)

    def index_is_dropped(self):
        _raise_if.index_in_cols(self._dropped_cols, self._table_data)

    def cols_are_not_in_stored_data(self):
        some_cols_not_in_stored_cols = bool(self._dropped_cols - self._stored_cols)
        if some_cols_not_in_stored_cols:
            raise IndexError("Trying to drop a column not found in table")

    def all_rows_are_dropped(self):
        trying_to_drop_all_cols = not bool(self._stored_cols - self._dropped_cols)
        if trying_to_drop_all_cols:
            raise IndexError("Can't drop all columns. To drop full table, use 'drop_table()'")

    def items_not_str(self):
        _raise_if.cols_argument_items_is_not_str(self._cols.values())


def drop_cols_from_data(df, cols):
    return df.drop(cols.values())


def create_partitions(df, rows_per_partition, partition_names):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    partition_names = partition_names[:len(partitions)]
    partitions = _table_utils.assign_ids_to_partitions(partitions, partition_names)
    return partitions


def get_partitions_to_drop(df, partition_names):
    names = [x for x in partition_names if x not in df.keys()]
    return names


def drop_partitions(table, partitions):
    for partition in partitions:
        _delete_partition(table._table_path, partition)
        _delete_partition_metadata(table, partition)


def _delete_partition(table_path, partition_name):
    partition_path = os.path.join(table_path, f'{partition_name}.feather')
    try:
        os.remove(partition_path)
    except PermissionError as e:
        try:
            os.system(f'cmd /k "del /f /q /a {e.filename}"')
        except Exception:
            raise PermissionError('File still opened by memory-map')


def _delete_partition_metadata(table, partition_name):
    del table._partition_data[partition_name]
