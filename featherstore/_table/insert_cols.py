from numbers import Integral

import pyarrow.compute as pc

from featherstore import _utils
from featherstore._table import _raise_if, _table_utils
from featherstore.exceptions import IndexMismatchError


def can_insert_columns(table, df, idx, warnings):
    _raise_if.not_connected_or_table_not_exists(table)
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _raise_if.df_is_not_table_type(df, _table_utils.EDIT_TABLE_TYPES)

    table_data = table._table_data
    index_name = table_data["index_name"]
    cols = _table_utils.get_data_col_names(df, index_name=index_name)
    index = _table_utils.get_index_if_exists(df, index_name)

    _raise_if.incoming_index_schema_incompatible_with_stored(
        df, table_data, cols, index=index
    )
    _raise_if.index_in_cols(cols, table_data)
    _raise_if.cols_already_in_table(cols, table_data)
    _raise_if.row_count_does_not_match_stored(df, table_data)
    _raise_if_idx_is_invalid(cols, idx)


def _raise_if_idx_is_invalid(cols, idx):
    num_new_cols = len(cols)

    if _table_utils.is_collection(idx):
        if len(idx) != num_new_cols:
            raise ValueError(
                f"Length of 'idx' ({len(idx)}) != number of new columns "
                f"({num_new_cols})"
            )
        for position in idx:
            if not isinstance(position, Integral):
                raise TypeError(
                    f"Elements in 'idx' must be of type int (is type {type(position)})"
                )
    elif not isinstance(idx, Integral):
        raise TypeError(f"'idx' must be an int (is type {type(idx)})")


def insert_columns(old_df, df, index):
    index_name = _table_utils.get_index_name(old_df)
    _raise_if_indices_do_not_match(old_df, df, index_name)
    return _insert_cols(old_df, df, index, index_name)


def _raise_if_indices_do_not_match(old_df, df, index_name):
    old_index = old_df[index_name]
    new_index = df[index_name]
    if len(old_index) != len(new_index):
        raise IndexMismatchError("New and old indices doesn't match")
    if not old_index.equals(new_index):
        mismatched = pc.invert(pc.equal(old_index, new_index))
        raise IndexMismatchError(
            "New and old indices doesn't match "
            f"(new={new_index.filter(mismatched).to_pylist()}, "
            f"stored={old_index.filter(mismatched).to_pylist()})"
        )


def _insert_cols(old_df, df, index, index_name):
    new_cols = [c for c in df.column_names if c != index_name]
    if index == -1:
        return _append_columns(old_df, df, new_cols)
    if _table_utils.is_collection(index):
        return _insert_columns_at_positions(old_df, df, new_cols, index)
    return _insert_columns_block(old_df, df, new_cols, index)


def _append_columns(old_df, df, new_cols):
    result = old_df
    for col in new_cols:
        result = result.append_column(col, df[col])
    return result


def _insert_columns_at_positions(old_df, df, new_cols, positions):
    result = old_df
    for col, position in zip(new_cols, positions):
        result = result.add_column(position + 1, col, df[col])
    return result


def _insert_columns_block(old_df, df, new_cols, index):
    result = old_df
    insert_at = index + 1
    for col in new_cols:
        result = result.add_column(insert_at, col, df[col])
        insert_at += 1
    return result


def create_partitions(df, rows_per_partition, partition_names):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    new_partition_names = _table_utils.add_new_partition_ids(
        partitions, partition_names
    )
    partitions = _table_utils.assign_ids_to_partitions(partitions, new_partition_names)
    return partitions
