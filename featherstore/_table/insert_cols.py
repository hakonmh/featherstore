from numbers import Integral

import pandas as pd

from featherstore._table import _raise_if, _table_utils
from featherstore._table._indexers import ColIndexer
from featherstore.connection import Connection
from featherstore.exceptions import (
    ColumnAlreadyExistsError,
    ColumnLengthMismatchError,
    IndexMismatchError,
)


def can_insert_columns(table, df, idx=-1):
    Connection._raise_if_not_connected()

    _raise_if.table_not_exists(table)
    _raise_if.df_is_not_pandas_table(df)

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns.tolist()
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if.index_in_cols(cols, table._table_data)
    _raise_if_col_name_already_in_table(cols, table._table_data)

    _raise_if_num_rows_does_not_match(df, table._table_data)
    _raise_if.index_values_contains_duplicates(df.index)
    _raise_if.index_type_not_same_as_stored_index(df, table._table_data)
    _raise_if_idx_is_invalid(df, idx)


def _raise_if_col_name_already_in_table(cols, table_data):
    stored_cols = table_data["columns"]
    cols = ColIndexer(cols)

    cols = cols.like(stored_cols)
    some_cols_in_stored_cols = set(stored_cols) - (set(stored_cols) - set(cols))
    if some_cols_in_stored_cols:
        existing = sorted(some_cols_in_stored_cols)
        raise ColumnAlreadyExistsError(
            f"Column names already exist in table ({existing})"
        )


def _raise_if_num_rows_does_not_match(df, table_data):
    stored_table_length = table_data["num_rows"]

    new_cols_length = len(df)

    if new_cols_length != stored_table_length:
        raise ColumnLengthMismatchError(
            f"Length of new cols ({new_cols_length}) doesn't match "
            f"length of stored data ({stored_table_length})"
        )


def _raise_if_idx_is_invalid(df, idx):
    num_new_cols = 1 if isinstance(df, pd.Series) else len(df.columns)

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
    # TODO: Use arrow instead
    old_df, df = _format_tables(old_df, df)
    _raise_if_rows_not_in_old_data(old_df, df)
    df = _insert_cols(old_df, df, index)
    return df


def _format_tables(old_df, df):
    if isinstance(df, pd.Series):
        df = df.to_frame()

    index_not_sorted = not df.index.is_monotonic_increasing
    if index_not_sorted:
        df = df.sort_index()

    old_df = old_df.to_pandas()
    return old_df, df


def _raise_if_rows_not_in_old_data(old_df, df):
    index = df.index
    old_index = old_df.index
    if not index.equals(old_index):
        raise IndexMismatchError(
            f"New and old indices doesn't match "
            f"(new={index.tolist()}, stored={old_index.tolist()})"
        )


def _insert_cols(old_df, df, index):
    new_cols = df.columns.tolist()
    cols = old_df.columns.tolist()
    df = old_df.join(df)

    if index == -1:
        cols.extend(new_cols)
    elif _table_utils.is_collection(index):
        for col, position in zip(new_cols, index):
            cols.insert(position, col)
    else:
        for col in new_cols:
            cols.insert(index, col)
            index += 1
    df = df[cols]
    return df


def create_partitions(df, rows_per_partition, partition_names):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    new_partition_names = _table_utils.add_new_partition_ids(
        partitions, partition_names
    )
    partitions = _table_utils.assign_ids_to_partitions(partitions, new_partition_names)
    return partitions
