import pandas as pd

from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._table import _table_utils
from featherstore._table._indexers import ColIndexer


def can_add_columns(table, df):
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


def _raise_if_col_name_already_in_table(cols, table_data):
    stored_cols = table_data["columns"]
    cols = ColIndexer(cols)

    cols = cols.like(stored_cols)
    some_cols_in_stored_cols = set(stored_cols) - (set(stored_cols) - set(cols))
    if some_cols_in_stored_cols:
        raise IndexError("Column name already exists in table")


def _raise_if_num_rows_does_not_match(df, table_data):
    stored_table_length = table_data["num_rows"]

    new_cols_length = len(df)

    if new_cols_length != stored_table_length:
        raise IndexError(f"Length of new cols ({new_cols_length}) doesn't match "
                         f"length of stored data ({stored_table_length})")


def add_columns(old_df, df, index):
    # TODO: Use arrow instead
    old_df, df = _format_tables(old_df, df)
    _raise_if_rows_not_in_old_data(old_df, df)
    df = _add_cols(old_df, df, index)
    return df


def _format_tables(old_df, df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    else:
        df = df

    index_not_sorted = not df.index.is_monotonic_increasing
    if index_not_sorted:
        df = df.sort_index()

    old_df = old_df.to_pandas()
    return old_df, df


def _raise_if_rows_not_in_old_data(old_df, df):
    index = df.index
    old_index = old_df.index
    if not index.equals(old_index):
        raise ValueError("New and old indices doesn't match")


def _add_cols(old_df, df, index):
    new_cols = df.columns.tolist()
    cols = old_df.columns.tolist()
    df = old_df.join(df)

    if index == -1:
        cols.extend(new_cols)
    else:
        for col in new_cols:
            cols.insert(index, col)
            index += 1
    df = df[cols]
    return df


def create_partitions(df, rows_per_partition, partition_names):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    new_partition_names = _table_utils.add_new_partition_ids(partitions, partition_names)
    partitions = _table_utils.assign_ids_to_partitions(partitions, new_partition_names)
    return partitions
