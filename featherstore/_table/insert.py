import pandas as pd
import pyarrow as pa

from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._table import _table_utils


def can_insert_table(table, df):
    Connection._raise_if_not_connected()

    _raise_if.table_not_exists(table)
    _raise_if.df_is_not_pandas_table(df)

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns.tolist()

    _raise_if.index_name_not_same_as_stored_index(df, table._table_data)
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if.index_values_contains_duplicates(df.index)
    _raise_if.index_type_not_same_as_stored_index(df, table._table_data)
    _raise_if.cols_does_not_match(df, table._table_data)


def insert_data(df, *, to):
    index_name = _table_utils.get_index_name(df)
    _raise_if_rows_in_old_data(to, df, index_name)

    df = _table_utils.concat_arrow_tables(to, df)
    df = _table_utils.sort_arrow_table(df, by=index_name)
    return df


def _raise_if_rows_in_old_data(old_df, df, index_name):
    index = df[index_name]
    old_index = old_df[index_name]

    is_in = pa.compute.is_in(index, value_set=old_index)
    rows_in_old_df = pa.compute.any(is_in).as_py()
    if rows_in_old_df:
        raise ValueError("Some rows already in stored table")


def create_partitions(df, rows_per_partition, partition_names, all_partition_names):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    new_partition_names = _insert_new_partition_ids(partitions, partition_names,
                                                    all_partition_names)
    partitions = _table_utils.assign_ids_to_partitions(partitions, new_partition_names)
    return partitions


def _insert_new_partition_ids(partitioned_df, partition_names, all_partition_names):
    num_partitions = len(partitioned_df)
    num_partition_names = len(partition_names)
    num_names_to_make = num_partitions - num_partition_names
    subsequent_partition = _table_utils.get_next_item(item=partition_names[-1],
                                                      sequence=all_partition_names)
    new_partition_names = _make_partition_names(num_names_to_make,
                                                partition_names,
                                                subsequent_partition)
    return new_partition_names


def _make_partition_names(num_names, partition_names, subsequent_partition):
    last_id = _table_utils.convert_partition_id_to_int(partition_names[-1])
    subsequent_partition_exists = subsequent_partition is not None
    if subsequent_partition_exists:
        subsequent_id = _table_utils.convert_partition_id_to_int(subsequent_partition)
        increment = (subsequent_id - last_id) / (num_names + 1)
    else:  # Called only when partition_names[-1] is the end of the table
        increment = 1

    new_partition_names = partition_names.copy()
    for partition_num in range(1, num_names + 1):
        new_partition_id = last_id + increment * partition_num
        new_partition_id = _table_utils.convert_int_to_partition_id(new_partition_id)
        new_partition_names.append(new_partition_id)

    return sorted(new_partition_names)


def has_still_default_index(table, df):
    has_default_index = table._table_data["has_default_index"]
    if not has_default_index:
        return False

    index_name = table._table_data["index_name"]
    rows = df[index_name]
    last_stored_value = _table_utils.get_last_stored_index_value(table._partition_data)
    first_row_value = rows[0].as_py()

    rows_are_continuous = all(a.as_py() + 1 == b.as_py() for a, b in zip(rows, rows[1:]))
    if first_row_value > last_stored_value and rows_are_continuous:
        _has_still_default_index = True
    elif len(rows) == 0:
        _has_still_default_index = True
    else:
        _has_still_default_index = False
    return _has_still_default_index
