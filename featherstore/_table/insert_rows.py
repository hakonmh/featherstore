from featherstore.connection import Connection
from featherstore import _utils
from featherstore._table import _raise_if
from featherstore._table import _table_utils
from featherstore._table import common


def can_insert_rows(table, df, warnings):
    Connection._raise_if_not_connected()
    _utils.raise_if_warnings_argument_is_not_valid(warnings)

    _raise_if.table_not_exists(table)
    _raise_if.df_is_not_table_type(df, _table_utils.EDIT_TABLE_TYPES)

    table_data = table._table_data
    cols = _table_utils.get_col_names(df, has_default_index=False)
    common.validate_incoming_table_schema(df, table_data, cols)
    _raise_if.cols_does_not_match(df, table_data)

    index_name = table_data["index_name"]
    index = _table_utils.get_index_if_exists(df, index_name)
    _raise_if.index_values_contains_duplicates(index)


def insert_data(df, *, to):
    index_name = _table_utils.get_index_name(df)
    _raise_if.index_values_in_stored_data(to, df, index_name, all_must_be_in=False)

    df = _table_utils.concat_arrow_tables(to, df)
    df = _table_utils.sort_arrow_table(df, by=index_name)
    return df


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
