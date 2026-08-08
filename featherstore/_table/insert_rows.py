import itertools

from featherstore import _utils
from featherstore._table import _raise_if, _table_utils


def can_insert_rows(table, df, warnings):
    _raise_if.not_connected_or_table_not_exists(table)
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _raise_if.df_is_not_table_type(df, _table_utils.EDIT_TABLE_TYPES)

    table_data = table._table_data
    cols = _table_utils.get_col_names(df, has_default_index=False)
    index_name = table_data["index_name"]
    index = _table_utils.get_index_if_exists(df, index_name)
    _raise_if.df_index_or_column_names_incompatible_with_stored(
        df, table_data, cols, index=index
    )
    _raise_if.cols_does_not_match(df, table_data)


def insert_data(df, *, to):
    index_name = _table_utils.get_index_name(df)
    _raise_if.index_values_in_stored_data(to, df, index_name, all_must_be_in=False)

    df = _table_utils.concat_arrow_tables(to, df)
    df = _table_utils.sort_arrow_table(df, by=index_name)
    return df


def create_partitions(df, rows_per_partition, partition_names, all_partition_names):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    new_partition_names = _insert_new_partition_ids(
        partitions, partition_names, all_partition_names
    )
    partitions = _table_utils.assign_ids_to_partitions(partitions, new_partition_names)
    return partitions


def _insert_new_partition_ids(partitioned_df, partition_names, all_partition_names):
    num_partitions = len(partitioned_df)
    num_partition_names = len(partition_names)
    num_names_to_make = num_partitions - num_partition_names
    subsequent_partition = _table_utils.get_next_item(
        item=partition_names[-1], sequence=all_partition_names
    )
    new_partition_names = _make_partition_names(
        num_names_to_make, partition_names, subsequent_partition
    )
    return new_partition_names


def _make_partition_names(num_names, partition_names, subsequent_partition):
    last_id = _table_utils.convert_partition_id_to_float(partition_names[-1])
    subsequent_partition_exists = subsequent_partition is not None
    if subsequent_partition_exists:
        subsequent_id = _table_utils.convert_partition_id_to_float(subsequent_partition)
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

    rows_are_continuous = all(
        a.as_py() + 1 == b.as_py() for a, b in itertools.pairwise(rows)
    )
    if first_row_value > last_stored_value and rows_are_continuous or len(rows) == 0:
        _has_still_default_index = True
    else:
        _has_still_default_index = False
    return _has_still_default_index
