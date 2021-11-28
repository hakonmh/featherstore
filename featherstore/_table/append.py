from pyarrow.compute import add

from featherstore.connection import Connection
from featherstore import _metadata
from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from featherstore._table import _raise_if
from featherstore._table import common


def can_append_table(
    df,
    warnings,
    table_path,
):
    Connection.is_connected()
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _raise_if.table_not_exists(table_path)
    _raise_if.df_is_not_supported_table_dtype(df)
    _raise_if.columns_does_not_match(df, table_path)
    _raise_if_append_data_index_not_gt_stored_data_index(df, table_path)


def _raise_if_append_data_index_not_gt_stored_data_index(df, table_path):
    has_default_index = _metadata.Metadata(table_path, "table")["has_default_index"]
    append_data_start = _get_first_append_value(df, table_path,
                                                has_default_index)
    stored_data_end = _metadata.get_partition_attr(table_path, 'max')[-1]
    if append_data_start <= stored_data_end:
        raise ValueError(
            f"New_data.index can't be <= old_data.index[-1] ({append_data_start}"
            f" <= {stored_data_end})")


def _get_first_append_value(df, table_path, has_default_index):
    if has_default_index:
        append_data_start = _get_default_index_start(table_path)
    else:
        append_data_start = _get_non_default_index_start(df, table_path)
    return append_data_start


def _get_default_index_start(table_path):
    stored_data_end = _metadata.get_partition_attr(table_path, 'max')[-1]
    append_data_start = int(stored_data_end) + 1
    return append_data_start


def _get_non_default_index_start(df, table_path):
    index_col = _metadata.Metadata(table_path, "table")["index_name"]
    first_row = df[:1]
    first_row = common._convert_to_pyarrow_table(first_row)
    append_data_start = first_row[index_col][0].as_py()
    return append_data_start


def format_default_index(df, table_path):
    """Formats the appended data's index to continue from where the stored
    data's index stops
    """
    index_col = df[DEFAULT_ARROW_INDEX_NAME]
    first_value = _get_default_index_start(table_path)

    formatted_index_col = add(index_col, first_value)

    df = df.drop([DEFAULT_ARROW_INDEX_NAME])
    df = df.append_column(DEFAULT_ARROW_INDEX_NAME, formatted_index_col)
    return df


def sort_columns(df, columns):
    columns_not_sorted = df.column_names != columns
    if columns_not_sorted:
        df = df.select(columns)
    return df


def append_new_partition_ids(partitioned_df, last_partition_id):
    partition_ids = [last_partition_id]

    num_partitions = len(partitioned_df)
    range_start = common._convert_partition_id_to_int(last_partition_id) + 1
    range_end = range_start + num_partitions - 1

    for partition_num in range(range_start, range_end):
        partition_id = common._convert_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids
