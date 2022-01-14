import pyarrow as pa
from pyarrow.compute import add

from featherstore.connection import Connection
from featherstore import _metadata
from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from featherstore._table import _raise_if
from featherstore._table import _table_utils


def can_append_table(
    df,
    warnings,
    table_path,
):
    Connection.is_connected()
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _raise_if.table_not_exists(table_path)
    _raise_if.df_is_not_supported_table_dtype(df)
    _raise_if.index_dtype_not_same_as_stored_index(df, table_path)
    _raise_if.cols_does_not_match(df, table_path)

    has_default_index = _metadata.Metadata(table_path, 'table')['has_default_index']
    index_name = _metadata.Metadata(table_path, 'table')['index_name']

    pd_index = _table_utils.get_pd_index_if_exists(df, index_name)
    index_is_provided = pd_index is not None
    if not has_default_index or index_is_provided:
        _raise_if_append_data_not_ordered_after_stored_data(df, table_path)

    raise_if_index_not_exist(pd_index, has_default_index)

    if index_is_provided:
        _raise_if.index_is_not_supported_dtype(pd_index)
        _raise_if.index_values_contains_duplicates(pd_index)


def _raise_if_append_data_not_ordered_after_stored_data(df, table_path):
    append_data_start = _get_first_append_value(df, table_path)
    stored_data_end = _get_last_stored_value(table_path)
    if append_data_start <= stored_data_end:
        raise ValueError(
            f"New_data.index can't be <= old_data.index[-1] ({append_data_start}"
            f" <= {stored_data_end})")


def raise_if_index_not_exist(index, has_default_index):
    index_not_provided = index is None
    if index_not_provided and not has_default_index:
        raise ValueError("Must provide index")


def _get_first_append_value(df, table_path):
    append_data_start = _get_non_default_index_start(df, table_path)
    return append_data_start


def _get_default_index_start(table_path):
    stored_data_end = _get_last_stored_value(table_path)
    append_data_start = int(stored_data_end) + 1
    return append_data_start


def _get_non_default_index_start(df, table_path):
    index_col = _metadata.Metadata(table_path, "table")["index_name"]
    first_row = _table_utils.get_first_row(df)
    first_row = _table_utils.convert_to_arrow(first_row)
    append_data_start = first_row[index_col][0].as_py()
    return append_data_start


def _get_last_stored_value(table_path):
    df = _metadata.Metadata(table_path, 'partition')
    last_partition_name = df.keys()[-1]
    stored_data_end = df[last_partition_name]['max']
    return stored_data_end


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


def append_data(df, *, to):
    cols = to.column_names
    df = _sort_cols(df, cols=cols)
    try:
        full_table = pa.concat_tables([to, df])
    except pa.ArrowInvalid:
        raise TypeError("Column dtypes doesn't match")
    return full_table


def _sort_cols(df, cols):
    cols_not_sorted = df.column_names != cols
    if cols_not_sorted:
        df = df.select(cols)
    return df


def create_partitions(df, rows_per_partition, last_partition_name):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    new_partition_names = _append_new_partition_ids(len(partitions), last_partition_name)
    partitions = _table_utils.assign_ids_to_partitions(partitions, new_partition_names)
    return partitions


def _append_new_partition_ids(num_partitions, last_partition_id):
    partition_ids = [last_partition_id]

    range_start = _table_utils.convert_partition_id_to_int(last_partition_id) + 1
    range_end = range_start + num_partitions - 1

    for partition_num in range(range_start, range_end):
        partition_id = _table_utils.convert_int_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids
