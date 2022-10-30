import pandas as pd
from pyarrow.compute import add

from featherstore.connection import Connection
from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from featherstore._table import _raise_if
from featherstore._table import _table_utils


def can_append_table(table, df, warnings):
    Connection._raise_if_not_connected()
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _raise_if.table_not_exists(table)
    _raise_if.df_is_not_supported_table_type(df)

    table_data = table._table_data
    cols = _table_utils.get_col_names(df, has_default_index=False)
    _raise_if.index_name_not_same_as_stored_index(df, table_data)
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if.index_type_not_same_as_stored_index(df, table_data)
    _raise_if.cols_does_not_match(df, table_data)

    has_default_index = table_data['has_default_index']
    index_name = table_data['index_name']

    pd_index = _table_utils.get_pd_index_if_exists(df, index_name)
    index_is_provided = pd_index is not None
    if not has_default_index or index_is_provided:
        if not _index_provided_is_default(pd_index):
            _raise_if_append_data_not_ordered_after_stored_data(df, table_data,
                                                                table._partition_data)

    raise_if_index_not_exist(pd_index, has_default_index)
    _raise_if.index_values_contains_duplicates(pd_index)


def _index_provided_is_default(pd_index):
    default_rangeindex = pd.RangeIndex(len(pd_index))
    return pd_index.equals(default_rangeindex)


def _raise_if_append_data_not_ordered_after_stored_data(df, table_data, partition_data):
    append_data_start = _get_first_append_value(df, table_data)
    stored_data_end = _get_last_stored_value(partition_data)
    if append_data_start <= stored_data_end:
        raise ValueError(
            f"New_data.index can't be <= old_data.index[-1] ({append_data_start}"
            f" <= {stored_data_end})")


def _get_first_append_value(df, table_data):
    append_data_start = _get_non_default_index_start(df, table_data)
    return append_data_start


def _get_non_default_index_start(df, table_data):
    index_col = table_data["index_name"]
    first_row = _get_first_row(df)
    first_row = _table_utils.convert_to_arrow(first_row)
    append_data_start = first_row[index_col][0].as_py()
    return append_data_start


def _get_first_row(df):
    if isinstance(df, pd.DataFrame):
        return df.iloc[:1]
    else:
        return df[:1]


def _get_last_stored_value(partition_data):
    df = partition_data
    last_partition_name = df.keys()[-1]
    stored_data_end = df[last_partition_name]['max']
    return stored_data_end


def raise_if_index_not_exist(index, has_default_index):
    index_not_provided = index is None
    if index_not_provided and not has_default_index:
        raise ValueError("Must provide index")


def format_default_index(table, df):
    """Formats the appended data's index to continue from where the stored
    data's index stops
    """
    index_col = df[DEFAULT_ARROW_INDEX_NAME]
    first_value = _get_default_index_start(table._partition_data)

    formatted_index_col = add(index_col, first_value)

    df = df.drop([DEFAULT_ARROW_INDEX_NAME])
    df = df.append_column(DEFAULT_ARROW_INDEX_NAME, formatted_index_col)
    return df


def _get_default_index_start(partition_data):
    stored_data_end = _get_last_stored_value(partition_data)
    append_data_start = int(stored_data_end) + 1
    return append_data_start


def append_data(df, *, to):
    df = _table_utils.concat_arrow_tables(to, df)
    return df


def create_partitions(df, rows_per_partition, last_partition_name):
    partitions = _table_utils.make_partitions(df, rows_per_partition)
    new_partition_names = _table_utils.append_new_partition_ids(len(partitions),
                                                                [last_partition_name])
    partitions = _table_utils.assign_ids_to_partitions(partitions, new_partition_names)
    return partitions
