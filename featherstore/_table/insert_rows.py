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
