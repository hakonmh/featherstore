import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._table import _table_utils


def can_update_table(table, df):
    Connection._raise_if_not_connected()

    _raise_if.table_not_exists(table)
    _raise_if.df_is_not_table_type(df, _table_utils.EDIT_TABLE_TYPES)

    table_data = table._table_data
    cols = _get_col_names(df, table_data)
    index_name = table_data["index_name"]
    index = _table_utils.get_index_if_exists(df, index_name)

    _raise_if.index_name_not_same_as_stored_index(df, table_data)
    _raise_if.col_names_contains_duplicates(cols)
    _raise_if.index_type_not_same_as_stored_index(df, table_data)
    _raise_if.index_values_contains_duplicates(index)
    _raise_if.cols_not_in_table(cols, table_data)


def _get_col_names(df, table_data):
    if isinstance(df, pd.Series):
        return [df.name]
    if isinstance(df, pd.DataFrame):
        return df.columns.tolist()
    index_name = table_data["index_name"]
    cols = _table_utils.get_col_names(df, has_default_index=False)
    return [c for c in cols if c != index_name]


def update_data(old_df, *, to):
    """Apply updates to an Arrow table without converting to Pandas."""
    index_name = _table_utils.get_index_name(old_df)
    _raise_if_rows_is_not_in_old_data(old_df, to, index_name)

    old_index = old_df[index_name]
    new_index = to[index_name]
    mask = pc.is_in(old_index, value_set=new_index)
    indices_in_new = pc.index_in(old_index, value_set=new_index)
    safe_indices = pc.fill_null(indices_in_new, 0)

    result = old_df
    for col_name in to.column_names:
        if col_name == index_name:
            continue
        old_col = result[col_name]
        new_col = to[col_name]
        try:
            new_col = new_col.cast(old_col.type)
        except (pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError, TypeError):
            raise TypeError("New and old column dtypes do not match") from None

        replacements = new_col.take(safe_indices)
        updated = pc.if_else(mask, replacements, old_col)
        col_idx = result.column_names.index(col_name)
        result = result.set_column(col_idx, col_name, updated)
    return result


def _raise_if_rows_is_not_in_old_data(old_df, df, index_name):
    index = df[index_name]
    old_index = old_df[index_name]
    is_in = pc.is_in(index, value_set=old_index)
    if not pc.all(is_in).as_py():
        raise ValueError("Some rows not in stored table")
