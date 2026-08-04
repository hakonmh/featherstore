import pyarrow as pa
import pyarrow.compute as pc

from featherstore.connection import Connection
from featherstore._table import _raise_if
from featherstore._table import _table_utils
from featherstore._table import common


def can_update_table(table, df):
    Connection._raise_if_not_connected()

    _raise_if.table_not_exists(table)
    _raise_if.df_is_not_table_type(df, _table_utils.EDIT_TABLE_TYPES)

    table_data = table._table_data
    cols = _table_utils.get_col_names(df, has_default_index=False)
    common.validate_incoming_table_schema(df, table_data, cols)
    _raise_if.cols_not_in_table(cols, table_data)

    index_name = table_data["index_name"]
    index = _table_utils.get_index_if_exists(df, index_name)
    _raise_if.index_values_contains_duplicates(index)


def update_data(old_df, *, to):
    """Apply updates to an Arrow table without converting to Pandas."""
    index_name = _table_utils.get_index_name(old_df)
    _raise_if.index_values_in_stored_data(old_df, to, index_name, all_must_be_in=True)

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
