import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc

from featherstore._table import _raise_if, _table_utils
from featherstore.exceptions import ColumnDtypeMismatchError, RowNotFoundError


def can_update_table(table, df):
    _raise_if.not_connected_or_table_not_exists(table)
    _raise_if.df_is_not_table_type(df, _table_utils.EDIT_TABLE_TYPES)

    table_data = table._table_data
    cols = _table_utils.get_col_names(df, has_default_index=False)
    index_name = table_data["index_name"]
    index = _table_utils.get_index_if_exists(df, index_name)
    _raise_if.df_index_or_column_names_incompatible_with_stored(
        df, table_data, cols, index=index
    )
    _raise_if.cols_not_in_table(cols, table_data)


def update_data(old_df, *, to):
    index_name = _table_utils.get_index_name(old_df)
    row_positions = _get_row_positions(old_df[index_name], to[index_name])
    return _update_columns(old_df, to, row_positions, index_name)


def _get_row_positions(old_index, new_index):
    """Position each updated row within ``old_index``, using Pandas' hash index
    since it outperforms Arrow's ``index_in`` for this direction of lookup.
    """
    old_index = pd.Index(old_index.combine_chunks().to_pandas())
    new_index = new_index.combine_chunks().to_pandas()
    positions = old_index.get_indexer(new_index)

    missing = positions < 0
    if missing.any():
        missing_values = new_index[missing].tolist()
        raise RowNotFoundError(f"Some rows not in stored table ({missing_values})")
    return positions


def _update_columns(old_df, to, row_positions, index_name):
    cols_to_update = {name for name in to.column_names if name != index_name}
    max_index = len(old_df) - 1

    arrays = []
    for col_name in old_df.column_names:
        old_col = old_df[col_name]
        if col_name not in cols_to_update:
            arrays.append(old_col)
            continue

        new_col = _cast_col(to[col_name], old_col.type)
        scattered = pc.scatter(new_col, row_positions, max_index=max_index)
        arrays.append(pc.coalesce(scattered, old_col))

    return pa.Table.from_arrays(arrays, schema=old_df.schema)


def _cast_col(col, dtype):
    try:
        return col.cast(dtype)
    except (pa.lib.ArrowInvalid, pa.lib.ArrowNotImplementedError, TypeError):
        raise ColumnDtypeMismatchError(
            "New and old column dtypes do not match"
        ) from None
