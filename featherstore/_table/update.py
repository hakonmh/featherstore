import pandas as pd
from featherstore._metadata import Metadata
from featherstore._table.common import _check_index_constraints


def can_update_table(df, table_path, table_exists):
    if not table_exists:
        raise FileNotFoundError("Table doesn't exist")

    if not isinstance(df, (pd.DataFrame, pd.Series)):
        raise TypeError(
            f"'df' must be a pd.DataFrame or pd.Series (is type {type(df)})"
        )

    _check_index_constraints(df)

    stored_data_cols = Metadata(table_path, "table")["columns"]
    columns_not_in_stored_data = set(df.columns) - set(stored_data_cols)
    if columns_not_in_stored_data:
        raise ValueError(f"Columns {columns_not_in_stored_data} not in stored table")

    stored_index_type = Metadata(table_path, "table")["index_dtype"]
    index_type = str(df.index.dtype)
    if 'datetime' in index_type:
        index_type = 'datetime64'
    if index_type != stored_index_type:
        raise ValueError("Index types do not match")


def update_data(old_df, *, to):
    new_data = to
    old_df = old_df.to_pandas()
    _check_if_all_rows_is_in_old_data(old_df, new_data)

    new_data = _coerce_column_dtypes(new_data, to=old_df)
    old_df.loc[new_data.index, new_data.columns] = new_data
    df = old_df
    return df


def _check_if_all_rows_is_in_old_data(old_df, df):
    index = df.index
    old_index = old_df.index
    rows_not_in_old_df = not all(index.isin(old_index))
    if rows_not_in_old_df:
        raise ValueError(f"Some rows not in stored table")


def _coerce_column_dtypes(df, *, to):
    cols = df.columns
    dtypes = to[cols].dtypes
    try:
        df = df.astype(dtypes)
    except ValueError:
        raise TypeError("New and old column dtypes do not match")
    return df
