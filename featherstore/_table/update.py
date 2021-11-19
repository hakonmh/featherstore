import pandas as pd
import pyarrow as pa

from featherstore._metadata import Metadata, _get_index_dtype
from featherstore._table.common import (_check_index_constraints,
                                        _check_column_constraints,
                                        _coerce_column_dtypes)


def can_update_table(df, table_path, table_exists):
    if not table_exists:
        raise FileNotFoundError("Table doesn't exist")

    if not isinstance(df, (pd.DataFrame, pd.Series)):
        raise TypeError(
            f"'df' must be a pd.DataFrame or pd.Series (is type {type(df)})")

    if isinstance(df, pd.Series):
        cols = [df.name]
    else:
        cols = df.columns

    _check_index_constraints(df.index)
    _check_column_constraints(cols)

    index_name = Metadata(table_path, "table")['index_name']
    stored_data_cols = Metadata(table_path, "table")["columns"]
    stored_data_cols.remove(index_name)
    columns_not_in_stored_data = set(cols) - set(stored_data_cols)
    if columns_not_in_stored_data:
        raise ValueError(
            f"Columns {columns_not_in_stored_data} not in stored table")

    # Take one row and converts it to pa.Table to check its arrow datatype
    first_row = df.head(1)
    if isinstance(first_row, pd.Series):
        first_row = first_row.to_frame()
    arrow_df = pa.Table.from_pandas(first_row, preserve_index=True)
    index_type = _get_index_dtype([arrow_df])
    stored_index_type = Metadata(table_path, "table")["index_dtype"]
    if index_type != stored_index_type:
        raise TypeError("Index types do not match")


def update_data(old_df, *, to):
    if isinstance(to, pd.Series):
        new_data = to.to_frame()
    else:
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
