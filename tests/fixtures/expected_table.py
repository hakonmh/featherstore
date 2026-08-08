import pandas as pd

from .convert_table import convert_table
from .misc import df_has_default_index, drop_default_index_if_exists, sort_table


def merge_rows(df, other, *, as_arrow=False):
    new_df = sort_table(pd.concat([df, other]))
    if as_arrow:
        new_df = convert_table(new_df, to="arrow")
    return new_df


def update_table(df, update_df):
    expected = df.copy()
    expected.update(update_df)
    return expected


def convert_expected(df, *, to, like=None):
    """Convert expected result for assertions.

    Drops the default index when ``like`` has one. If ``like`` is omitted, drops
    it when ``df`` itself has a default index after conversion.
    """
    as_series = to.startswith("pandas")
    expected = convert_table(df, to=to, as_series=as_series, keep_index=True)
    if as_series:
        return expected
    if like is None or df_has_default_index(like):
        return drop_default_index_if_exists(expected)
    return expected


def insert_columns_expected(expected, new_cols, idx):
    expected = expected.copy()
    new_cols = new_cols.copy()
    new_cols.index = expected.index
    for offset, col_name in enumerate(new_cols.columns):
        expected.insert(idx + offset, col_name, new_cols[col_name])
    return expected


def insert_column_names_at(df, col_names, insert_idx):
    columns = df.columns.tolist()
    start, end = _column_slice_for_insert(insert_idx, len(col_names))
    columns[start:end] = col_names
    df.columns = columns
    return df


def _column_slice_for_insert(insert_idx, num_cols):
    end = insert_idx + num_cols
    if insert_idx < 0:
        insert_idx = -num_cols
        end = None
    return insert_idx, end
