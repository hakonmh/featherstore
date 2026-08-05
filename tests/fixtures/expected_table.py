import pandas as pd

from .convert_table import convert_table
from .misc import format_arrow_table, sort_table


def merge_rows(df, other, *, as_arrow=False):
    new_df = sort_table(pd.concat([df, other]))
    if as_arrow:
        new_df = convert_table(new_df, to="arrow")
        new_df = format_arrow_table(new_df)
    return new_df


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
