import warnings

import pandas as pd
import pyarrow as pa

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

from .convert_table import convert_table
from .misc import (
    df_has_default_index,
    drop_default_index_if_exists,
    format_arrow_table,
)
from .split_table import split_table


from . import _utils


def parse_astype(astype):
    backend = astype.split('[')[0]
    return backend, '[series]' in astype


def write_index_for_astype(pdf, astype, *, original_df=None):
    if astype.startswith('pandas'):
        return None
    index_name = None
    if original_df is not None:
        index_name = _utils.get_index_name(original_df)
    index_name = index_name or (pdf.index.name or DEFAULT_ARROW_INDEX_NAME)
    if pdf.index.name:
        index_name = pdf.index.name
    return index_name


def arrow_table_with_index(pdf, index_name=None):
    index_name = index_name or pdf.index.name or DEFAULT_ARROW_INDEX_NAME
    arrow = convert_table(pdf, to='arrow')
    if index_name not in arrow.column_names:
        arrow = arrow.add_column(0, index_name, pa.array(pdf.index))
    return format_arrow_table(arrow)


def _pandas_table(df, *, as_series, single_col=False):
    if isinstance(df, pd.Series):
        return df
    if as_series or single_col:
        return df.squeeze(axis=1)
    return df


def _is_single_column(pdf):
    if isinstance(pdf, pd.Series):
        return True
    return pdf.shape[1] == 1


def convert_row_edit_tables(original_pd, change_pd, expected_pd, *, astype,
                            drop_default_index=False):
    backend, as_series = parse_astype(astype)
    single_col = _is_single_column(expected_pd)

    if backend == 'pandas':
        return (
            _pandas_table(original_pd, as_series=as_series, single_col=single_col),
            _pandas_table(change_pd, as_series=as_series, single_col=single_col),
            _pandas_table(expected_pd, as_series=as_series, single_col=single_col),
        )

    index_name = expected_pd.index.name or DEFAULT_ARROW_INDEX_NAME

    def _to_arrow(pdf):
        if isinstance(pdf, pd.Series):
            pdf = pdf.to_frame()
        return arrow_table_with_index(pdf, index_name)

    original_df = _to_arrow(original_pd)
    change_df = _to_arrow(change_pd)
    expected = _to_arrow(expected_pd)
    if drop_default_index and df_has_default_index(expected):
        expected = drop_default_index_if_exists(expected)
    if backend == 'polars':
        original_df = convert_table(original_df, to='polars', as_series=False)
        change_df = convert_table(change_df, to='polars', as_series=False)
        expected = convert_table(expected, to='polars', as_series=False)
    return original_df, change_df, expected


def convert_col_edit_tables(original_pd, new_cols_pd, expected_pd, *, astype):
    backend, as_series = parse_astype(astype)
    single_col = expected_pd.shape[1] == 1

    if backend == 'pandas':
        new_cols = new_cols_pd.squeeze(axis=1) if as_series else new_cols_pd
        original_df = _pandas_table(
            original_pd, as_series=as_series, single_col=single_col)
        expected = _pandas_table(
            expected_pd, as_series=as_series, single_col=single_col)
        return original_df, new_cols, expected

    index_name = expected_pd.index.name or DEFAULT_ARROW_INDEX_NAME
    full = arrow_table_with_index(expected_pd, index_name)
    original_df, new_cols = split_table(
        full, cols=list(new_cols_pd.columns), keep_index=True, index_name=index_name)
    expected = format_arrow_table(full)
    if df_has_default_index(expected):
        expected = drop_default_index_if_exists(expected)
    if backend == 'polars':
        original_df = convert_table(original_df, to='polars', as_series=False)
        new_cols = convert_table(new_cols, to='polars', as_series=False)
        expected = convert_table(expected, to='polars', as_series=False)
    return original_df, new_cols, expected


def expected_after_update(df, update_df):
    expected = df.copy()
    rows = update_df.index
    if isinstance(df, pd.Series):
        expected.loc[rows] = update_df
    else:
        cols = update_df.columns
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            expected.loc[rows, cols] = update_df
    return expected
