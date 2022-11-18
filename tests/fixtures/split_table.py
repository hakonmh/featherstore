import pyarrow as pa
import polars as pl
import pandas as pd

from featherstore._table._table_utils import filter_arrow_table, DEFAULT_ARROW_INDEX_NAME
from featherstore._utils import filter_items_like_pattern
from featherstore._table.common import format_rows_arg

from . import _utils
from .convert_table import convert_table


def split_table(df, rows=None, cols=None, index_name=None, keep_index=False, iloc=False):
    if cols is not None and rows is not None:
        df, other = split_rows(df, rows, index_name, iloc)
        df, _ = split_cols(df, cols, index_name, keep_index)
        _, other = split_cols(other, cols, index_name, keep_index)
    elif cols is not None:
        df, other = split_cols(df, cols, index_name, keep_index)
    elif rows is not None:
        df, other = split_rows(df, rows, index_name, iloc)
    return df, other


def split_cols(df, cols, index_name=None, keep_index=False):
    df_cols, other_cols = _split_col_arg(df, cols)

    if keep_index and not isinstance(df, (pd.DataFrame, pd.Series)):
        if index_name not in df_cols:
            df_cols.insert(0, index_name)
        if index_name not in other_cols:
            other_cols.insert(0, index_name)

    df, other = _split_df_by_cols(df, df_cols, other_cols)
    return df, other


def _split_col_arg(df, cols):
    df_cols = _utils.get_col_names(df, index=False)
    if isinstance(cols, dict):
        cols = __filter_items_like_pattern(cols, df_cols)
    not_cols = [c for c in df_cols if c not in cols]
    return not_cols, cols,


def __filter_items_like_pattern(cols, df_cols):
    pattern = cols['like']
    if not isinstance(pattern, str):
        pattern = pattern[0]
    cols = filter_items_like_pattern(df_cols, like=pattern)
    return cols


def _split_df_by_cols(df, df_cols, other_cols):
    if isinstance(df, pa.Table):
        other = df.select(other_cols)
        df = df.select(df_cols)
    elif isinstance(df, pd.Series):
        other = pd.DataFrame(index=df.index)
        try:
            if df.name == other_cols[0]:
                other, df = df, other
        except IndexError:
            pass
    else:
        other = df[other_cols]
        df = df[df_cols]
    return df, other


def split_rows(df, rows, index_name, iloc):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        df, other = _split_pd_rows(df, rows, iloc)
    elif isinstance(df, pa.Table):
        df, other = _split_pa_rows(df, rows, index_name, iloc)
    elif isinstance(df, (pl.DataFrame, pl.Series)):
        df, other = _split_pl_rows(df, rows, index_name, iloc)
    return df, other


def _split_pd_rows(df, rows, iloc):
    as_series = False
    if isinstance(df, pd.Series):
        df = df.to_frame()
        as_series = True

    arrow_df = pa.Table.from_pandas(df, preserve_index=True)
    df, other = _split_pa_rows(arrow_df, rows=rows, index_name=df.index.name, iloc=iloc)
    df = convert_table(df, to='pandas', as_series=as_series)
    other = convert_table(other, to='pandas', as_series=as_series)

    if isinstance(df.index, pd.RangeIndex) and isinstance(other.index, pd.RangeIndex):
        start_value = df.index[-1] + 1
        other.index = other.index + start_value
    return df, other


def _split_pl_rows(df, rows, index_name, iloc):
    df = df.to_arrow()
    df, other = _split_pa_rows(df, rows=rows, index_name=index_name, iloc=iloc)

    df = pl.DataFrame(df)
    other = pl.DataFrame(other)
    return df, other


def _split_pa_rows(df, rows, index_name, iloc):
    if index_name is None:
        index_name = _utils.get_index_name(df)

    if index_name is None or iloc:
        index_name = '__rangeindex__'
        df = df.append_column('__rangeindex__', [pd.RangeIndex(len(df))])

    rows = format_rows_arg(rows, to_dtype=str(df[index_name].type))
    other = filter_arrow_table(df, rows, index_name)

    mask = pa.compute.is_in(df[index_name], value_set=other[index_name])
    mask = pa.compute.invert(mask)
    df = df.filter(mask)

    if index_name in (DEFAULT_ARROW_INDEX_NAME, '__rangeindex__'):
        index = df[index_name]
        if _utils.is_rangeindex(index):
            df = df.drop([index_name])
            other = other.drop([index_name])
    return df, other
