import re
from collections.abc import Iterable

import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._table._table_utils import DEFAULT_ARROW_INDEX_NAME
from . import _utils
from .convert_table import convert_table


def split_table(df, rows=None, cols=None, index_name=None, keep_index=False, iloc=False):
    if cols is not None and rows is not None:
        df, other = split_cols(df, cols, index_name, keep_index)
        df, _ = split_rows(df, rows, index_name, iloc)
        _, other = split_rows(other, rows, index_name, iloc)
    elif cols is not None:
        df, other = split_cols(df, cols, index_name, keep_index)
    elif rows is not None:
        df, other = split_rows(df, rows, index_name, iloc)
    else:
        other = df
        df = None
    return df, other


def split_cols(df, cols, index_name=None, keep_index=False):
    df_cols, other_cols = split_col_arg(df, cols)

    if keep_index and not isinstance(df, (pd.DataFrame, pd.Series)):
        if index_name not in df_cols:
            df_cols.insert(0, index_name)
        if index_name not in other_cols:
            other_cols.insert(0, index_name)

    df, other = split_df_by_cols(df, df_cols, other_cols)
    return df, other


def split_col_arg(df, cols):
    df_cols = _utils.get_col_names(df, index=False)
    if isinstance(cols, dict):
        cols = _filter_items_like_pattern(cols, df_cols)
    not_cols = [c for c in df_cols if c not in cols]
    return not_cols, cols,


def _filter_items_like_pattern(cols, df_cols):
    pattern = cols['like']
    if not isinstance(pattern, str):
        pattern = pattern[0]
    pattern = __sql_str_pattern_to_regexp(pattern)
    cols = __filter(df_cols, like=pattern)
    return cols


def __sql_str_pattern_to_regexp(pattern):
    if pattern[0] != "%":
        pattern = "^" + pattern
    if pattern[-1] != "%":
        pattern = pattern + "$"
    pattern = pattern.replace("?", ".")
    pattern = pattern.replace("%", ".*")

    pattern = pattern.lower()
    return re.compile(pattern)


def __filter(items, *, like):
    str_lower_list = [item.lower() for item in items]
    filtered_list = set(filter(like.search, str_lower_list))
    results = [item for item in items if item.lower() in filtered_list]
    return results


def split_df_by_cols(df, df_cols, other_cols):
    if isinstance(df, pa.Table):
        other = df.select(other_cols)
        df = df.select(df_cols)
    elif isinstance(df, pd.Series):
        other = pd.DataFrame(index=df.index, columns=[])
        try:
            if df.name == other_cols[0]:
                other, df = df, other
        except IndexError:
            pass
    elif isinstance(df, pl.Series):
        other = pl.Series()
        if df.name == other_cols[0]:
            other, df = df, other
    else:
        other = df[other_cols]
        df = df[df_cols]
    return df, other


def split_rows(df, rows, index_name, iloc):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        df, other = split_pd_rows(df, rows, iloc)
    elif isinstance(df, pa.Table):
        df, other = split_pa_rows(df, rows, index_name, iloc)
    elif isinstance(df, (pl.DataFrame, pl.Series)):
        df, other = split_pl_rows(df, rows, index_name, iloc)
    return df, other


def split_pd_rows(df, rows, iloc):
    as_series = False
    if isinstance(df, pd.Series):
        df = df.to_frame()
        as_series = True

    arrow_df = pa.Table.from_pandas(df, preserve_index=True)
    df, other = split_pa_rows(arrow_df, rows=rows, index_name=df.index.name, iloc=iloc)
    df = convert_table(df, to='pandas', as_series=as_series)
    other = convert_table(other, to='pandas', as_series=as_series)

    if isinstance(df.index, pd.RangeIndex) and isinstance(other.index, pd.RangeIndex):
        start_value = df.index[-1] + 1
        other.index = other.index + start_value
    return df, other


def split_pl_rows(df, rows, index_name, iloc):
    as_series = False
    if isinstance(df, pl.Series):
        df = df.to_frame()
        as_series = True

    df = df.to_arrow()
    df, other = split_pa_rows(df, rows=rows, index_name=index_name, iloc=iloc)

    df = convert_table(df, to='polars', as_series=as_series)
    other = convert_table(other, to='polars', as_series=as_series)
    return df, other


def split_pa_rows(df, rows, index_name, iloc):
    if index_name is None:
        index_name = _utils.get_index_name(df)

    if index_name is None or iloc:
        if DEFAULT_ARROW_INDEX_NAME in df.column_names:
            index_name = '__rangeindex__'
        else:
            index_name = DEFAULT_ARROW_INDEX_NAME
        df = df.add_column(0, index_name, [pd.RangeIndex(len(df))])

    rows = _format_rows_arg(rows, to_dtype=str(df[index_name].type))
    other = _filter_arrow_table(df, rows, index_name)

    mask = pa.compute.is_in(df[index_name], value_set=other[index_name])
    mask = pa.compute.invert(mask)
    df = df.filter(mask)

    if index_name in (DEFAULT_ARROW_INDEX_NAME, '__rangeindex__'):
        if _utils.is_rangeindex(df[index_name]):
            df = df.drop([index_name])
            other = other.drop([index_name])
    return df, other


def _format_rows_arg(rows, *, to_dtype=None):
    if to_dtype:
        is_dict = isinstance(rows, dict)
        if is_dict:
            keyword = tuple(rows.keys())[0]
            rows = rows[keyword]

        if not __is_collection(rows):
            rows = [rows]
        rows = __convert_items(rows, to=to_dtype)

        if is_dict:
            rows = {keyword: rows}
    return rows


def __is_collection(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes))


def __convert_items(items, to):
    if "time" in to or "date" in to:
        items = list(map(pd.to_datetime, items))
    elif "string" in to:
        items = list(map(str, items))
    elif "int" in to:
        items = list(map(int, items))
    return items


def _filter_arrow_table(df, rows, index_col_name):
    index = df[index_col_name]
    if not isinstance(rows, dict):
        if not rows:
            rows = pa.array([], type=index.type)
        row_indices = pa.compute.index_in(rows, value_set=index)
        df = df.take(row_indices)
        return df
    keyword = tuple(rows.keys())[0]
    rows = rows[keyword]

    if keyword == 'before':
        upper_bound = __compute_upper_bound(rows[0], index)
        df = df[:upper_bound]
    elif keyword == 'after':
        lower_bound = __compute_lower_bound(rows[0], index)
        df = df[lower_bound:]
    elif keyword == 'between':
        lower_bound = __compute_lower_bound(rows[0], index)
        upper_bound = __compute_upper_bound(rows[1], index)
        df = df[lower_bound:upper_bound]
    return df


def __compute_lower_bound(row, index):
    lower_bound = __fetch_row_idx(row, index)
    return lower_bound


def __compute_upper_bound(row, index):
    upper_bound = __fetch_row_idx(row, index, is_upper_bound=True)
    return upper_bound


def __fetch_row_idx(row, index, is_upper_bound=False):
    row_idx = __fetch_exact_row_idx(row, index, is_upper_bound)

    no_row_idx_found = row_idx is None
    if no_row_idx_found:
        row_idx = __fetch_closest_row_idx(row, index)

    no_close_row_idx_found = row_idx is None
    if no_close_row_idx_found:
        row_idx = __fetch_last_row_idx(index)
    return row_idx


def __fetch_exact_row_idx(row, index, is_upper_bound):
    row_idx = index.index(row)
    row_idx = row_idx.as_py()
    if row_idx == -1:
        row_idx = None
    elif is_upper_bound:
        row_idx += 1
    return row_idx


def __fetch_closest_row_idx(row, index):
    mask = pa.compute.less_equal(row, index)
    row_idx = mask.index(True)
    row_idx = row_idx.as_py()
    if row_idx == -1:
        row_idx = None
    return row_idx


def __fetch_last_row_idx(index):
    return len(index)
