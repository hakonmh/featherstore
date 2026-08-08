import json
from collections.abc import Iterable
from collections.abc import Set as AbstractSet

import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
from featherstore.exceptions import (
    ColumnDtypeMismatchError,
    RowNotFoundError,
)

SUPPORTED_TABLE_TYPES = (pd.DataFrame, pd.Series, pl.DataFrame, pl.Series, pa.Table)
EDIT_TABLE_TYPES = (pd.DataFrame, pd.Series, pl.DataFrame, pa.Table)


def concat_arrow_tables(*dfs):
    main_df = dfs[0]
    dfs = _sort_cols(dfs, cols=main_df.column_names)
    try:
        dfs = _coerce_arrow_col_types(dfs, schema=main_df.schema)
        full_table = pa.concat_tables(dfs)
    except pa.ArrowInvalid:
        raise ColumnDtypeMismatchError("New and old column types doesn't match")
    return full_table


def _sort_cols(dfs, cols):
    sorted_dfs = []
    for df in dfs:
        cols_not_sorted = df.column_names != cols
        if cols_not_sorted:
            df = df.select(cols)
        sorted_dfs.append(df)
    return sorted_dfs


def _coerce_arrow_col_types(dfs, schema):
    coerced_dfs = []
    for df in dfs:
        df = df.cast(schema)
        coerced_dfs.append(df)
    return coerced_dfs


def sort_arrow_table(df, *, by):
    indices = pa.compute.sort_indices(df, sort_keys=[(by, "ascending")])
    return df.take(indices)


def get_data_col_names(df, *, index_name):
    cols = _raw_col_names(df)
    if isinstance(df, (pd.Series, pd.DataFrame)):
        return cols
    return [c for c in cols if c != index_name]


def get_col_names(df, has_default_index):
    cols = list(_raw_col_names(df))
    if isinstance(df, (pd.DataFrame, pd.Series)):
        index_name = df.index.name or DEFAULT_ARROW_INDEX_NAME
        if index_name not in cols:
            cols.append(index_name)
    elif has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
        cols.append(DEFAULT_ARROW_INDEX_NAME)
    return cols


def _raw_col_names(df):
    if isinstance(df, pd.Series):
        return [df.name]
    if isinstance(df, pd.DataFrame):
        return df.columns.tolist()
    if isinstance(df, pl.DataFrame):
        return list(df.columns)
    if isinstance(df, pa.Table):
        return df.column_names
    return [df.name]


def convert_to_arrow(df, as_array=False):
    if isinstance(df, (pl.Series, pd.Series, pd.Index)):
        if as_array:
            df = pa.array(df)
        else:
            df = df.to_frame()
    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    return df


def convert_to_polars(df, as_array=False):
    if isinstance(df, (pd.Series, pd.DataFrame, pd.Index)):
        df = convert_to_arrow(df, as_array=as_array)
    if isinstance(df, (pa.Table, pa.Array, pa.ChunkedArray)):
        df = pl.from_arrow(df, rechunk=False)
    return df


def convert_to_pandas(df):
    if isinstance(df, pd.DataFrame):
        pd_df = df
    elif isinstance(df, pd.Series):
        pd_df = df.to_frame()
    elif isinstance(df, (pa.Table, pl.DataFrame)):
        pd_df = df.to_pandas()

    if DEFAULT_ARROW_INDEX_NAME in pd_df.columns:
        pd_df = pd_df.set_index(DEFAULT_ARROW_INDEX_NAME)
        pd_df.index.name = None

    return pd_df


def get_previous_item(item, sequence):
    idx = sequence.index(item)
    is_not_first_item = idx > 0
    if is_not_first_item:
        return sequence[idx - 1]


def get_next_item(item, sequence):
    idx = sequence.index(item)
    is_not_last_item = idx < (len(sequence) - 1)
    if is_not_last_item:
        return sequence[idx + 1]


def get_index_name(df):
    pd_metadata = df.schema.pandas_metadata
    if pd_metadata is None:
        no_index_name = True
    else:
        (index_name,) = pd_metadata["index_columns"]
        no_index_name = not isinstance(index_name, str)

    if no_index_name:
        index_name = DEFAULT_ARROW_INDEX_NAME

    return index_name


def get_index_dtype(df):
    # Uses the fact that index should be first col
    index_dtype = str(df.field(0).type)
    return index_dtype


def typestring_is_temporal(index_dtype):
    return "time" in index_dtype or "date" in index_dtype


def typestring_is_duration(index_dtype):
    return "duration" in index_dtype


def typestring_is_string(index_dtype):
    return "string" in index_dtype


def typestring_is_int(index_dtype):
    return index_dtype.startswith(("int", "uint"))


def typestring_is_float(index_dtype):
    return (
        "float" in index_dtype or "double" in index_dtype or "halffloat" in index_dtype
    )


def typestring_is_decimal(index_dtype):
    return "decimal" in index_dtype


def typestring_is_binary(index_dtype):
    return "binary" in index_dtype


def index_type_is_supported(index_type):
    if isinstance(index_type, pa.DataType):
        return _pa_index_type_is_supported(index_type)
    return _index_typestring_is_supported(str(index_type))


def _pa_index_type_is_supported(dtype):
    return any(
        check(dtype)
        for check in (
            pa.types.is_integer,
            pa.types.is_floating,
            pa.types.is_decimal,
            pa.types.is_binary,
            pa.types.is_string,
            pa.types.is_large_string,
            pa.types.is_temporal,
        )
    )


def _index_typestring_is_supported(index_dtype):
    return any(
        check(index_dtype)
        for check in (
            typestring_is_string,
            typestring_is_temporal,
            typestring_is_duration,
            typestring_is_int,
            typestring_is_float,
            typestring_is_decimal,
            typestring_is_binary,
        )
    )


def get_index_if_exists(df, index_name):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        index = convert_to_arrow(df.index, as_array=True)
    else:
        try:
            index = df[index_name]
        except (KeyError, TypeError, IndexError, pl.exceptions.ColumnNotFoundError):
            index = None
    return convert_to_arrow(index, as_array=True)


def filter_arrow_table(df, rows, index_col_name):
    index = df[index_col_name]
    if not rows.keyword:
        df = _fetch_rows_in_list(df, index, rows.values())
    elif rows.keyword == "before":
        df = _fetch_rows_before(df, index, rows[0])
    elif rows.keyword == "after":
        df = _fetch_rows_after(df, index, rows[0])
    elif rows.keyword == "between":
        df = _fetch_rows_between(df, index, low=rows[0], high=rows[1])
    return df


def _fetch_rows_in_list(df, index, rows):
    if not rows:
        return pa.table([[]] * len(df.column_names), schema=df.schema)
    row_indices = pa.compute.index_in(rows, value_set=index)
    _raise_if_rows_not_in_table(row_indices, rows, index)
    df = pa.compute.take(df, row_indices, boundscheck=False)
    return df


def _raise_if_rows_not_in_table(row_indices, rows, index):
    contains_null = row_indices.null_count > 0
    if contains_null:
        is_in = pa.compute.is_in(rows, value_set=index)
        missing_mask = pa.compute.invert(is_in)
        missing = pa.compute.filter(rows, missing_mask).to_pylist()
        raise RowNotFoundError(f"Trying to access rows not found in table ({missing})")


def _fetch_rows_before(df, index, row):
    upper_bound = _compute_upper_bound(row, index)
    df = df[:upper_bound]
    return df


def _fetch_rows_after(df, index, row):
    lower_bound = _compute_lower_bound(row, index)
    df = df[lower_bound:]
    return df


def _fetch_rows_between(df, index, low, high):
    lower_bound = _compute_lower_bound(low, index)
    upper_bound = _compute_upper_bound(high, index)
    df = df[lower_bound:upper_bound]
    return df


def _compute_lower_bound(row, index):
    lower_bound = __fetch_row_idx(row, index)
    return lower_bound


def _compute_upper_bound(row, index):
    upper_bound = __fetch_row_idx(row, index, is_upper_bound=True)
    return upper_bound


def __fetch_row_idx(row, index, is_upper_bound=False):
    row_idx = _fetch_exact_row_idx(row, index, is_upper_bound)

    no_row_idx_found = row_idx is None
    if no_row_idx_found:
        row_idx = _fetch_closest_row_idx(row, index)

    no_close_row_idx_found = row_idx is None
    if no_close_row_idx_found:
        row_idx = _fetch_last_row_idx(index)
    return row_idx


def _fetch_exact_row_idx(row, index, is_upper_bound):
    row_idx = index.index(row)
    row_idx = row_idx.as_py()
    if row_idx == -1:
        row_idx = None
    elif is_upper_bound:
        row_idx += 1
    return row_idx


def _fetch_closest_row_idx(row, index):
    mask = pa.compute.less_equal(row, index)
    row_idx = mask.index(True)
    row_idx = row_idx.as_py()
    if row_idx == -1:
        row_idx = None
    return row_idx


def _fetch_last_row_idx(index):
    return len(index)


def is_collection(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes))


def is_list_like(obj):
    is_iterable = isinstance(obj, Iterable)
    is_not_string = not isinstance(obj, (str, bytes))
    is_not_set = not isinstance(obj, AbstractSet)
    return is_iterable and is_not_string and is_not_set


def is_transposed(df):
    fs_metadata = df.schema.metadata[b"featherstore"]
    fs_metadata = json.loads(fs_metadata)
    return fs_metadata["transposed"]
