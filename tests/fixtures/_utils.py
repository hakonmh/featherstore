import pandas as pd
import polars as pl
import pyarrow as pa

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME


def make_index_first_column(df):
    index_name = df.schema.pandas_metadata["index_columns"][0]
    cols = df.column_names
    cols.remove(index_name)
    cols.insert(0, index_name)
    df = df.select(cols)
    return df


def is_rangeindex(index):
    rangeindex = pa.array(pd.RangeIndex(len(index)))
    TYPES_NOT_MATCHING = pa.lib.ArrowNotImplementedError
    try:
        is_rangeindex = pa.compute.equal(index, rangeindex)
        is_rangeindex = pa.compute.all(is_rangeindex).as_py()
    except TYPES_NOT_MATCHING:
        is_rangeindex = False

    return is_rangeindex


def get_index_name(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        index_name = None
    else:
        cols = get_col_names(df)
        if 'Date' in cols:
            index_name = 'Date'
        elif 'index' in cols:
            index_name = 'index'
        elif 'Index' in cols:
            index_name = 'index'
        elif DEFAULT_ARROW_INDEX_NAME in cols:
            index_name = DEFAULT_ARROW_INDEX_NAME
        else:
            index_name = None
    return index_name


def get_col_names(df, index=True):
    if isinstance(df, (pd.DataFrame, pl.DataFrame)):
        cols = list(df.columns)
    elif isinstance(df, pa.Table):
        cols = df.column_names
    else:
        cols = [df.name]

    if isinstance(df, (pd.DataFrame, pd.Series)) and index:
        index_name = df.index.name
        index_name = index_name if index_name else DEFAULT_ARROW_INDEX_NAME
        cols.append(index_name)
    return cols


def convert_object_cols_to_string(df):
    if isinstance(df, dict):
        dtypes = {col: df[col].dtype.kind for col in df}
    else:
        dtypes = df.dtypes
    for col, dtype in dtypes.items():
        if dtype in ('O', 'U'):
            df[col] = pd.array(df[col], dtype="string")
    return df
