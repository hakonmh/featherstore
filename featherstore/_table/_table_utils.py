import pyarrow as pa
import pandas as pd
import polars as pl

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

PARTITION_NAME_LENGTH = 14
INSERTION_BUFFER_LENGTH = 10**6


def _get_first_row(df):
    return df[:1]


def _get_col_names(df, has_default_index):
    first_row = _get_first_row(df)
    first_row = _convert_to_arrow(first_row)
    cols = first_row.column_names
    if has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
        cols.append(DEFAULT_ARROW_INDEX_NAME)
    return cols


def _coerce_col_dtypes(df, *, to):
    cols = df.columns
    dtypes = to[cols].dtypes
    try:
        df = df.astype(dtypes)
    except ValueError:
        raise TypeError("New and old column dtypes do not match")
    return df


def _convert_to_arrow(df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    return df


def _convert_to_polars(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        df = _convert_to_arrow(df)
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)
    return df


def _convert_to_pandas(df):
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


def _convert_int_to_partition_id(partition_id):
    partition_id = int(partition_id * INSERTION_BUFFER_LENGTH)
    format_string = f'0{PARTITION_NAME_LENGTH}d'
    partition_id = format(partition_id, format_string)
    return partition_id


def _convert_partition_id_to_int(partition_id):
    return int(partition_id) // INSERTION_BUFFER_LENGTH


def _get_index_name(df):
    pd_metadata = df.schema.pandas_metadata
    if pd_metadata is None:
        no_index_name = True
    else:
        index_name, = pd_metadata["index_columns"]
        no_index_name = not isinstance(index_name, str)

    if no_index_name:
        index_name = DEFAULT_ARROW_INDEX_NAME

    return index_name


def _get_index_dtype(df):
    # Uses the fact that index should be first col
    index_dtype = str(df.field(0).type)
    return index_dtype


def _str_is_temporal_dtype(index_dtype):
    return "time" in index_dtype or "date" in index_dtype


def _str_is_string_dtype(index_dtype):
    return "string" in index_dtype


def _str_is_int_dtype(index_dtype):
    return "int" in index_dtype


def _get_pd_index_if_exists(df, index_name):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        index = df.index
    else:
        index = __get_index_if_index_in_table(df, index_name)
    return index


def __get_index_if_index_in_table(df, index_name):
    INDEX_NOT_IN_DF = (KeyError, RuntimeError, TypeError)
    try:
        pd_index = __get_index_as_pd_index(df, index_name)
    except INDEX_NOT_IN_DF:
        pd_index = None
    return pd_index


def __get_index_as_pd_index(df, index_name):
    index = df[index_name]
    if isinstance(index, pl.Series):
        index = index.to_arrow()
    return pd.Index(index)
