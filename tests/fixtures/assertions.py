import pandas as pd
import polars as pl
import pyarrow as pa
from pandas.testing import assert_frame_equal, assert_series_equal

from .convert_table import convert_table


def assert_table_equals(table, expected, *, rows=None, cols=None, astype=None):
    if not astype:
        astype = _get_astype(expected)

    if astype == 'all':
        for astype in ('arrow', 'polars', 'pandas'):
            expected_ = convert_table(expected, to=astype)
            assert_table_equals(table, expected_, rows=rows, cols=cols, astype=astype)
    else:
        df = _read_df(table, astype, rows=rows, cols=cols)
        assert_df_equals(df, expected, astype)


def _get_astype(df):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        return "pandas"
    elif isinstance(df, (pl.DataFrame, pl.Series)):
        return "polars"
    elif isinstance(df, pa.Table):
        return "arrow"


def _read_df(table, astype, rows, cols):
    if astype == "arrow":
        df = table.read_arrow(rows=rows, cols=cols)
    elif astype == 'polars':
        df = table.read_polars(rows=rows, cols=cols)
    elif astype.startswith('pandas'):
        df = table.read_pandas(rows=rows, cols=cols)
    return df


def assert_df_equals(df, expected, astype=None):
    if not astype:
        astype = _get_astype(df)

    if astype == "arrow":
        assert df.equals(expected)
    elif astype.startswith('polars'):
        _assert_polars(df, expected)
    elif astype.startswith('pandas'):
        _assert_pandas(df, expected)


def _assert_polars(df, expected):
    if isinstance(df, pl.DataFrame):
        assert df.frame_equal(expected)
    else:
        assert df.series_equal(expected)


def _assert_pandas(df, expected):
    if isinstance(df, pd.DataFrame):
        assert_frame_equal(df, expected, check_dtype=True, check_freq=True)
    else:
        assert_series_equal(df, expected, check_dtype=True, check_freq=True)
