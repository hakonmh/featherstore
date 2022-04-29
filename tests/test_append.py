import pyarrow as pa
import polars as pl

import pytest
import random
from .fixtures import *


def _wrong_index_dtype():
    df = make_table(sorted_datetime_index, astype="pandas")
    return df


def _wrong_index_values():
    df = make_table(rows=5, astype="pandas")
    df.index = [-5, -4, -3, -2, -1]
    return df


def _duplicate_index_values():
    df = make_table(rows=3, astype="pandas")
    df = df.head(5)
    df.index = [11, 12, 12]
    return df


def _wrong_column_dtype():
    df = make_table(rows=15, cols=5, astype="pandas")
    df.columns = ['c2', 'c4', 'c3', 'c0', 'c1']
    df = df[['c0', 'c1', 'c2', 'c3', 'c4']]
    df = df.tail(5)
    return df


def _wrong_column_names():
    df = make_table(cols=5, astype="pandas")
    df = df.tail(5)
    df.columns = ['c0', 'c1', 'c2', 'c3', 'invalid_col_name']
    return df


def _duplicate_column_names():
    df = make_table(cols=5, astype="pandas")
    df = df.tail(5)
    df.columns = ['c0', 'c1', 'c2', 'c2', 'c4']
    return df


@pytest.mark.parametrize(
    ("append_df", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_wrong_index_values(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_wrong_column_dtype(), TypeError),
        (_wrong_column_names(), ValueError),
        (_duplicate_column_names(), ValueError),
    ],
    ids=[
        "_wrong_index_dtype",
        "_wrong_index_values",
        "_duplicate_index_values",
        "_wrong_column_dtype",
        "_wrong_column_names",
        "_duplicate_column_names",
    ],
)
def test_can_append_table(append_df, exception, store):
    # Arrange
    original_df = make_table(rows=10, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.append(append_df)
    # Assert
    assert isinstance(e.type(), exception)


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(),
        make_table(sorted_datetime_index),
        make_table(sorted_string_index),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_append_arrow_table(original_df, store):
    # Arrange
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df.slice(0, slice_)
    appended_df = original_df.slice(slice_)
    cols = appended_df.column_names
    shuffled_cols = random.sample(cols, len(cols))
    appended_df = appended_df.select(shuffled_cols)
    index_name = get_index_name(prewritten_df)

    partition_size = get_partition_size(original_df,
                                        NUMBER_OF_PARTITIONS)
    store.write_table(TABLE_NAME,
                      prewritten_df,
                      partition_size=partition_size,
                      index=index_name)
    store.append_table(TABLE_NAME, appended_df)
    # Act
    df = store.read_arrow(TABLE_NAME)
    # Assert
    assert df.equals(original_df)


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(astype="polars"),
        make_table(sorted_datetime_index, astype="polars"),
        make_table(sorted_string_index, astype="polars"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_append_polars_table(original_df, store):
    # Arrange
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df[:slice_]
    appended_df = original_df[slice_:]
    cols = appended_df.columns
    shuffled_cols = random.sample(cols, len(cols))
    appended_df = appended_df[shuffled_cols]
    index_name = get_index_name(prewritten_df)

    partition_size = get_partition_size(prewritten_df,
                                        NUMBER_OF_PARTITIONS)
    store.write_table(TABLE_NAME,
                      prewritten_df,
                      partition_size=partition_size,
                      index=index_name)
    store.append_table(TABLE_NAME, appended_df)
    # Act
    df = store.read_polars(TABLE_NAME)
    # Assert
    assert df.frame_equal(original_df)


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(astype="pandas"),
        make_table(sorted_datetime_index, astype="pandas"),
        make_table(sorted_string_index, astype="pandas"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_append_pd_dataframe(original_df, store):
    # Arrange
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df.iloc[:slice_]
    appended_df = original_df.iloc[slice_:]
    cols = appended_df.columns
    shuffled_cols = random.sample(tuple(cols), len(cols))
    appended_df = appended_df[shuffled_cols]

    partition_size = get_partition_size(original_df,
                                        NUMBER_OF_PARTITIONS)
    store.write_table(TABLE_NAME,
                      prewritten_df,
                      partition_size=partition_size)
    store.append_table(TABLE_NAME, appended_df)
    # Act
    df = store.read_pandas(TABLE_NAME)
    # Assert
    assert df.equals(original_df)


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(sorted_datetime_index, cols=1, astype="pandas"),
        make_table(cols=1, astype="pandas"),
        make_table(sorted_string_index, cols=1, astype="pandas"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_append_pd_series(original_df, store):
    # Arrange
    original_df = original_df.squeeze()
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df[:slice_]
    appended_df = original_df[slice_:]
    partition_size = get_partition_size(original_df,
                                        NUMBER_OF_PARTITIONS)
    store.write_table(TABLE_NAME,
                      prewritten_df,
                      partition_size=partition_size)
    store.append_table(TABLE_NAME, appended_df)
    # Act
    df = store.read_pandas(TABLE_NAME)
    # Assert
    assert df.equals(original_df)


def _pandas_appended_df(appended_df):
    appended_df = appended_df.reset_index(drop=True)
    return appended_df


def _polars_appended_df(appended_df):
    appended_df = _pandas_appended_df(appended_df)
    appended_df = pl.from_pandas(appended_df)
    return appended_df


def _arrow_appended_df(appended_df):
    appended_df = _pandas_appended_df(appended_df)
    appended_df = pa.Table.from_pandas(appended_df)
    return appended_df


@pytest.mark.parametrize(
    "make_appended_df",
    [
        _pandas_appended_df,
        _polars_appended_df,
        _arrow_appended_df,
    ],
    ids=["pandas", "polars", 'pyarrow'],
)
def test_append_using_default_index(make_appended_df, store):
    original_df = make_table(astype="pandas")
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df.iloc[:slice_]
    partition_size = get_partition_size(original_df,
                                        NUMBER_OF_PARTITIONS)
    store.write_table(TABLE_NAME,
                      prewritten_df,
                      partition_size=partition_size)

    appended_df = original_df.iloc[slice_:]
    appended_df = make_appended_df(appended_df)

    store.append_table(TABLE_NAME, appended_df)
    # Act
    df = store.read_pandas(TABLE_NAME)
    # Assert
    assert df.equals(original_df)
