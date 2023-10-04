import pytest
from .fixtures import *

import pandas as pd
import numpy as np


def test_that_rangeindex_is_converted_back(store):
    # Arrange
    original_df = make_table(fake_default_index, astype="pandas")
    original_df.index.name = 'index'
    store.write_table(TABLE_NAME, original_df)
    # Act
    df = store.read_pandas(TABLE_NAME)
    # Assert
    assert_df_equals(df, original_df)
    assert not isinstance(original_df.index, pd.RangeIndex)
    assert isinstance(df.index, pd.RangeIndex)
    assert df.index.name == original_df.index.name


@pytest.mark.parametrize(
    ["index", "rows", "cols"],
    [
        (default_index, None, ['c0', 'c5', 'c2']),
        (default_index, None, {"like": "c?"}),
        (default_index, None, {"like": ["%1"]}),
        (default_index, None, {"like": "?1%"}),
        (default_index, pd.Index([0, 1, 27]), ['c0', 'c5', 'c2']),
        (default_index, {'after': [12]}, None),
        (default_index, {'between': [12, 27]}, None),
        (continuous_datetime_index, ["2021-01-07", "2021-01-20"], None),
        (continuous_datetime_index, {'before': [pd.Timestamp("2022-02-02")]}, None),
        (continuous_datetime_index, {'after': pd.Timestamp("2021-01-12")}, None),
        (continuous_datetime_index, {'between': ["2021-01-12", "2021-01-20"]}, None),
        (continuous_string_index, ['aa', 'ba'], None),
        (continuous_string_index, {"before": ['aj']}, None),
        (continuous_string_index, {"after": 'aj'}, None),
        (continuous_string_index, {"between": ['a', 'b']}, ['c0']),
        (continuous_string_index, {"between": ['aj', 'ba']}, None),
        (sorted_string_index, {"between": ['b', 'u']}, {"like": "c?"})
    ]
)
def test_pandas_filtering(store, index, rows, cols):
    # Arrange
    original_df = make_table(index, cols=15, astype='pandas[series]')
    _, expected = split_table(original_df, rows=rows, cols=cols)
    expected = expected.squeeze(axis=1)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    df = table.read_pandas(rows=rows, cols=cols)
    # Assert
    assert_df_equals(df, expected)


@pytest.mark.parametrize("cols", [['c0'], {"like": "c?"}, {"like": ["%0"]},
                                  {"like": "?1%"}])
def test_pandas_series_filtering_cols(store, cols):
    # Arrange
    original_df = make_table(cols=1, astype='pandas[series]')
    _, expected = split_table(original_df, cols=cols)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    df = table.read_pandas(cols=cols)
    # Assert
    assert_df_equals(df, expected)


def test_pandas_series_without_name_io(store):
    # Arrange
    original_df = make_table(cols=1, astype='pandas[series]')
    original_df.name = None

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    df = table.read_pandas()
    # Assert
    assert_df_equals(df, original_df)


def test_pandas_categorical_col_io(store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    original_df['c1'] = pd.cut(original_df['c1'], bins=[-np.inf, -0.5, 0.5, np.inf], labels=['low', 'medium', 'high'])
    original_df['c2'] = pd.cut(original_df['c2'], bins=[-np.inf, 0, np.inf], labels=[0, 1])

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    df = table.read_pandas()
    # Assert
    assert_df_equals(df, original_df)
