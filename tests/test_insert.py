import pytest
from .fixtures import *

import pandas as pd
import numpy as np

DROPPED_ROWS_INDICES = [2, 5, 7, 10]


@pytest.mark.parametrize("original_df", [
    make_table(unsorted_int_index, rows=30, astype="pandas"),
    make_table(unsorted_datetime_index, rows=37, astype="pandas"),
    make_table(unsorted_string_index, rows=125, astype="pandas")
])
def test_insert_table(original_df, basic_data, store):
    # Arrange
    row_indices = np.random.choice(original_df.index, size=5, replace=False)
    insert_df = original_df.loc[row_indices, :]
    expected = original_df.copy().sort_index()
    original_df = original_df.drop(index=row_indices)

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.insert(insert_df)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)


def test_insert_table_with_pandas_series(basic_data, store):
    # Arrange
    original_df = make_table(cols=1, astype='pandas').squeeze()
    row_indices = np.random.choice(original_df.index, size=5, replace=False)
    insert_df = original_df.loc[row_indices]
    expected = original_df.copy().sort_index()
    original_df = original_df.drop(index=row_indices)

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.insert(insert_df)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)


def _wrong_index_dtype():
    df = make_table(sorted_datetime_index, astype="pandas")
    return df


def _existing_index_values():
    df = make_table(astype="pandas")
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df = pd.concat([df, df])  # Duplicate df
    return df


def _wrong_column_dtype():
    df = make_table(sorted_string_index, cols=4, astype="pandas")
    df = df.reset_index()
    df.columns = ['c0', 'c1', 'c2', 'c3', 'c4']
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    return df


def _wrong_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ['c1', 'non-existant_column']
    return df


def _duplicate_column_names():
    df = make_table(cols=6, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ['c0', 'c0', 'c1', 'c2', 'c3', 'c4']
    return df


@pytest.mark.parametrize(
    ("insert_df", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_existing_index_values(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_wrong_column_dtype(), TypeError),
        (_wrong_column_names(), ValueError),
        (_duplicate_column_names(), IndexError),
    ],
    ids=[
        "_wrong_index_dtype",
        "_existing_index_values",
        "_duplicate_index_values",
        "_wrong_column_dtype",
        "_wrong_column_names",
        "_duplicate_column_names",
    ],
)
def test_can_insert_table(insert_df, exception, basic_data, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    original_df = original_df.drop(index=DROPPED_ROWS_INDICES)
    store.write_table(basic_data["table_name"], original_df)
    table = store.select_table(basic_data["table_name"])
    # Act
    with pytest.raises(exception) as e:
        table.insert(insert_df)
    # Assert
    assert isinstance(e.type(), exception)
