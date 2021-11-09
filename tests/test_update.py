import pytest
from .fixtures import *

import pandas as pd


@pytest.mark.parametrize(["num_partitions", "rows"],
                         [(7, 30), (3, 125), (27, 36)])
def test_update_table(
    num_partitions, rows, basic_data, database, connection, store
):
    # Arrange
    INDEX_NAME = 'index'
    original_df = make_table(rows=rows, cols=5, astype="pandas")
    original_df.index.name = INDEX_NAME

    ROW_INDICES = [10, 13, 14, 21]
    COL_INDICES = [3, 1]
    update_df = make_table(rows=rows, cols=5, astype='pandas')
    update_df = update_df.iloc[ROW_INDICES, COL_INDICES]
    expected = original_df.copy()
    expected.iloc[ROW_INDICES, COL_INDICES] = update_df

    partition_size = get_partition_size(original_df, num_partitions=num_partitions)
    store.write_table(
        basic_data["table_name"], original_df, partition_size=partition_size
    )
    table = store.table(basic_data["table_name"])
    partition_names = table._table_data["partitions"]
    partition_data = table._partition_data.read()

    # Act
    table.update(update_df)

    # Assert
    df = store.read_pandas(basic_data["table_name"])
    arrow_df = store.read_arrow(basic_data["table_name"])
    arrow_index = arrow_df[INDEX_NAME]
    # Check that partitions keep the same tructure after update
    for partition, partition_name in zip(arrow_index.chunks, partition_names):
        metadata = partition_data[partition_name]
        index_start = partition[0].as_py()
        index_end = partition[-1].as_py()

        assert index_start == int(metadata['min'])
        assert index_end == int(metadata['max'])
        assert len(partition) == metadata['num_rows']

    assert df.equals(expected)
    assert not df.equals(original_df)


def test_update_table_with_pandas_series(basic_data, database, connection, store):
    # Arrange
    ROW_INDICES = [10, 13, 14, 21]
    original_df = make_table(cols=5, astype='pandas')
    store.write_table(basic_data["table_name"], original_df)

    update_df = make_table(cols=1, astype='pandas')
    update_series = update_df.squeeze()
    update_series = update_series[ROW_INDICES]

    expected = original_df.copy()
    expected.iloc[ROW_INDICES, 0] = update_series

    table = store.table(basic_data["table_name"])
    # Act
    table.update(update_series)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)
    assert not df.equals(original_df)


def _wrong_index_dtype():
    df = make_table(sorted_datetime_index, astype="pandas")
    return df


def _wrong_index_values():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 459]
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 10]
    return df


def _wrong_column_dtype():
    df = make_table(sorted_string_index, cols=1, astype="pandas")
    df = df.reset_index()
    df.columns = ['c1', 'c2']
    df = df.head(5)
    return df


def _wrong_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c1', 'non-existant_column']
    return df


def _duplicated_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c2', 'c2']
    return df


@pytest.mark.parametrize(
    ("update_df", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_wrong_index_values(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_wrong_column_dtype(), TypeError),
        (_wrong_column_names(), ValueError),
        (_duplicated_column_names(), IndexError),
    ],
)
def test_can_update_table(
    update_df, exception, basic_data, database, connection, store
):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    store.write_table(basic_data["table_name"], original_df)
    table = store.table(basic_data["table_name"])
    # Act
    with pytest.raises(exception) as e:
        table.update(update_df)
    # Assert
    assert isinstance(e.type(), exception)
