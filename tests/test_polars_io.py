import pytest
from .fixtures import *
import pandas as pd


@pytest.mark.parametrize("index",
                         [default_index,
                          unsorted_int_index,
                          sorted_datetime_index,
                          unsorted_string_index])
def test_polars_io(store, index):
    # Arrange
    original_df = make_table(index, astype="polars")
    index_name = get_index_name(original_df)
    expected = sort_table(original_df, by=index_name)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size,
                warnings='ignore')
    df = table.read_polars()
    # Assert
    assert df.frame_equal(expected)


def test_store_read_polars(store):
    # Arrange
    original_df = make_table(astype='polars')
    store.write_table(TABLE_NAME, original_df, warnings='ignore')
    # Act
    df = store.read_polars(TABLE_NAME)
    # Assert
    assert df.frame_equal(original_df)


def test_polars_to_pandas(store):
    # Arrange
    original_df = make_table(astype="polars", cols=4)
    expected = convert_table(original_df, to='pandas')

    index_name = get_index_name(original_df)
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size)
    df = table.read_pandas()
    # Assert
    assert df.equals(expected)


@pytest.mark.parametrize(
    ["index", "rows", "cols"],
    [
        (fake_default_index, {'before': 12}, {"like": "c?"}),
        (continuous_datetime_index, ["2021-01-07", "2021-01-20"], None),
    ]
)
def test_polars_filtering(store, index, rows, cols):
    # Arrange
    original_df = make_table(index, astype="polars")
    index_name = get_index_name(original_df)
    _, expected = split_table(original_df, rows=rows, cols=cols, index_name=index_name, keep_index=True)
    if index == fake_default_index:
        original_df = original_df.drop(DEFAULT_ARROW_INDEX_NAME)
        index_name = None

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size,
                warnings='ignore')
    df = table.read_polars(rows=rows, cols=cols)
    # Assert
    assert df.frame_equal(expected)
