import pytest
import random
from .fixtures import *


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(cols=1, astype="pandas"),
        make_table(sorted_datetime_index, cols=1, astype="pandas"),
        make_table(sorted_string_index, cols=1, astype="pandas"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_pandas_series_io(original_df, basic_data, database, connection, store):
    # Arrange
    original_df = make_table(sorted_string_index, cols=1, astype="pandas")
    original_df = original_df.squeeze()
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], original_df, partition_size=partition_size
    )
    # Act
    df = store.read_pandas(basic_data["table_name"])
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
def test_append_table(original_df, basic_data, database, connection, store):
    # Arrange
    original_df = original_df.squeeze()
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df[:slice_]
    appended_df = original_df[slice_:]
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], prewritten_df, partition_size=partition_size
    )
    store.append_table(basic_data["table_name"], appended_df)
    # Act
    df = store.read_pandas(basic_data["table_name"])
    # Assert
    assert df.equals(original_df)
