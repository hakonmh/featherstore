import pytest
import random
from .fixtures import *


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(astype="polars"),
        make_table(sorted_datetime_index, astype="polars"),
        make_table(sorted_string_index, astype="polars"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_sorted_polars_io(
    original_df, basic_data, database, connection, store
):
    # Arrange
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], original_df, partition_size=partition_size
    )
    # Act
    df = store.read_polars(basic_data["table_name"])
    # Assert
    assert df.frame_equal(original_df)


def test_unsorted_polars_io(
    basic_data, database, connection, store
):
    # Arrange
    original_df = make_table(unsorted_int_index, astype="polars")
    sorted_original_df = original_df.sort(by="__index_level_0__")
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"],
        original_df,
        partition_size=partition_size,
        warnings="ignore",
    )
    # Act
    df = store.read_polars(basic_data["table_name"])
    # Assert
    assert df.frame_equal(sorted_original_df)


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(astype="polars"),
        make_table(sorted_datetime_index, astype="polars"),
        make_table(sorted_string_index, astype="polars"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_polars_append_table(
    original_df, basic_data, database, connection, store
):
    # Arrange
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df[:slice_]
    appended_df = original_df[slice_:]
    cols = appended_df.columns
    shuffled_cols = random.sample(cols, len(cols))
    appended_df = appended_df[shuffled_cols]

    partition_size = get_partition_size(prewritten_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], prewritten_df, partition_size=partition_size
    )
    store.append_table(basic_data["table_name"], appended_df)
    # Act
    df = store.read_polars(basic_data["table_name"])
    # Assert
    assert df.frame_equal(original_df)
