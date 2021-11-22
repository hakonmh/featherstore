import pytest
import random
from .fixtures import *


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(),
        make_table(sorted_datetime_index),
        make_table(sorted_string_index)
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_sorted_arrow_io(original_df, basic_data, database, connection, store):
    # Arrange
    partition_size = get_partition_size(original_df,
                                        basic_data["num_partitions"])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size)
    # Act
    df = store.read_arrow(basic_data["table_name"])
    # Assert
    assert df.equals(original_df)


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(unsorted_int_index),
        make_table(unsorted_datetime_index),
        make_table(unsorted_string_index),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_unsorted_arrow_io(original_df, basic_data, database, connection,
                           store):
    # Arrange
    index_name = original_df.schema.pandas_metadata["index_columns"][0]
    sorted_index = pa.compute.sort_indices(original_df[index_name])
    sorted_original_df = original_df.take(sorted_index)

    partition_size = get_partition_size(original_df,
                                        basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"],
        original_df,
        partition_size=partition_size,
        warnings="ignore",
    )
    # Act
    df = store.read_arrow(basic_data["table_name"])
    # Assert
    assert df.equals(sorted_original_df)


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(),
        make_table(sorted_datetime_index),
        make_table(sorted_string_index),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_append_table(original_df, basic_data, database, connection, store):
    # Arrange
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df.slice(0, slice_)
    appended_df = original_df.slice(slice_)
    cols = appended_df.column_names
    shuffled_cols = random.sample(cols, len(cols))
    appended_df = appended_df.select(shuffled_cols)

    partition_size = get_partition_size(original_df,
                                        basic_data["num_partitions"])
    store.write_table(basic_data["table_name"],
                      prewritten_df,
                      partition_size=partition_size)
    store.append_table(basic_data["table_name"], appended_df)
    # Act
    df = store.read_arrow(basic_data["table_name"])
    # Assert
    assert df.equals(original_df)
