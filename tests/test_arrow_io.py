import pytest
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
def test_sorted_arrow_io(original_df, store):
    # Arrange
    partition_size = get_partition_size(original_df,
                                        NUMBER_OF_PARTITIONS)
    index_name = get_index_name(original_df)
    store.write_table(TABLE_NAME,
                      original_df,
                      partition_size=partition_size,
                      index=index_name)
    # Act
    df = store.read_arrow(TABLE_NAME)
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
def test_unsorted_arrow_io(original_df, store):
    # Arrange
    index_name = original_df.schema.pandas_metadata["index_columns"][0]
    sorted_index = pa.compute.sort_indices(original_df[index_name])
    sorted_original_df = original_df.take(sorted_index)

    partition_size = get_partition_size(original_df,
                                        NUMBER_OF_PARTITIONS)
    index_name = get_index_name(original_df)
    store.write_table(
        TABLE_NAME,
        original_df,
        partition_size=partition_size,
        warnings="ignore",
        index=index_name
    )
    # Act
    df = store.read_arrow(TABLE_NAME)
    # Assert
    assert df.equals(sorted_original_df)


def test_unpartitioned_arrow_io(store):
    # Arrange
    original_df = make_table()
    index_name = get_index_name(original_df)
    store.write_table(TABLE_NAME,
                      original_df,
                      partition_size=-1,
                      index=index_name)
    # Act
    df = store.read_arrow(TABLE_NAME)
    # Assert
    assert df.equals(original_df)
