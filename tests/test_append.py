import pytest
import random
from .fixtures import *


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(),
        make_table(sorted_datetime_index),
        make_table(sorted_string_index),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_append_arrow_table(original_df, basic_data, database, connection, store):
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


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(astype="polars"),
        make_table(sorted_datetime_index, astype="polars"),
        make_table(sorted_string_index, astype="polars"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_append_polars_table(original_df, basic_data, database, connection, store):
    # Arrange
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df[:slice_]
    appended_df = original_df[slice_:]
    cols = appended_df.columns
    shuffled_cols = random.sample(cols, len(cols))
    appended_df = appended_df[shuffled_cols]

    partition_size = get_partition_size(prewritten_df,
                                        basic_data["num_partitions"])
    store.write_table(basic_data["table_name"],
                      prewritten_df,
                      partition_size=partition_size)
    store.append_table(basic_data["table_name"], appended_df)
    # Act
    df = store.read_polars(basic_data["table_name"])
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
def test_append_pd_dataframe(original_df, basic_data, database, connection, store):
    # Arrange
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df.iloc[:slice_]
    appended_df = original_df.iloc[slice_:]
    cols = appended_df.columns
    shuffled_cols = random.sample(tuple(cols), len(cols))
    appended_df = appended_df[shuffled_cols]

    partition_size = get_partition_size(original_df,
                                        basic_data["num_partitions"])
    store.write_table(basic_data["table_name"],
                      prewritten_df,
                      partition_size=partition_size)
    store.append_table(basic_data["table_name"], appended_df)
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
def test_append_pd_series(original_df, basic_data, database, connection, store):
    # Arrange
    original_df = original_df.squeeze()
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df[:slice_]
    appended_df = original_df[slice_:]
    partition_size = get_partition_size(original_df,
                                        basic_data["num_partitions"])
    store.write_table(basic_data["table_name"],
                      prewritten_df,
                      partition_size=partition_size)
    store.append_table(basic_data["table_name"], appended_df)
    # Act
    df = store.read_pandas(basic_data["table_name"])
    # Assert
    assert df.equals(original_df)
