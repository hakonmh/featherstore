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
def test_sorted_polars_io(original_df, basic_data, database, connection, store):
    # Arrange
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], original_df, partition_size=partition_size
    )
    # Act
    df = store.read_polars(basic_data["table_name"])
    # Assert
    assert df.frame_equal(original_df)


def test_unsorted_polars_io(basic_data, database, connection, store):
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
def test_append_table(original_df, basic_data, database, connection, store):
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


@pytest.mark.parametrize(
    ("original_df", "rows"),
    [
        (make_table(astype="pandas"), [2, 6, 9]),
        (
            make_table(sorted_datetime_index, astype="pandas"),
            ["2021-01-07", "2021-01-20"],
        ),
        (make_table(hardcoded_string_index, astype="pandas"), ["row10", "row3"]),
    ],
)
def test_filtering_rows_with_list(
    original_df, rows, basic_data, database, connection, store
):
    # Arrange
    original_df.index.name = "index"
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"],
        original_df,
        warnings="ignore",
        partition_size=partition_size,
    )
    expected = original_df.loc[rows, :]
    expected = expected.reset_index()
    expected = pl.from_pandas(expected)
    # Act
    df = store.read_polars(basic_data["table_name"], rows=rows)
    # Assert
    assert df.frame_equal(expected)


@pytest.mark.parametrize(
    ("low", "high"),
    [
        (0, 5),
        (5, 9),
        (7, 13),
        (6, 10),
        (3, 19),
    ],
)
def test_filtering_columns_and_rows_between(
    low, high, basic_data, database, connection, store
):
    # Arrange
    COLUMNS = ["c0", "c1"]
    ROWS = ["between", low, high]
    original_df = make_table(astype="polars")
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], original_df, partition_size=partition_size
    )
    expected = original_df[low : (high + 1), COLUMNS]
    # Act
    df = store.read_polars(basic_data["table_name"], cols=COLUMNS, rows=ROWS)
    index = df["__index_level_0__"]
    df = df.drop("__index_level_0__")
    # Assert
    assert index[0] == low and index[-1] == high
    assert df.frame_equal(expected)


@pytest.mark.parametrize(
    "high",
    [
        "G",
        "lR",
        "T9est",
    ],
)
def test_filtering_rows_before_low_with_string_index(
    high, basic_data, database, connection, store
):
    # Arrange
    ROWS = ["before", high]
    original_df = make_table(sorted_string_index, astype="pandas")
    expected = original_df.loc[:high, :]
    original_df = pl.from_pandas(original_df.reset_index())
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"],
        original_df,
        index="index",
        partition_size=partition_size,
    )
    # Act
    df = store.read_polars(basic_data["table_name"], rows=ROWS)
    df = df.to_pandas().set_index("index")
    df.index.name = None
    # Assert
    assert df.equals(expected)


@pytest.mark.parametrize(
    "low",
    [
        "2021-01-02",
        pd.Timestamp("2021-01-02"),
        "2021-01-05",
        "2021-01-12",
    ],
)
def test_filtering_rows_after_low_with_datetime_index(
    low, basic_data, database, connection, store
):
    # Arrange
    ROWS = ["after", low]
    original_df = make_table(sorted_datetime_index, astype="pandas")
    expected = original_df.loc[low:, :]
    original_df = pl.from_pandas(original_df.reset_index())
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"],
        original_df,
        index="Date",
        partition_size=partition_size,
    )
    # Act
    df = store.read_polars(basic_data["table_name"], rows=ROWS)
    df = df.to_pandas().set_index("Date")
    # Assert
    assert df.equals(expected)
