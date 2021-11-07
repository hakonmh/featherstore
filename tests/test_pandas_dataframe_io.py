import pytest
from random import sample
from .fixtures import *


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(astype="pandas"),
        make_table(sorted_datetime_index, astype="pandas"),
        make_table(sorted_string_index, astype="pandas"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_sorted_pandas_io(original_df, basic_data, database, connection, store):
    # Arrange
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
        make_table(unsorted_int_index, astype="pandas"),
        make_table(unsorted_datetime_index, astype="pandas"),
        make_table(unsorted_string_index, astype="pandas"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_unsorted_pandas_io(original_df, basic_data, database, connection, store):
    # Arrange
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"],
        original_df,
        partition_size=partition_size,
        warnings="ignore",
    )
    # Act
    df = store.read_pandas(basic_data["table_name"])
    # Assert
    assert df.equals(original_df.sort_index())
    assert df.index.name == original_df.index.name


def convert_rangeindex_to_int64_index(df):
    INDEX_NAME = "index"
    int64index = list(df.index)
    df.index = int64index
    df.index.name = INDEX_NAME
    return df


def test_that_pandas_rangeindex_is_converted_back(
    basic_data, database, connection, store
):
    # Arrange
    original_df = make_table(astype="pandas")
    original_df = convert_rangeindex_to_int64_index(original_df)
    store.write_table(basic_data["table_name"], original_df)
    # Act
    df = store.read_pandas(basic_data["table_name"])
    # Assert
    assert isinstance(df.index, pd.RangeIndex)
    assert df.index.name == original_df.index.name


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(astype="pandas"),
        make_table(sorted_datetime_index, astype="pandas"),
        make_table(sorted_string_index, astype="pandas"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_append_table(original_df, basic_data, database, connection, store):
    # Arrange
    slice_ = original_df.shape[0] // 2
    prewritten_df = original_df.iloc[:slice_]
    appended_df = original_df.iloc[slice_:]
    cols = appended_df.columns
    shuffled_cols = sample(tuple(cols), len(cols))
    appended_df = appended_df[shuffled_cols]

    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], prewritten_df, partition_size=partition_size
    )
    store.append_table(basic_data["table_name"], appended_df)
    # Act
    df = store.read_pandas(basic_data["table_name"])
    # Assert
    assert df.equals(original_df)


def test_filter_columns(basic_data, database, connection, store):
    # Arrange
    original_df = make_table(cols=6, astype="pandas")
    cols = ["aapl", "MAST", "test", "4", "TSLA", "Åge"]
    original_df.columns = cols
    store.write_table(basic_data["table_name"], original_df)
    # Act
    df = store.read_pandas(basic_data["table_name"], cols=["LiKE", "%a%"])
    # Assert
    assert df.columns.tolist() == ["aapl", "MAST", "TSLA"]


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
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"],
        original_df,
        warnings="ignore",
        partition_size=partition_size,
    )
    expected = original_df.loc[rows, :]
    # Act
    df = store.read_pandas(basic_data["table_name"], rows=rows)
    # Assert
    assert df.equals(expected)


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
    original_df = make_table(astype="pandas")
    original_df.index.name = "index"
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], original_df, partition_size=partition_size
    )
    expected = original_df.loc[low:high, COLUMNS]
    # Act
    df = store.read_pandas(basic_data["table_name"], cols=COLUMNS, rows=ROWS)
    # Assert
    assert df.equals(expected)


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
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], original_df, partition_size=partition_size
    )
    expected = original_df.loc[:high, :]
    # Act
    df = store.read_pandas(basic_data["table_name"], rows=ROWS)
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
    partition_size = get_partition_size(original_df, basic_data["num_partitions"])
    store.write_table(
        basic_data["table_name"], original_df, partition_size=partition_size
    )
    expected = original_df.loc[low:, :]
    # Act
    df = store.read_pandas(basic_data["table_name"], rows=ROWS)
    # Assert
    assert df.equals(expected)
