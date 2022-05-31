import pytest
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
def test_sorted_polars_io(original_df, store):
    # Arrange
    partition_size = get_partition_size(original_df)
    index_name = get_index_name(original_df)
    store.write_table(TABLE_NAME,
                      original_df,
                      partition_size=partition_size,
                      index=index_name)
    # Act
    df = store.read_polars(TABLE_NAME)
    # Assert
    assert df.frame_equal(original_df)


def test_unsorted_polars_io(store):
    # Arrange
    original_df = make_table(unsorted_int_index, astype="polars")
    sorted_original_df = original_df.sort(by="__index_level_0__")
    partition_size = get_partition_size(original_df)
    index_name = get_index_name(original_df)
    store.write_table(
        TABLE_NAME,
        original_df,
        partition_size=partition_size,
        warnings="ignore",
        index=index_name
    )
    # Act
    df = store.read_polars(TABLE_NAME)
    # Assert
    assert df.frame_equal(sorted_original_df)


@pytest.mark.parametrize(
    ("original_df", "rows"),
    [
        (make_table(astype="pandas"), [2, 6, 9]),
        (
            make_table(hardcoded_datetime_index, astype="pandas"),
            ["2021-01-07", "2021-01-20"],
        ),
        (make_table(hardcoded_string_index,
                    astype="pandas"), ["row00010", "row00003"]),
    ],
)
def test_filtering_rows_with_list(original_df, rows, store):
    # Arrange
    original_df.index.name = "index"
    partition_size = get_partition_size(original_df)
    index_name = get_index_name(original_df)
    store.write_table(
        TABLE_NAME,
        original_df,
        warnings="ignore",
        partition_size=partition_size,
        index=index_name
    )
    expected = original_df.loc[rows, :]
    expected = expected.reset_index()
    expected = pl.from_pandas(expected)
    # Act
    df = store.read_polars(TABLE_NAME, rows=rows)
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
def test_filtering_columns_and_rows_between(low, high, store):
    # Arrange
    COLUMNS = ["c0", "c1"]
    ROWS = ["between", low, high]
    original_df = make_table(astype="polars")
    partition_size = get_partition_size(original_df)
    index_name = get_index_name(original_df)
    store.write_table(TABLE_NAME,
                      original_df,
                      partition_size=partition_size,
                      index=index_name)
    expected = original_df[low:(high + 1), COLUMNS]
    # Act
    df = store.read_polars(TABLE_NAME, cols=COLUMNS, rows=ROWS)
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
def test_filtering_rows_before_low_with_string_index(high, store):
    # Arrange
    ROWS = ["before", high]
    pandas_df = make_table(sorted_string_index, astype="pandas")
    original_df = pl.from_pandas(pandas_df.reset_index())

    expected = pandas_df.loc[:high, :]
    expected = pl.from_pandas(expected.reset_index())

    partition_size = get_partition_size(original_df)
    store.write_table(
        TABLE_NAME,
        original_df,
        index="index",
        partition_size=partition_size,
    )
    # Act
    df = store.read_polars(TABLE_NAME, rows=ROWS)
    # Assert
    assert df.frame_equal(expected)


@pytest.mark.parametrize(
    "low",
    [
        "2021-01-02",
        pd.Timestamp("2021-01-02"),
        "2021-01-05",
        "2021-01-12",
    ],
)
def test_filtering_rows_after_low_with_datetime_index(low, store):
    # Arrange
    ROWS = ["after", low]
    pandas_df = make_table(hardcoded_datetime_index, astype="pandas")
    original_df = pl.from_pandas(pandas_df.reset_index())

    expected = pandas_df.loc[low:, :]
    expected = pl.from_pandas(expected.reset_index())

    partition_size = get_partition_size(original_df)
    store.write_table(
        TABLE_NAME,
        original_df,
        index="Date",
        partition_size=partition_size,
    )
    # Act
    df = store.read_polars(TABLE_NAME, rows=ROWS)
    # Assert
    assert df.frame_equal(expected)


def test_polars_to_pandas(store):
    # Arrange
    original_df = make_table(astype="polars", cols=4)
    expected = original_df.to_pandas()
    expected = expected.astype({'c0': 'string'})
    partition_size = get_partition_size(original_df)
    index_name = get_index_name(original_df)
    store.write_table(TABLE_NAME,
                      original_df,
                      partition_size=partition_size,
                      index=index_name)
    # Act
    df = store.read_pandas(TABLE_NAME)
    # Assert
    assert df.equals(expected)
