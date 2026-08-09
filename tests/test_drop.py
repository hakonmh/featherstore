import pandas as pd
import pytest

from featherstore.exceptions import (
    CannotDropAllColumnsError,
    CannotDropAllRowsError,
    ColumnNotFoundError,
    IndexNameInColumnsError,
    IndexTypeMismatchError,
    RowNotFoundError,
)

from .fixtures import (
    TABLE_NAME,
    assert_df_equals,
    assert_partition_metadata_matches_files,
    assert_table_equals,
    continuous_datetime_index,
    continuous_string_index,
    default_index,
    fake_default_index,
    get_partition_size,
    make_table,
    partition_layout,
    partition_names,
    sorted_string_index,
    split_table,
)

ARGS = [
    (default_index, [10, 24, 0, 13], None),
    (default_index, [], None),
    (default_index, pd.RangeIndex(10, 13), None),
    (default_index, {"before": 10}, None),
    (default_index, {"after": [10]}, None),
    (default_index, {"between": [10, 13]}, None),
    (continuous_string_index, pd.Index(["ab", "bd", "al"]), None),
    (continuous_string_index, {"before": ["al"]}, None),
    (continuous_string_index, {"after": "al"}, None),
    (continuous_string_index, {"between": ["aj", "ba"]}, None),
    (continuous_string_index, {"between": ["a", "b"]}, None),
    (sorted_string_index, {"between": ["a", "f"]}, None),
    (continuous_datetime_index, pd.DatetimeIndex(["2021-01-01", "2021-01-17"]), None),
    (continuous_datetime_index, {"before": pd.Timestamp("2021-01-17")}, None),
    (continuous_datetime_index, {"after": "2021-01-17"}, None),
    (continuous_datetime_index, {"between": ["2021-01-10", "2021-01-14"]}, None),
    (default_index, None, ["c0", "c3", "c1"]),
    (default_index, None, {"like": "c?"}),
    (default_index, None, {"like": "%1"}),
    (default_index, None, {"like": "?1%"}),
    (default_index, {"between": [10, 13]}, {"like": "c?"}),
    (default_index, None, []),
]


@pytest.mark.parametrize(["index", "rows", "cols"], ARGS)
def test_drop(store, index, rows, cols):
    # Arrange
    original_df = make_table(index, cols=12, astype="pandas")
    expected, _ = split_table(original_df, rows=rows, cols=cols)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings="ignore")
    # Act
    table.drop(rows=rows, cols=cols)
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize(
    "rows",
    (
        {"before": 5},
        {"before": -1},
        {"after": 25},
        {"between": [-3, -1]},
        {"between": [15, 21]},
        {"between": [27, 35]},
        [],
        [4, 4],
        [4, 29],
        [29, 26, 27, 28],
    ),
)
def test_default_index_behavior_when_dropping(store, rows):
    # Arrange
    original_df = make_table(fake_default_index, cols=5, astype="arrow")
    expected, _ = split_table(original_df, rows=rows)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings="ignore")
    # Act
    table.drop(rows=rows)
    # Assert
    assert_table_equals(table, expected)


def _drop_before_a_partitions_last_row(partitions):
    return {"before": partitions[0].max}


def _drop_after_a_partitions_first_row(partitions):
    return {"after": partitions[-1].min}


def _drop_between_two_adjacent_partitions(partitions):
    return {"between": [partitions[1].max, partitions[2].min]}


def _drop_rows_on_both_sides_of_a_seam(partitions):
    return [partitions[1].max, partitions[2].min]


SEAM_DROPS = [
    _drop_before_a_partitions_last_row,
    _drop_after_a_partitions_first_row,
    _drop_between_two_adjacent_partitions,
    _drop_rows_on_both_sides_of_a_seam,
]
SEAM_DROP_IDS = [seam_drop.__name__ for seam_drop in SEAM_DROPS]


@pytest.mark.parametrize("seam_drop", SEAM_DROPS, ids=SEAM_DROP_IDS)
def test_dropping_at_a_partition_seam(store, seam_drop):
    # Arrange
    original_df = make_table(default_index, astype="pandas")
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    rows = seam_drop(partition_layout(table))
    expected, _ = split_table(original_df, rows=rows)
    # Act
    table.drop(rows=rows)
    # Assert
    assert_table_equals(table, expected)
    assert_partition_metadata_matches_files(table)


def test_dropping_a_partitions_last_row_pulls_the_next_row_across_the_seam(store):
    # Arrange
    original_df = make_table(default_index, astype="pandas")
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    partitions = partition_layout(table)
    # Act
    table.drop_rows([partitions[0].max])
    # Assert
    assert partition_layout(table)[0].max == partitions[1].min
    assert_partition_metadata_matches_files(table)


def _drop_the_first_partitions_last_row(store, original_df):
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    seam = partition_layout(table)[0].max
    table.drop_rows([seam])
    return table, seam


def test_reading_before_a_dropped_seam_value_excludes_it(store):
    # Arrange
    original_df = make_table(default_index, astype="pandas")
    table, dropped_seam = _drop_the_first_partitions_last_row(store, original_df)

    expected = original_df.drop(index=dropped_seam).loc[:dropped_seam]
    # Act
    df = table.read_pandas(rows={"before": dropped_seam})
    # Assert
    assert_df_equals(df, expected)


def test_reading_after_the_moved_seam_finds_the_row_that_crossed_it(store):
    # Arrange
    original_df = make_table(default_index, astype="pandas")
    table, dropped_seam = _drop_the_first_partitions_last_row(store, original_df)
    moved_seam = partition_layout(table)[0].max

    expected = original_df.drop(index=dropped_seam).loc[moved_seam:]
    # Act
    df = table.read_pandas(rows={"after": moved_seam})
    # Assert
    assert_df_equals(df, expected)


def test_dropping_every_row_of_a_middle_partition_removes_it(store):
    # Arrange
    original_df = make_table(default_index, astype="pandas")
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    partitions = partition_layout(table)
    emptied = partitions[2]
    # Act
    table.drop_rows({"between": [emptied.min, emptied.max]})
    # Assert
    remaining = partitions[:2] + partitions[3:]
    assert partition_names(partition_layout(table)) == partition_names(remaining)
    assert_partition_metadata_matches_files(table)


def test_reading_across_a_removed_partition_returns_the_surrounding_rows(store):
    # Arrange
    original_df = make_table(default_index, astype="pandas")
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    partitions = partition_layout(table)
    emptied = partitions[2]
    table.drop_rows({"between": [emptied.min, emptied.max]})

    across_the_hole = [partitions[1].max, partitions[3].min]
    expected = original_df.drop(index=original_df.loc[emptied.min : emptied.max].index)
    expected = expected.loc[across_the_hole[0] : across_the_hole[1]]
    # Act
    df = table.read_pandas(rows={"between": across_the_hole})
    # Assert
    assert_df_equals(df, expected)


INVALID_ROWS_DTYPE = "c1, c2, c3"
INVALID_ROWS_ELEMENTS_DTYPE = ["3", "19", "25"]
ROWS_NOT_IN_TABLE = [2, 5, 7, 10, 459]
DROP_ALL_ROWS = list(pd.RangeIndex(0, 30))
INVALID_COLS_DTYPE = "c1, c2"
INVALID_COLS_ELEMENTS_DTYPE = ["c1", 2]
DROP_INDEX = ["c1", "index"]
COL_NOT_IN_TABLE = ["c1", "Non-existant col"]
DROP_ALL_COLS = {"like": "c%"}


@pytest.mark.parametrize(
    ("rows", "cols", "exception"),
    [
        (INVALID_ROWS_DTYPE, None, TypeError),
        (INVALID_ROWS_ELEMENTS_DTYPE, None, IndexTypeMismatchError),
        (ROWS_NOT_IN_TABLE, None, RowNotFoundError),
        (DROP_ALL_ROWS, None, CannotDropAllRowsError),
        (None, INVALID_COLS_DTYPE, TypeError),
        (None, INVALID_COLS_ELEMENTS_DTYPE, TypeError),
        (None, DROP_INDEX, IndexNameInColumnsError),
        (None, COL_NOT_IN_TABLE, ColumnNotFoundError),
        (None, DROP_ALL_COLS, CannotDropAllColumnsError),
    ],
    ids=[
        "INVALID_ROWS_DTYPE",
        "INVALID_ROWS_ELEMENTS_DTYPE",
        "ROWS_NOT_IN_TABLE",
        "DROP_NO_ROWS",
        "INVALID_COLS_DTYPE",
        "INVALID_COLS_ELEMENTS_DTYPE",
        "DROP_INDEX",
        "COL_NOT_IN_TABLE",
        "DROP_ALL_COLS",
    ],
)
def test_can_drop(store, rows, cols, exception):
    # Arrange
    original_df = make_table(cols=5, astype="pandas")
    original_df.index.name = "index"
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.drop(rows=rows, cols=cols)
