import warnings

import pandas as pd
import pytest

from featherstore.exceptions import (
    ColumnDtypeMismatchError,
    ColumnMismatchError,
    DuplicateColumnNamesError,
    DuplicateIndexValuesError,
    IndexNameMismatchError,
    IndexTypeMismatchError,
    RowAlreadyExistsError,
)

from .fixtures import (
    TABLE_NAME,
    assert_df_equals,
    assert_partition_bounds_are_ordered,
    assert_partition_metadata_matches_files,
    assert_table_equals,
    continuous_datetime_index,
    continuous_string_index,
    convert_table,
    default_index,
    get_index_name,
    get_partition_size,
    make_table,
    partition_layout,
    sort_table,
    sorted_float_index,
    sorted_string_index,
    split_table,
    unsorted_int_index,
)

DROPPED_ROWS_INDICES = [2, 5, 7, 10]


@pytest.mark.parametrize(
    ["index", "row_indices"],
    [
        [default_index, [4, 1, 7, 8, 3]],
        [unsorted_int_index, [0, 1, 2, 3, 4]],
        [continuous_string_index, ["ab", "al"]],
        [continuous_datetime_index, ["2021-01-10", "2021-01-14"]],
    ],
)
@pytest.mark.parametrize(
    ["num_rows", "num_cols", "num_partitions"],
    [[75, 5, 5], [75, 5, 30], [30, 1, 1], [30, 1, 5]],
)
@pytest.mark.parametrize("astype", ["pandas", "polars", "arrow"])
def test_insert_table(
    store, index, row_indices, num_rows, num_cols, num_partitions, astype
):
    # Arrange
    as_series = astype.startswith("pandas")

    expected_pd = make_table(index, rows=num_rows, cols=num_cols, astype="pandas")
    original_pd, insert_pd = split_table(expected_pd, rows=row_indices)
    expected_pd = sort_table(expected_pd)

    original_df = convert_table(original_pd, to=astype, keep_index=True)
    insert_df = convert_table(insert_pd, to=astype, keep_index=True)
    expected = convert_table(
        expected_pd, to=astype, as_series=as_series, keep_index=True
    )

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(
        original_df,
        partition_size=partition_size,
        warnings="ignore",
        index=get_index_name(original_df),
    )
    # Act
    table.insert_rows(insert_df, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize(
    ["index", "row_indices"],
    [
        [default_index, [4, 1, 7, 8, 3]],
        [continuous_string_index, ["ab", "al"]],
        [continuous_datetime_index, ["2021-01-10", "2021-01-14"]],
    ],
)
def test_insert_series(store, index, row_indices):
    # Arrange
    expected = make_table(index, rows=30, cols=1, astype="pandas[series]")
    original_df, insert_df = split_table(expected, rows=row_indices)
    expected = sort_table(expected)

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings="ignore")
    # Act
    table.insert_rows(insert_df, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize(
    "row_indices",
    (
        [-2, -1],
        [30, 33],
        [33, 30, 32, 31],
        [32, 33],
        [30, 31],
    ),
)
def test_default_index_behavior_when_inserting(store, row_indices):
    # Arrange
    original_df = make_table(default_index, rows=30, astype="pandas")
    insert_df = make_table(default_index, rows=len(row_indices), astype="pandas")
    insert_df.index = row_indices

    expected = _insert_rows(original_df, insert_df)

    partition_size = get_partition_size(original_df, 5)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings="ignore")
    # Act
    table.insert_rows(insert_df, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


def _insert_rows(df, other):
    expected = pd.concat([df, other])
    expected = sort_table(expected)
    expected = convert_table(expected, to="arrow")
    return expected


@pytest.mark.parametrize("num_partitions", [17, 30])
def test_insert_rows_with_successive_mid_table_inserts(store, num_partitions):
    # Arrange
    expected = make_table(sorted_float_index, rows=100, cols=3, astype="pandas")
    first_rows = [idx + 0.5 for idx in range(10, 30, 2)]
    second_rows = [idx + 0.5 for idx in range(11, 30, 2)]

    without_second, second_insert = split_table(expected, rows=second_rows)
    original_df, first_insert = split_table(without_second, rows=first_rows)
    expected = sort_table(expected)

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings="ignore")
    # Act
    table.insert_rows(first_insert, warnings="ignore")
    table.insert_rows(second_insert, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


def test_inserting_into_a_partition_seam_extends_the_preceding_partition(store):
    # Arrange
    original_df = make_table(sorted_float_index, astype="pandas")
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    partitions = partition_layout(table)
    seam_value = _midpoint(partitions[0].max, partitions[1].min)
    insert_df = _row_at(original_df, seam_value)
    # Act
    table.insert_rows(insert_df, warnings="ignore")
    # Assert
    assert partition_layout(table)[0].max == seam_value
    assert_partition_bounds_are_ordered(table)
    assert_partition_metadata_matches_files(table)


def test_reading_across_a_seam_that_has_been_inserted_into(store):
    # Arrange
    original_df = make_table(sorted_float_index, astype="pandas")
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    partitions = partition_layout(table)
    seam_start, seam_end = partitions[0].max, partitions[1].min
    insert_df = _row_at(original_df, _midpoint(seam_start, seam_end))
    table.insert_rows(insert_df, warnings="ignore")

    expected = sort_table(pd.concat([original_df, insert_df]))
    expected = expected.loc[seam_start:seam_end]
    # Act
    df = table.read_pandas(rows={"between": [seam_start, seam_end]})
    # Assert
    assert_df_equals(df, expected)


def _midpoint(lower, upper):
    return (lower + upper) / 2


def _row_at(df, index_value):
    row = df.head(1).copy()
    row.index = pd.Index([index_value], name=df.index.name)
    return row


def test_insert_rows_warns_on_unsorted_index(store):
    # Arrange
    expected = make_table(rows=30, cols=3, astype="pandas")
    original_df, insert_df = split_table(expected, rows=[4, 1, 7])
    insert_df = insert_df.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings="ignore")
    # Act and Assert
    with pytest.warns(UserWarning, match="unsorted"):
        table.insert_rows(insert_df, warnings="warn")


def test_insert_rows_can_ignore_unsorted_index_warning(store):
    # Arrange
    expected = make_table(rows=30, cols=3, astype="pandas")
    original_df, insert_df = split_table(expected, rows=[4, 1, 7])
    insert_df = insert_df.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings="ignore")
    # Act and Assert
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        table.insert_rows(insert_df, warnings="ignore")


def _insert_table_not_supported_type():
    return make_table(cols=1, astype="polars[series]")


def _non_matching_index_dtype():
    df = make_table(sorted_string_index, astype="pandas")
    return df


def _non_matching_column_dtypes():
    df = make_table(dtype="string", astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    return df


def _index_values_already_in_stored_data():
    df = make_table(astype="pandas")
    return df


def _column_name_not_in_stored_data():
    df = make_table(cols=2, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ["c1", "non-existant_column"]
    return df


def _index_name_not_the_same_as_stored_index():
    df = make_table(astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.index.name = "new_index_name"
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df = pd.concat([df, df])
    return df


def _duplicate_column_names():
    df = make_table(cols=6, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ["c0", "c0", "c1", "c2", "c3", "c4"]
    return df


@pytest.mark.parametrize(
    ("insert_df", "exception"),
    [
        (_insert_table_not_supported_type, TypeError),
        (_non_matching_index_dtype, IndexTypeMismatchError),
        (_non_matching_column_dtypes, ColumnDtypeMismatchError),
        (_index_values_already_in_stored_data, RowAlreadyExistsError),
        (_column_name_not_in_stored_data, ColumnMismatchError),
        (_index_name_not_the_same_as_stored_index, IndexNameMismatchError),
        (_duplicate_index_values, DuplicateIndexValuesError),
        (_duplicate_column_names, DuplicateColumnNamesError),
    ],
    ids=[
        "_insert_table_not_supported_type",
        "_non_matching_index_dtype",
        "_non_matching_column_dtypes",
        "_index_values_already_in_stored_data",
        "_column_name_not_in_stored_data",
        "_index_name_not_the_same_as_stored_index",
        "_duplicate_index_values",
        "_duplicate_column_names",
    ],
)
def test_can_insert_rows(store, insert_df, exception):
    # Arrange
    insert_df = insert_df()
    original_df = make_table(cols=5, astype="pandas")
    original_df = original_df.drop(index=DROPPED_ROWS_INDICES)
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.insert_rows(insert_df)


def test_insert_rows_rejects_invalid_warnings(store):
    # Arrange
    original_df = make_table(cols=5, astype="pandas")
    insert_df = original_df.iloc[DROPPED_ROWS_INDICES, :].copy()
    original_df = original_df.drop(index=DROPPED_ROWS_INDICES)
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(ValueError):
        table.insert_rows(insert_df, warnings="abcd")
