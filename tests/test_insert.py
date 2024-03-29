import pytest
from .fixtures import *

import pandas as pd


DROPPED_ROWS_INDICES = [2, 5, 7, 10]


@pytest.mark.parametrize(["index", "row_indices"],
                         [[default_index, [4, 1, 7, 8, 3]],
                          [unsorted_int_index, [0, 1, 2, 3, 4]],
                          [continuous_string_index, ['ab', 'al']],
                          [continuous_datetime_index, ['2021-01-10', '2021-01-14']]]
                         )
@pytest.mark.parametrize(["num_rows", "num_cols", "num_partitions"],
                         [[75, 5, 5],
                          [75, 5, 30],
                          [30, 1, 1],
                          [30, 1, 5]]
                         )
def test_insert_table(store, index, row_indices, num_rows, num_cols, num_partitions):
    # Arrange
    expected = make_table(index, rows=num_rows, cols=num_cols,
                          astype='pandas[series]')
    original_df, insert_df = split_table(expected, rows=row_indices)
    expected = sort_table(expected)

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(insert_df)
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize("row_indices", ([-2, -1], [30, 33], [33, 30, 32, 31]))
def test_default_index_behavior_when_inserting(store, row_indices):
    # Arrange
    original_df = make_table(default_index, rows=30, astype='pandas')
    insert_df = make_table(default_index, rows=len(row_indices), astype='pandas')
    insert_df.index = row_indices

    expected = _insert(original_df, insert_df)

    partition_size = get_partition_size(original_df, 5)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(insert_df)
    # Assert
    assert_table_equals(table, expected)


def _insert(df, other):
    new_df = pd.concat([df, other])
    new_df = sort_table(new_df)
    new_df = convert_table(new_df, to='arrow')
    new_df = format_arrow_table(new_df)
    return new_df


def _insert_table_not_pd_table():
    df = make_table(astype="polars")
    return df


def _non_matching_index_dtype():
    df = make_table(sorted_string_index, astype="pandas")
    return df


def _non_matching_column_dtypes():
    df = make_table(dtype='string', astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    return df


def _index_values_already_in_stored_data():
    df = make_table(astype="pandas")
    return df


def _column_name_not_in_stored_data():
    df = make_table(cols=2, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ['c1', 'non-existant_column']
    return df


def _index_name_not_the_same_as_stored_index():
    df = make_table(astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.index.name = 'new_index_name'
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df = pd.concat([df, df])
    return df


def _duplicate_column_names():
    df = make_table(cols=6, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ['c0', 'c0', 'c1', 'c2', 'c3', 'c4']
    return df


@pytest.mark.parametrize(
    ("insert_df", "exception"),
    [
        (_insert_table_not_pd_table, TypeError),
        (_non_matching_index_dtype, TypeError),
        (_non_matching_column_dtypes, TypeError),
        (_index_values_already_in_stored_data, ValueError),
        (_column_name_not_in_stored_data, ValueError),
        (_index_name_not_the_same_as_stored_index, ValueError),
        (_duplicate_index_values, IndexError),
        (_duplicate_column_names, IndexError),
    ],
    ids=[
        "_insert_table_not_pd_table",
        "_non_matching_index_dtype",
        "_non_matching_column_dtypes",
        "_index_values_already_in_stored_data",
        "_column_name_not_in_stored_data",
        "_index_name_not_the_same_as_stored_index",
        "_duplicate_index_values",
        "_duplicate_column_names",
    ],
)
def test_can_insert_table(store, insert_df, exception):
    # Arrange
    insert_df = insert_df()
    original_df = make_table(cols=5, astype='pandas')
    original_df = original_df.drop(index=DROPPED_ROWS_INDICES)
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.insert(insert_df)
