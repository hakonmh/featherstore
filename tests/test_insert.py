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
    fixtures = InsertFixtures(index, num_rows, num_cols)
    original_df = fixtures.original_df(row_indices)
    insert_df = fixtures.insert_df(row_indices)
    expected = fixtures.expected(original_df, insert_df)

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(insert_df)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)


@pytest.mark.parametrize("row_indices", ([-2, -1], [30, 33], [33, 30, 32, 31]))
def test_default_index_behavior_when_inserting(store, row_indices):
    # Arrange
    fixtures = InsertFixtures(default_index, 30, 5)
    original_df = fixtures.original_df(row_indices)
    insert_df = fixtures.insert_df(row_indices)
    expected = fixtures.expected(original_df, insert_df)
    expected = convert_table(expected, to='arrow')
    expected = format_arrow_table(expected)

    partition_size = get_partition_size(original_df, 5)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(insert_df)
    # Assert
    df = table.read_arrow()
    assert df.equals(expected)


class InsertFixtures:
    def __init__(self, index, num_rows, num_cols):
        self._index = index
        self._num_rows = num_rows
        self._num_cols = num_cols

    def original_df(self, row_indices):
        df = make_table(self._index, rows=self._num_rows, cols=self._num_cols,
                        astype="pandas")

        row_indices = pd.Index(row_indices)
        if isinstance(df.index, pd.DatetimeIndex):
            row_indices = pd.DatetimeIndex(row_indices)

        if row_indices.isin(df.index).all():
            df = df.drop(index=row_indices)

        df = df.squeeze()
        return df

    def insert_df(self, row_indices):
        df = make_table(self._index, rows=len(row_indices), cols=self._num_cols,
                        astype="pandas")

        row_indices = pd.Index(row_indices)
        if isinstance(df.index, pd.DatetimeIndex):
            row_indices = pd.DatetimeIndex(row_indices)

        index_name = df.index.name
        df.index = row_indices
        df.index.name = index_name

        df = df.squeeze()
        return df

    def expected(self, original_df, insert_df):
        df = pd.concat([original_df, insert_df])
        return df.sort_index()


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
        (_insert_table_not_pd_table(), TypeError),
        (_non_matching_index_dtype(), TypeError),
        (_non_matching_column_dtypes(), TypeError),
        (_index_values_already_in_stored_data(), ValueError),
        (_column_name_not_in_stored_data(), ValueError),
        (_index_name_not_the_same_as_stored_index(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_duplicate_column_names(), IndexError),
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
    original_df = make_table(cols=5, astype='pandas')
    original_df = original_df.drop(index=DROPPED_ROWS_INDICES)
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.insert(insert_df)
