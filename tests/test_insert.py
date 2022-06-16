import pytest
from .fixtures import *

import pandas as pd

DROPPED_ROWS_INDICES = [2, 5, 7, 10]


@pytest.mark.parametrize(["index", "row_indices"],
                         [[default_index, [4, 1, 7, 8, 3]],
                          [unsorted_int_index, [0, 1, 2, 3, 4]],
                          [hardcoded_string_index, ['row00010', 'row00000']],
                          [hardcoded_datetime_index, ['2021-01-10', '2021-01-14']]]
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
    expected = fixtures.expected()

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(insert_df)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)


class InsertFixtures:
    def __init__(self, index, num_rows, num_cols):
        df = make_table(index, rows=num_rows, cols=num_cols, astype="pandas")
        self._df = df.squeeze()

    def original_df(self, row_indices):
        return self._df.drop(index=row_indices)

    def insert_df(self, row_indices):
        return self._df.loc[row_indices]

    def expected(self):
        return self._df.sort_index()


def _wrong_index_dtype():
    df = make_table(sorted_datetime_index, astype="pandas")
    return df


def _existing_index_values():
    df = make_table(astype="pandas")
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df = pd.concat([df, df])  # Duplicate df
    return df


def _wrong_column_dtype():
    df = make_table(sorted_string_index, cols=4, astype="pandas")
    df = df.reset_index()
    df.columns = ['c0', 'c1', 'c2', 'c3', 'c4']
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    return df


def _wrong_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ['c1', 'non-existant_column']
    return df


def _duplicate_column_names():
    df = make_table(cols=6, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ['c0', 'c0', 'c1', 'c2', 'c3', 'c4']
    return df


@pytest.mark.parametrize(
    ("insert_df", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_existing_index_values(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_wrong_column_dtype(), TypeError),
        (_wrong_column_names(), ValueError),
        (_duplicate_column_names(), IndexError),
    ],
    ids=[
        "_wrong_index_dtype",
        "_existing_index_values",
        "_duplicate_index_values",
        "_wrong_column_dtype",
        "_wrong_column_names",
        "_duplicate_column_names",
    ],
)
def test_can_insert_table(store, insert_df, exception):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    original_df = original_df.drop(index=DROPPED_ROWS_INDICES)
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.insert(insert_df)
    # Assert
    assert isinstance(e.type(), exception)
