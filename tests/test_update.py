import pytest
from .fixtures import *


@pytest.mark.parametrize(["index", "update_rows_loc", "num_cols"],
                         [(None, [10, 13, 14, 21], 5),
                          (hardcoded_string_index, ["row00010", "row00013",
                                                    "row00014", "row00021"], 1),
                          (hardcoded_datetime_index, ["2021-01-01", "2021-01-16",
                                                      "2021-01-07"], 1)
                          ]
                         )
def test_update_table(store, index, update_rows_loc, num_cols):
    # Arrange
    fixtures = UpdateFixtures(update_rows_loc=update_rows_loc,
                              update_cols_loc=['c0'],
                              index=index)
    original_df = fixtures.make_df()
    update_series = fixtures.generate_update_values(cols=num_cols)
    expected = fixtures.update_table(update_series)

    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    table.update(update_series)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)
    assert not df.equals(original_df)


@pytest.mark.parametrize(["num_partitions", "rows"], [(7, 30), (3, 125), (27, 36)])
def test_partition_structure_after_update_table(num_partitions, rows, store):
    fixtures = UpdateFixtures(update_rows_loc=(10, 13, 14, 21),
                              update_cols_loc=('c2', 'c0'),
                              rows=rows)
    original_df = fixtures.make_df()
    update_df = fixtures.generate_update_values()

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    partition_names = table._partition_data.keys()
    partition_data = table._partition_data.read()
    # Act
    table.update(update_df)
    # Assert
    _assert_that_partitions_are_the_same(table, partition_names, partition_data)


def _assert_that_partitions_are_the_same(table, partition_names, partition_data):
    # Check that partitions keep the same structure after update
    df = table.read_arrow()
    index = df['index']
    for partition, partition_name in zip(index.chunks, partition_names):
        metadata = partition_data[partition_name]

        index_start = partition[0].as_py()
        index_end = partition[-1].as_py()
        num_rows = len(partition)

        assert index_start == metadata['min']
        assert index_end == metadata['max']
        assert num_rows == metadata['num_rows']


class UpdateFixtures:

    def __init__(self, update_rows_loc, update_cols_loc, rows=30, index=None):
        self.rows = rows
        self.index = index
        self.update_rows_loc = update_rows_loc
        self.update_cols_loc = update_cols_loc

    def make_df(self, cols=5):
        self.df = make_table(index=self.index, rows=self.rows, cols=cols, astype="pandas")
        self.df.index.name = 'index'
        return self.df

    def generate_update_values(self, cols=5):
        update_values = make_table(index=self.index, rows=self.rows, cols=cols, astype='pandas')
        update_values = update_values.loc[self.update_rows_loc, self.update_cols_loc]
        if cols == 1:
            update_values = update_values.squeeze()
        return update_values

    def update_table(self, values):
        expected = self.df.copy()
        expected.loc[self.update_rows_loc, self.update_cols_loc] = values
        return expected


def _wrong_index_dtype():
    df = make_table(sorted_datetime_index, astype="pandas")
    return df


def _wrong_index_values():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 459]
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 10]
    return df


def _wrong_column_dtype():
    df = make_table(sorted_string_index, cols=1, astype="pandas")
    df = df.reset_index()
    df.columns = ['c1', 'c2']
    df = df.head(5)
    return df


def _wrong_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c1', 'non-existant_column']
    return df


def _duplicate_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c2', 'c2']
    return df


@pytest.mark.parametrize(
    ("update_df", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_wrong_index_values(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_wrong_column_dtype(), TypeError),
        (_wrong_column_names(), IndexError),
        (_duplicate_column_names(), IndexError),
    ],
    ids=[
        "_wrong_index_dtype",
        "_wrong_index_values",
        "_duplicate_index_values",
        "_wrong_column_dtype",
        "_wrong_column_names",
        "_duplicate_column_names",
    ],
)
def test_can_update_table(update_df, exception, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    store.write_table(TABLE_NAME, original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    with pytest.raises(exception) as e:
        table.update(update_df)
    # Assert
    assert isinstance(e.type(), exception)
