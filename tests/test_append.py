import pytest
import random
from .fixtures import *


@pytest.mark.parametrize("index",
                         [default_index, sorted_datetime_index, sorted_string_index])
@pytest.mark.parametrize("astype",
                         ["arrow", "polars", "pandas"])
@pytest.mark.parametrize("cols",
                         [5, 1])
def test_append_table(store, index, astype, cols):
    # Arrange
    fixture = AppendFixtures(index=index, astype=astype, cols=cols)
    original_df = fixture.original_df
    appended_df = fixture.appended_df

    partition_size = get_partition_size(original_df)
    index_name = get_index_name(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, index=index_name)
    # Act
    table.append(appended_df)
    # Assert
    fixture.assert_equal_expected(table)


class AppendFixtures:

    def __init__(self, index=None, rows=30, cols=5, slice_=0.5, astype="pandas"):
        self._astype = astype
        self._df = make_table(index=index, rows=rows, cols=cols, astype=self._astype)
        original_slice = round(rows * slice_)
        self.original_df = self._df[:original_slice]
        self._make_appended_df(original_slice, index)

    def _make_appended_df(self, original_slice, index):
        df = self._df[original_slice:]
        df = convert_table(df, to='pandas', as_series=False)
        df = self.__shuffle_cols(df)
        if index is None:
            df = self.__make_default_index(df)
        df = convert_table(df, to=self._astype)
        self.appended_df = df

    def __shuffle_cols(self, df):
        cols = df.columns
        shuffled_cols = random.sample(tuple(cols), len(cols))
        df = df[shuffled_cols]
        return df

    def __make_default_index(self, df):
        """For testing append_data with a default index"""
        df = df.reset_index(drop=True)
        return df

    def assert_equal_expected(self, table):
        expected = self._make_expected()

        if self._astype == "arrow":
            df = table.read_arrow()
            if set(df.column_names) == set(expected.column_names):
                expected = expected.select(df.column_names)
            assert df.equals(expected)

        elif self._astype == 'polars':
            df = table.read_polars()
            if set(df.columns) == set(expected.columns):
                expected = expected[df.columns]
            assert df.frame_equal(expected)

        else:
            df = table.read_pandas()
            assert all(df.eq(expected))

    def _make_expected(self):
        index_name = get_index_name(self._df)
        expected = convert_table(self._df, to=self._astype, index_name=index_name)
        return expected


def _wrong_index_dtype():
    df = make_table(sorted_datetime_index, astype="pandas")
    return df


def _wrong_index_values():
    df = make_table(rows=5, astype="pandas")
    df.index = [-5, -4, -3, -2, -1]
    return df


def _duplicate_index_values():
    df = make_table(rows=3, astype="pandas")
    df = df.head(5)
    df.index = [11, 12, 12]
    return df


def _wrong_column_dtype():
    df = make_table(rows=15, cols=5, astype="pandas")
    df.columns = ['c2', 'c4', 'c3', 'c0', 'c1']
    df = df[['c0', 'c1', 'c2', 'c3', 'c4']]
    df = df.tail(5)
    return df


def _wrong_column_names():
    df = make_table(cols=5, astype="pandas")
    df = df.tail(5)
    df.columns = ['c0', 'c1', 'c2', 'c3', 'invalid_col_name']
    return df


def _duplicate_column_names():
    df = make_table(cols=5, astype="pandas")
    df = df.tail(5)
    df.columns = ['c0', 'c1', 'c2', 'c2', 'c4']
    return df


@pytest.mark.parametrize(
    ("append_df", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_wrong_index_values(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_wrong_column_dtype(), TypeError),
        (_wrong_column_names(), ValueError),
        (_duplicate_column_names(), ValueError),
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
def test_can_append_table(append_df, exception, store):
    # Arrange
    original_df = make_table(rows=10, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.append(append_df)
    # Assert
    assert isinstance(e.type(), exception)
