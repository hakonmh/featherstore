import random
import pytest
from .fixtures import *


@pytest.mark.parametrize("index",
                         [default_index, sorted_datetime_index, sorted_string_index])
@pytest.mark.parametrize("astype",
                         ["arrow", "polars", "pandas"])
def test_append_table(store, index, astype):
    # Arrange
    fixture = AppendFixture(index=index, astype=astype)
    original_df = fixture.original_df
    append_df = fixture.appended_df
    expected = _append(append_df, to=original_df, index=index)

    partition_size = get_partition_size(original_df)
    index_name = get_index_name(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, index=index_name)
    # Act
    table.append(append_df, warnings='ignore')
    # Assert
    _assert_table_equals(table, expected)


class AppendFixture:

    def __init__(self, index, rows=30, astype="pandas"):
        self._SLICE = round(rows * 0.5)
        self._astype = astype
        self._index = index
        self._df = make_table(self._index, rows=rows, astype=self._astype)

    @property
    def original_df(self):
        df = self._df[:self._SLICE]
        return df

    @property
    def appended_df(self):
        df = convert_table(self._df, to='pandas', as_series=False)

        df = df[self._SLICE:]
        df = self._shuffle_cols(df)
        df = self._shuffle_rows(df)
        if self._index is default_index:
            df = self._make_default_index(df)
        if 'Date' in df.columns:
            df = df.set_index('Date')

        df = convert_table(df, to=self._astype)
        return df

    def _shuffle_cols(self, df):
        cols = df.columns
        shuffled_cols = random.sample(tuple(cols), len(cols))
        df = df[shuffled_cols]
        return df

    def _shuffle_rows(self, df):
        if self._astype == "pandas" and self._index != default_index:
            df = df.sample(frac=1)
        return df

    def _make_default_index(self, df):
        """For testing append_data with a default index"""
        df = df.reset_index(drop=True)
        return df


def _append(df, *, to, index):
    astype = __get_astype(to)
    index_name = get_index_name(to)
    original_df = convert_table(to, to="pandas", index_name=index_name)
    append_df = convert_table(df, to="pandas", index_name=index_name)
    df = pd.concat([original_df, append_df])
    if index == default_index:
        df = df.reset_index(drop=True)
    df = df.sort_index()
    df = convert_table(df, to=astype, index_name=index_name)
    return df


def __get_astype(df):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        return "pandas"
    if isinstance(df, pl.DataFrame):
        return "polars"
    if isinstance(df, pa.Table):
        return "arrow"


def _assert_table_equals(table, expected):
    astype = __get_astype(expected)
    if astype == "arrow":
        df = table.read_arrow()
        expected = __reorganize_cols(df, expected)
        assert df.equals(expected)
    elif astype == 'polars':
        df = table.read_polars()
        expected = __reorganize_cols(df, expected)
        assert df.frame_equal(expected)
    else:
        df = table.read_pandas()
        assert all(df.eq(expected))


def __reorganize_cols(df, expected):
    if isinstance(df, pa.Table):
        if set(df.column_names) == set(expected.column_names):
            expected = expected.select(df.column_names)
    if isinstance(df, pl.DataFrame):
        if set(df.columns) == set(expected.columns):
            expected = expected[df.columns]
    return expected


def _non_matching_index_dtype():
    df = make_table(sorted_string_index, astype="pandas")
    return df


def _non_matching_column_dtypes():
    df = make_table(rows=15, cols=5, astype="pandas")
    df.columns = ['c2', 'c4', 'c3', 'c0', 'c1']
    df = df[['c0', 'c1', 'c2', 'c3', 'c4']]
    df = df.tail(5)
    return df


def _index_not_ordered_after_stored_data():
    df = make_table(rows=5, astype="pandas")
    df.index = [-5, -4, -3, -2, -1]
    return df


def _index_value_already_in_stored_data():
    df = make_table(rows=3, astype="pandas")
    df.index = [9, 10, 11]
    return df


def _column_name_not_in_stored_data():
    df = make_table(cols=5, astype="pandas")
    df = df.tail(5)
    df.columns = ['c0', 'c1', 'c2', 'c3', 'invalid_col_name']
    return df


def _index_name_not_the_same_as_stored_index():
    df = make_table(astype="pandas")
    df = df.tail(5)
    df.index.name = 'new_index_name'
    return df


def _duplicate_index_values():
    df = make_table(rows=3, astype="pandas")
    df = df.head(5)
    df.index = [11, 12, 12]
    return df


def _duplicate_column_names():
    df = make_table(cols=5, astype="pandas")
    df = df.tail(5)
    df.columns = ['c0', 'c1', 'c2', 'c2', 'c4']
    return df


def _num_cols_doesnt_match():
    df = make_table(cols=3, astype="pandas")
    df = df.tail(5)
    return df


@pytest.mark.parametrize(
    ("append_df", "exception"),
    [
        (_non_matching_index_dtype(), TypeError),
        (_non_matching_column_dtypes(), TypeError),
        (_index_not_ordered_after_stored_data(), ValueError),
        (_index_value_already_in_stored_data(), ValueError),
        (_column_name_not_in_stored_data(), ValueError),
        (_index_name_not_the_same_as_stored_index(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_duplicate_column_names(), IndexError),
        (_num_cols_doesnt_match(), ValueError),
    ],
    ids=[
        "_non_matching_index_dtype",
        "_non_matching_column_dtypes",
        "_index_not_ordered_after_stored_data",
        "_index_value_already_in_stored_data",
        "_column_name_not_in_stored_data",
        "_index_name_not_the_same_as_stored_index",
        "_duplicate_index_values",
        "_duplicate_column_names",
        "_num_cols_doesnt_match",
    ],
)
def test_can_append_table(store, append_df, exception):
    # Arrange
    original_df = make_table(rows=10, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.append(append_df)
