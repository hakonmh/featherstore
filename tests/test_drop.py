import re
import pytest
from .fixtures import *

ARGS = [
    (default_index, [10, 24, 0, 13], None),
    (default_index, pd.RangeIndex(10, 13), None),
    (default_index, ['before', 10], None),
    (default_index, ['after', 10], None),
    (default_index, ['between', 10, 13], None),
    (continuous_string_index, pd.Index(['ab', 'bd', 'al']), None),
    (continuous_string_index, ['before', 'al'], None),
    (continuous_string_index, ['after', 'al'], None),
    (continuous_string_index, ['between', 'aj', 'ba'], None),
    (continuous_string_index, ['between', 'a', 'b'], None),
    (sorted_string_index, ['between', 'a', 'f'], None),
    (continuous_datetime_index, pd.DatetimeIndex(['2021-01-01', '2021-01-17']), None),
    (continuous_datetime_index, ['before', pd.Timestamp('2021-01-17')], None),
    (continuous_datetime_index, ['after', '2021-01-17'], None),
    (continuous_datetime_index, ['between', '2021-01-10', '2021-01-14'], None),
    (default_index, None, ['c0', 'c3', 'c1']),
    (default_index, None, ['like', 'c?']),
    (default_index, None, ['like', '%1']),
    (default_index, None, ['like', '?1%']),
]


@pytest.mark.parametrize(
    ['index', 'rows', 'cols'], ARGS)
def test_drop(store, index, rows, cols):
    # Arrange
    original_df = Table(index, num_cols=12)
    expected = original_df.drop(rows, cols)

    partition_size = get_partition_size(original_df())
    table = store.select_table(TABLE_NAME)
    table.write(original_df(), partition_size=partition_size, warnings='ignore')
    # Act
    table.drop(rows=rows, cols=cols)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)


class Table:

    def __init__(self, index, num_rows=30, num_cols=5):
        self.table = make_table(index=index, rows=num_rows, cols=num_cols, astype='pandas')

    def __call__(self):
        return self.table

    def drop(self, rows, cols):
        if rows is not None:
            df = self._drop_rows(self.table, rows)
        elif cols is not None:
            df = self._drop_cols(self.table, cols)
        return df

    def _drop_rows(self, df, rows):
        if rows[0] in ('before', 'after', 'between'):
            index = df.index
        if rows[0] == 'before':
            end = rows[1]
            rows = index[end >= index]
        elif rows[0] == 'after':
            start = rows[1]
            rows = index[start <= index]
        elif rows[0] == 'between':
            start = rows[1]
            end = rows[2]
            rows = index[start <= index]
            rows = rows[end >= rows]
        df = df.drop(rows, axis=0)
        return df

    def _drop_cols(self, df, cols):
        if cols[0] == 'like':
            pattern = cols[1].replace('?', '.').replace('%', '.*') + '$'
            pattern = re.compile(pattern)
            cols = list(filter(pattern.search, df.columns))
        df = df.drop(cols, axis=1)
        return df


def _wrong_index_dtype():
    return ['3', '19', '25']


def _wrong_index_values():
    return [2, 5, 7, 10, 459]


def _drop_all_rows():
    return list(pd.RangeIndex(0, 30))


def _wrong_cols_format():
    return ('c1', 'c2', 'c3')


def _wrong_col_elements_dtype():
    return ['c1', 2]


def _drop_index():
    return ['c1', 'index']


def _col_not_in_stored_data():
    return ['c1', 'Non-existant col']


def _drop_all_cols():
    return ['like', 'c%']


def _drop_rows():
    return [4, 5]


def _drop_cols():
    return ['c1', 'c2']


@pytest.mark.parametrize(
    ('rows', 'cols', 'exception'),
    [
        (_wrong_index_dtype(), None, TypeError),
        (_wrong_index_values(), None, ValueError),
        (_drop_all_rows(), None, IndexError),
        (None, _wrong_cols_format(), TypeError),
        (None, _wrong_col_elements_dtype(), TypeError),
        (None, _drop_index(), ValueError),
        (None, _col_not_in_stored_data(), IndexError),
        (None, _drop_all_cols(), IndexError),
        (_drop_rows(), _drop_cols(), AttributeError)
    ],
    ids=[
        '_wrong_index_dtype',
        '_wrong_index_values',
        '_drop_all_values',
        '_wrong_cols_format',
        '_wrong_col_elements_dtype',
        '_drop_index',
        '_col_not_in_stored_data',
        '_drop_all_cols',
        '_drop_rows_and_cols_at_the_same_time'
    ])
def test_can_drop(store, rows, cols, exception):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    original_df.index.name = 'index'
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.drop(rows=rows, cols=cols)
    # Assert
    assert isinstance(e.type(), exception)
