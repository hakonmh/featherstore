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
    original_df = make_table(index, cols=12, astype='pandas')
    expected = _drop(original_df, rows, cols)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.drop(rows=rows, cols=cols)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)


@pytest.mark.parametrize(
    'rows',
    (['before', 5],
     ['before', -1],
     ['after', 25],
     ['between', -3, -1],
     ['between', 15, 21],
     ['between', 27, 35],
     [4, 29],
     [29, 26, 27, 28],
     )
)
def test_default_index_behavior_when_dropping(store, rows):
    # Arrange
    original_df = make_table(default_index, cols=5, astype='pandas')
    expected = _drop(original_df, rows, None)
    expected = convert_table(expected, to='arrow')
    expected = format_arrow_table(expected)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.drop(rows=rows)
    # Assert
    df = table.read_arrow()
    assert df.equals(expected)


def _drop(df, rows, cols):
    if rows is not None:
        df = __drop_rows(df, rows)
    elif cols is not None:
        df = __drop_cols(df, cols)
    return df


def __drop_rows(df, rows):
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


def __drop_cols(df, cols):
    if cols[0] == 'like':
        pattern = cols[1].replace('?', '.').replace('%', '.*') + '$'
        pattern = re.compile(pattern)
        cols = list(filter(pattern.search, df.columns))
    df = df.drop(cols, axis=1)
    return df


WRONG_ROWS_FORMAT = 'c1, c2, c3'
WRONG_ROW_ELEMENTS_DTYPE = ['3', '19', '25']
ROWS_NOT_IN_TABLE = [2, 5, 7, 10, 459]
DROP_ALL_ROWS = list(pd.RangeIndex(0, 30))
DROP_NO_ROWS = []
WRONG_COLS_FORMAT = 'c1, c2'
WRONG_COL_ELEMENTS_DTYPE = ['c1', 2]
DROP_INDEX = ['c1', 'index']
COL_NOT_IN_TABLE = ['c1', 'Non-existant col']
DROP_ALL_COLS = ['like', 'c%']
DROP_NO_COLS = []
DROP_ROWS_AND_COLS_AT_THE_SAME_TIME = [[4, 5], ['c1', 'c2']]


@pytest.mark.parametrize(
    ('rows', 'cols', 'exception'),
    [
        (WRONG_ROWS_FORMAT, None, TypeError),
        (WRONG_ROW_ELEMENTS_DTYPE, None, TypeError),
        (ROWS_NOT_IN_TABLE, None, IndexError),
        (DROP_ALL_ROWS, None, IndexError),
        (DROP_NO_ROWS, None, IndexError),
        (None, WRONG_COLS_FORMAT, TypeError),
        (None, WRONG_COL_ELEMENTS_DTYPE, TypeError),
        (None, DROP_INDEX, ValueError),
        (None, COL_NOT_IN_TABLE, IndexError),
        (None, DROP_ALL_COLS, IndexError),
        (None, DROP_NO_COLS, IndexError),
        (*DROP_ROWS_AND_COLS_AT_THE_SAME_TIME, AttributeError)
    ],
    ids=[
        'WRONG_ROWS_FORMAT',
        'WRONG_ROW_ELEMENTS_DTYPE',
        'ROWS_NOT_IN_TABLE',
        'DROP_ALL_ROWS',
        'DROP_NO_ROWS',
        'WRONG_COLS_FORMAT',
        'WRONG_COL_ELEMENTS_DTYPE',
        'DROP_INDEX',
        'COL_NOT_IN_TABLE',
        'DROP_ALL_COLS',
        'DROP_NO_COLS',
        'DROP_ROWS_AND_COLS_AT_THE_SAME_TIME'
    ])
def test_can_drop(store, rows, cols, exception):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    original_df.index.name = 'index'
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.drop(rows=rows, cols=cols)
