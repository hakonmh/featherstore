import pytest
import pandas as pd
from .fixtures import *

ARGS = [
    (default_index, [10, 24, 0, 13], None),
    (default_index, [], None),
    (default_index, pd.RangeIndex(10, 13), None),
    (default_index, {'before': 10}, None),
    (default_index, {'after': [10]}, None),
    (default_index, {'between': [10, 13]}, None),
    (continuous_string_index, pd.Index(['ab', 'bd', 'al']), None),
    (continuous_string_index, {'before': ['al']}, None),
    (continuous_string_index, {'after': 'al'}, None),
    (continuous_string_index, {'between': ['aj', 'ba']}, None),
    (continuous_string_index, {'between': ['a', 'b']}, None),
    (sorted_string_index, {'between': ['a', 'f']}, None),
    (continuous_datetime_index, pd.DatetimeIndex(['2021-01-01', '2021-01-17']), None),
    (continuous_datetime_index, {'before': pd.Timestamp('2021-01-17')}, None),
    (continuous_datetime_index, {'after': '2021-01-17'}, None),
    (continuous_datetime_index, {'between': ['2021-01-10', '2021-01-14']}, None),
    (default_index, None, ['c0', 'c3', 'c1']),
    (default_index, None, {'like': 'c?'}),
    (default_index, None, {'like': '%1'}),
    (default_index, None, {'like': '?1%'}),
    (default_index, {'between': [10, 13]}, {'like': 'c?'}),
    (default_index, None, []),
]


@pytest.mark.parametrize(
    ['index', 'rows', 'cols'], ARGS)
def test_drop(store, index, rows, cols):
    # Arrange
    original_df = make_table(index, cols=12, astype='pandas')
    expected, _ = split_table(original_df, rows=rows, cols=cols)

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
    ({'before': 5},
     {'before': -1},
     {'after': 25},
     {'between': [-3, -1]},
     {'between': [15, 21]},
     {'between': [27, 35]},
     [],
     [4, 4],
     [4, 29],
     [29, 26, 27, 28],
     )
)
def test_default_index_behavior_when_dropping(store, rows):
    # Arrange
    original_df = make_table(fake_default_index, cols=5, astype='arrow')
    expected, _ = split_table(original_df, rows=rows)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.drop(rows=rows)
    # Assert
    df = table.read_arrow()
    assert df.equals(expected)


INVALID_ROWS_DTYPE = 'c1, c2, c3'
INVALID_ROWS_ELEMENTS_DTYPE = ['3', '19', '25']
ROWS_NOT_IN_TABLE = [2, 5, 7, 10, 459]
DROP_ALL_ROWS = list(pd.RangeIndex(0, 30))
INVALID_COLS_DTYPE = 'c1, c2'
INVALID_COLS_ELEMENTS_DTYPE = ['c1', 2]
DROP_INDEX = ['c1', 'index']
COL_NOT_IN_TABLE = ['c1', 'Non-existant col']
DROP_ALL_COLS = {'like': 'c%'}


@pytest.mark.parametrize(
    ('rows', 'cols', 'exception'),
    [
        (INVALID_ROWS_DTYPE, None, TypeError),
        (INVALID_ROWS_ELEMENTS_DTYPE, None, TypeError),
        (ROWS_NOT_IN_TABLE, None, IndexError),
        (DROP_ALL_ROWS, None, IndexError),
        (None, INVALID_COLS_DTYPE, TypeError),
        (None, INVALID_COLS_ELEMENTS_DTYPE, TypeError),
        (None, DROP_INDEX, ValueError),
        (None, COL_NOT_IN_TABLE, IndexError),
        (None, DROP_ALL_COLS, IndexError)
    ],
    ids=[
        'INVALID_ROWS_DTYPE',
        'INVALID_ROWS_ELEMENTS_DTYPE',
        'ROWS_NOT_IN_TABLE',
        'DROP_NO_ROWS',
        'INVALID_COLS_DTYPE',
        'INVALID_COLS_ELEMENTS_DTYPE',
        'DROP_INDEX',
        'COL_NOT_IN_TABLE',
        'DROP_ALL_COLS',
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
