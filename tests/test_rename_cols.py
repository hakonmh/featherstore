import pytest
from .fixtures import *

from pandas.testing import assert_frame_equal


@pytest.mark.parametrize(
    ("columns", "to", "result"),
    [
        (['c0', 'c2'], ['d0', 'd2'], ['d0', 'c1', 'd2', 'c3']),
        ({'c0': 'd0', 'c2': 'd2'}, None, ['d0', 'c1', 'd2', 'c3']),
        (['c2', 'c3'], ['c3', 'c2'], ['c0', 'c1', 'c3', 'c2'])
    ],
)
def test_rename_cols(store, columns, to, result):
    # Arrange
    original_df = make_table(rows=30, cols=4, astype="pandas")
    expected = original_df.copy()
    expected.columns = result

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.rename_columns(columns, to=to)
    # Assert
    df = store.read_pandas(TABLE_NAME)
    assert_frame_equal(df, expected, check_dtype=False)


NEW_COL_NAMES_PROVIDED_TWICE = [{'c0': 'd0'}, ['d0']]
NEW_NAMES_NOT_PROVIDED = [['c0', 'c1'], None]
NUMBER_OF_COLS_DOESNT_MATCH = [['c0', 'c1'], ['d0']]
WRONG_OLD_COL_NAME_DTYPE = [[1], ['d0']]
WRONG_NEW_COL_NAME_DTYPE_AS_LIST = [['c0'], [1]]
WRONG_NEW_COL_NAME_DTYPE_AS_DICT = [{'c0': 1}, None]
FORBIDDEN_COL_NAME = [{'c0': 'like'}, None]
DUPLICATE_COL_NAMES = [['c0', 'c1'], ['d0', 'd0']]
COL_NAME_ALREADY_IN_TABLE = [{'c0': 'c1'}, None]


@pytest.mark.parametrize(
    ("args", "exception"),
    [
        (NEW_COL_NAMES_PROVIDED_TWICE, AttributeError),
        (NEW_NAMES_NOT_PROVIDED, AttributeError),
        (NUMBER_OF_COLS_DOESNT_MATCH, ValueError),
        (WRONG_OLD_COL_NAME_DTYPE, TypeError),
        (WRONG_NEW_COL_NAME_DTYPE_AS_LIST, TypeError),
        (WRONG_NEW_COL_NAME_DTYPE_AS_DICT, TypeError),
        (FORBIDDEN_COL_NAME, ValueError),
        (DUPLICATE_COL_NAMES, IndexError),
        (COL_NAME_ALREADY_IN_TABLE, IndexError),
    ],
    ids=[
        "NEW_COL_NAMES_PROVIDED_TWICE",
        "NEW_NAMES_NOT_PROVIDED",
        "NUMBER_OF_COLS_DOESNT_MATCH",
        "WRONG_OLD_COL_NAME_DTYPE",
        "WRONG_NEW_COL_NAME_DTYPE_AS_LIST",
        "WRONG_NEW_COL_NAME_DTYPE_AS_DICT",
        "FORBIDDEN_COL_NAME",
        "DUPLICATE_COL_NAMES",
        "COL_NAME_ALREADY_IN_TABLE",
    ]
)
def test_can_rename_cols(store, args, exception):
    # Arrange
    col_names, new_col_names = args
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.rename_columns(col_names, to=new_col_names)
