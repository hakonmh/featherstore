import pytest
from .fixtures import *


@pytest.mark.parametrize(
    ("columns", "to", "result"),
    [
        (['c0', 'c2'], ['d0', 'like'], ['d0', 'c1', 'like', 'c3']),
        ({'c0': 'd0', 'c2': 'like'}, None, ['d0', 'c1', 'like', 'c3']),
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
    assert_table_equals(table, expected)


NEW_COL_NAMES_PROVIDED_TWICE = [{'c0': 'd0'}, ['d0']]
NEW_NAMES_NOT_PROVIDED = [['c0', 'c1'], None]
NUMBER_OF_COLS_DOESNT_MATCH = [['c0', 'c1'], ['d0']]
INVALID_OLD_COLS_ARGUMENT_DTYPE = [[1], ['d0']]
INVALID_NEW_COLS_ARGUMENT_DTYPE = [['c0'], [1]]
INVALID_COLS_KEYS_ARGUMENT_DTYPE = [{0: 'd0'}, None]
INVALID_COLS_VALUES_ARGUMENT_DTYPE = [{'c0': 1}, None]
DUPLICATE_COL_NAMES = [['c0', 'c1'], ['d0', 'd0']]
COL_NAME_ALREADY_IN_TABLE = [{'c0': 'c1'}, None]
RENAME_COL_TO_INDEX_NAME = [{'c0': DEFAULT_ARROW_INDEX_NAME}, None]


@pytest.mark.parametrize(
    ("args", "exception"),
    [
        (NEW_COL_NAMES_PROVIDED_TWICE, AttributeError),
        (NEW_NAMES_NOT_PROVIDED, AttributeError),
        (NUMBER_OF_COLS_DOESNT_MATCH, ValueError),
        (INVALID_OLD_COLS_ARGUMENT_DTYPE, TypeError),
        (INVALID_NEW_COLS_ARGUMENT_DTYPE, TypeError),
        (INVALID_COLS_KEYS_ARGUMENT_DTYPE, TypeError),
        (INVALID_COLS_VALUES_ARGUMENT_DTYPE, TypeError),
        (DUPLICATE_COL_NAMES, IndexError),
        (COL_NAME_ALREADY_IN_TABLE, IndexError),
        (RENAME_COL_TO_INDEX_NAME, ValueError),
    ],
    ids=[
        "NEW_COL_NAMES_PROVIDED_TWICE",
        "NEW_NAMES_NOT_PROVIDED",
        "NUMBER_OF_COLS_DOESNT_MATCH",
        "INVALID_OLD_COLS_ARGUMENT_DTYPE",
        "INVALID_NEW_COLS_ARGUMENT_DTYPE",
        "INVALID_COLS_KEYS_ARGUMENT_DTYPE",
        "INVALID_COLS_VALUES_ARGUMENT_DTYPE",
        "DUPLICATE_COL_NAMES",
        "COL_NAME_ALREADY_IN_TABLE",
        "RENAME_COL_TO_INDEX_NAME",
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
