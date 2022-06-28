import pytest

from tests.test_rename_cols import NUMBER_OF_COLS_DOESNT_MATCH
from .fixtures import *


@pytest.mark.parametrize("use_property", [True, False])
def test_reorder_columns(store, use_property):
    # Arrange
    COLS = ["c1", "c0", "c2", "c4", "c3"]
    original_df = make_table(astype='pandas')
    expected = original_df[COLS]

    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    if use_property:
        table.columns = COLS
    else:
        table.reorder_columns(COLS)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)


COL_NOT_IN_TABLE = ['c0', 'c2', 'd1']
NUMBER_OF_COLS_DOESNT_MATCH = ['c1', 'c0']
INDEX_IS_PROVIDED = ['date', 'c0', 'c1']
CONTAINS_DUPLICATES = ['c0', 'c1', 'c1']
WRONG_DTYPE = ('c1', 'c0', 'c2')
WRONG_ELEMENTS_DTYPE = [1, 'c0', 'c2']


@pytest.mark.parametrize(
    ("cols", "exception"),
    [
        (COL_NOT_IN_TABLE, ValueError),
        (NUMBER_OF_COLS_DOESNT_MATCH, ValueError),
        (INDEX_IS_PROVIDED, ValueError),
        (CONTAINS_DUPLICATES, IndexError),
        (WRONG_DTYPE, TypeError),
        (WRONG_ELEMENTS_DTYPE, TypeError),
    ],
    ids=[
        "COL_NOT_IN_TABLE",
        "INDEX_IS_PROVIDED",
        "CONTAINS_DUPLICATES",
        "WRONG_DTYPE",
        "WRONG_ELEMENTS_DTYPE",
        "NUMBER_OF_COLS_DOESNT_MATCH",
    ],
)
def test_can_reorder_columns(store, cols, exception):
    # Arrange
    original_df = make_table(cols=3, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.columns = cols
