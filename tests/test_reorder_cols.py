import pytest

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


COLS_NOT_IN_TABLE = ['c0', 'c2', 'd1']
NUMBER_OF_COLS_DOESNT_MATCH = ['c1', 'c0']
INDEX_IS_PROVIDED = [DEFAULT_ARROW_INDEX_NAME, 'c0', 'c1']
CONTAINS_DUPLICATES = ['c0', 'c1', 'c1']
INVALID_DTYPE = {'c1', 'c0', 'c2'}
INVALID_ELEMENTS_DTYPE = [1, 'c0', 'c2']


@pytest.mark.parametrize(
    ("cols", "exception"),
    [
        (COLS_NOT_IN_TABLE, ValueError),
        (NUMBER_OF_COLS_DOESNT_MATCH, ValueError),
        (INDEX_IS_PROVIDED, ValueError),
        (CONTAINS_DUPLICATES, IndexError),
        (INVALID_DTYPE, TypeError),
        (INVALID_ELEMENTS_DTYPE, TypeError),
    ],
    ids=[
        "COLS_NOT_IN_TABLE",
        "NUMBER_OF_COLS_DOESNT_MATCH",
        "INDEX_IS_PROVIDED",
        "CONTAINS_DUPLICATES",
        "INVALID_DTYPE",
        "INVALID_ELEMENTS_DTYPE",
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
