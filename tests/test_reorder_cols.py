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


def _cols_doesnt_match():
    return ['c0', 'c2', 'd1']


def _index_provided():
    return ['date', 'c0', 'c1']


def _contains_duplicates():
    return ['c0', 'c1', 'c1']


@pytest.mark.parametrize(
    ("cols", "exception"),
    [
        (_cols_doesnt_match(), ValueError),
        (_index_provided(), ValueError),
        (_contains_duplicates(), IndexError),
    ],
    ids=[
        "_cols_doesnt_match",
        "_index_provided",
        "_contains_duplicates"
    ],
)
def test_can_reorder_columns(store, cols, exception):
    # Arrange
    original_df = make_table(cols=3, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.columns = cols
    # Assert
    assert isinstance(e.type(), exception)
