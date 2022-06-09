import pytest
from .fixtures import *


def test_rename_table(store):
    # Arrange
    NEW_TABLE_NAME = "new_table_name"
    df = make_table()
    store.write_table(TABLE_NAME, df)
    # Act
    store.rename_table(TABLE_NAME, to=NEW_TABLE_NAME)
    # Assert
    table_names = store.list_tables()
    assert table_names == [NEW_TABLE_NAME]


def test_drop_table(store):
    # Arrange
    df = make_table()
    store.write_table(TABLE_NAME, df)
    # Act
    store.drop_table(TABLE_NAME)
    # Assert
    table_names = store.list_tables()
    assert table_names == []


def test_list_tables_like(store):
    # Arrange
    df = make_table(sorted_datetime_index)
    index_name = get_index_name(df)
    TABLE_NAMES = ("a_table", "AAPL", "MSFT", "TSLA", "AMZN", "FB",
                   "2019-01-01", "saab")

    for table_name in TABLE_NAMES:
        store.write_table(table_name, df, index=index_name)
    # Act
    tables_like_bounded_wildcards = store.list_tables(like="?A??")
    tables_like_unbounded_wildcard = store.list_tables(like="A%")
    # Assert
    assert tables_like_bounded_wildcards == ["AAPL", "saab"]
    assert tables_like_unbounded_wildcard == ["AAPL", "AMZN", "a_table"]


def test_table_exists(store):
    # Arrange
    df = make_table()
    table = store.select_table(TABLE_NAME)
    # Act
    table_existed_before_write = store.table_exists(TABLE_NAME)
    table.write(df)
    table_exists_after_write = table.exists()
    # Assert
    assert not table_existed_before_write
    assert table_exists_after_write


@pytest.mark.parametrize("index", [default_index, sorted_datetime_index])
def test_get_shape(store, index):
    # Arrange
    df = make_table(index, astype="pandas")
    expected = (30, 6)  # table.shape includes index

    table = store.select_table(TABLE_NAME)
    table.write(df)
    # Act
    shape = table.shape
    # Assert
    assert shape == expected


def test_get_index(store):
    # Arrange
    df = make_table(sorted_datetime_index, astype="pandas")
    index_name = get_index_name(df)
    expected = df.index

    table = store.select_table(TABLE_NAME)
    table.write(df, index=index_name)
    # Act
    index = table.index
    # Assert
    assert index.equals(expected)
    assert index.name == expected.name


def test_get_columns(store):
    # Arrange
    df = make_table(sorted_datetime_index)
    index_name = get_index_name(df)
    expected = df.column_names

    table = store.select_table(TABLE_NAME)
    table.write(df, index=index_name)
    # Act
    columns = table.columns
    # Assert
    assert columns == expected


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
