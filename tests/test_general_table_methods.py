import pytest
from .fixtures import make_table, sorted_datetime_index, get_index_name


def test_rename_table(store):
    # Arrange
    NEW_TABLE_NAME = "new_table_name"
    df = make_table()
    store.write_table("table_name", df)
    store.rename_table("table_name", to=NEW_TABLE_NAME)
    table = store.select_table(NEW_TABLE_NAME)
    # Act
    table_names = store.list_tables()
    # Assert
    assert table_names == [NEW_TABLE_NAME]


def test_drop_table(store):
    # Arrange
    df = make_table()
    store.write_table("table_name", df)
    store.drop_table("table_name")
    # Act
    table_names = store.list_tables()
    # Assert
    assert table_names == []


def test_list_tables_like(store):
    # Arrange
    df = make_table(sorted_datetime_index)
    index_name = get_index_name(df)
    TABLE_NAMES = (
        "a_table",
        "AAPL",
        "MSFT",
        "TSLA",
        "AMZN",
        "FB",
        "2019-01-01",
        "saab",
    )
    for table_name in TABLE_NAMES:
        store.write_table(table_name, df, index=index_name)
    # Act
    tables_like_unbounded_wildcard = store.list_tables(like="A%")
    tables_like_bounded_wildcards = store.list_tables(like="?A??")
    # Assert
    assert tables_like_unbounded_wildcard == ["AAPL", "AMZN", "a_table"]
    assert tables_like_bounded_wildcards == ["AAPL", "saab"]


def test_get_index(store):
    # Arrange
    df = make_table(sorted_datetime_index, astype="pandas")
    index_name = get_index_name(df)
    expected = df.index
    store.write_table("table_name", df, index=index_name)
    table = store.select_table("table_name")
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
    store.write_table("table_name", df, index=index_name)
    table = store.select_table("table_name")
    # Act
    columns = table.columns
    # Assert
    assert columns == expected


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
def test_can_reorder_columns(cols, exception, basic_data, store):
    # Arrange
    original_df = make_table(sorted_datetime_index, cols=3, astype='pandas')
    table = store.select_table(basic_data["table_name"])
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.columns = cols
    # Assert
    assert isinstance(e.type(), exception)


def test_reorder_columns(store):
    # Arrange
    df = make_table(sorted_datetime_index, astype="pandas")
    cols = ["c1", "c0", "c2", "c4", "c3"]
    expected = df[cols]
    store.write_table("table_name", df)
    table = store.select_table("table_name")
    # Act
    table.reorder_columns(cols)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)
