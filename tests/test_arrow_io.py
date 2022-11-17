import pytest
from .fixtures import *


@pytest.mark.parametrize("index",
                         [default_index, sorted_datetime_index, sorted_string_index,
                          unsorted_int_index, unsorted_datetime_index,
                          unsorted_string_index])
@pytest.mark.parametrize("partition_size", [None, -1])
def test_arrow_io(store, index, partition_size):
    # Arrange
    original_df = make_table(index, astype='arrow')
    index_name = get_index_name(original_df)
    expected = sort_table(original_df, by=index_name)

    partition_size = get_partition_size(original_df, partition_size)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size,
                warnings='ignore')
    df = table.read_arrow()
    # Assert
    assert df.equals(expected)


def test_store_read_arrow(store):
    # Arrange
    original_df = make_table(astype='arrow')
    store.write_table(TABLE_NAME, original_df, warnings='ignore')
    # Act
    df = store.read_arrow(TABLE_NAME)
    # Assert
    assert df.equals(original_df)


@pytest.mark.parametrize(
    ["index", "rows", "cols"],
    [
        (fake_default_index, [0, 5, 12], ['c0', 'c4', 'c2']),
        (sorted_string_index, {"between": ['a', 'f']}, {"like": "c?"})
    ]
)
def test_arrow_filtering(store, index, rows, cols):
    # Arrange
    original_df = make_table(index, astype="arrow")
    index_name = get_index_name(original_df)
    _, expected = split_table(original_df, rows=rows, cols=cols, index_name=index_name, keep_index=True)
    if index == fake_default_index:
        original_df = original_df.drop([DEFAULT_ARROW_INDEX_NAME])
        index_name = None

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size,
                warnings='ignore')
    df = table.read_arrow(rows=rows, cols=cols)
    # Assert
    assert df.equals(expected)
