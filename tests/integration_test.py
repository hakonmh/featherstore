import pytest
from .fixtures import *


@pytest.mark.integration
def test_windows_permission_error(store):
    # Arrange
    original_df = make_table(rows=100, astype='arrow')
    update_df = make_table(rows=20, astype='pandas')

    partition_size = get_partition_size(original_df, num_partitions=100)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    df = table.read_arrow(mmap=False)
    table.update(update_df)
    df1 = table.read_arrow(mmap=False)
    table.drop_table()
    # Assert
    assert_df_equals(df, original_df)
    assert df != df1


@pytest.mark.integration
def test_linux_memory_mapping(store):
    """Tests that altering an array doesn't change the underlying file"""
    # Arrange
    df = make_table(rows=100, astype='arrow')
    original_df, insert_df = split_table(df, cols=['c4'])

    partition_size = get_partition_size(original_df, num_partitions=100)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    df = table.read_arrow(mmap=True)
    df1 = original_df.append_column('c4', insert_df['c4'])
    # Assert
    assert_df_equals(df, original_df)
    assert df != df1
