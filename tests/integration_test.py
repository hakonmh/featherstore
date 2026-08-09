import pytest

import featherstore as fs
from featherstore.exceptions import NotConnectedError

from .fixtures import (
    DB_PATH,
    TABLE_NAME,
    assert_df_equals,
    get_partition_size,
    make_table,
    regenerate_values,
    split_table,
    update_table,
)
from .fixtures.database import remove_database_marker


@pytest.mark.integration
def test_windows_permission_error(store):
    # Arrange
    original_df = make_table(rows=100, astype="arrow")
    update_df = make_table(rows=20, astype="pandas")

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
    df = make_table(rows=100, astype="arrow")
    original_df, insert_df = split_table(df, cols=["c4"])

    partition_size = get_partition_size(original_df, num_partitions=100)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    df = table.read_arrow(mmap=True)
    df1 = original_df.append_column("c4", insert_df["c4"])
    # Assert
    assert_df_equals(df, original_df)
    assert df != df1


@pytest.mark.integration
def test_table_data_survives_disconnect_reconnect_and_update(store):
    # Arrange
    original_df = make_table(astype="pandas")
    _, update_df = split_table(
        original_df, rows=[10, 13, 14, 21], cols=["c1", "c3", "c2"]
    )
    update_df = regenerate_values(update_df)
    expected_df = update_table(original_df, update_df)
    store.write_table(TABLE_NAME, original_df)

    # Act
    fs.disconnect()
    fs.connect(DB_PATH)
    after_reconnect = fs.Store(store.name).read_pandas(TABLE_NAME)
    fs.Store(store.name).select_table(TABLE_NAME).update(update_df)
    fs.disconnect()
    fs.connect(DB_PATH)
    after_update_reconnect = fs.Store(store.name).read_pandas(TABLE_NAME)

    # Assert
    assert_df_equals(after_reconnect, original_df)
    assert_df_equals(after_update_reconnect, expected_df)


@pytest.mark.integration
def test_removing_database_marker_while_connected_makes_is_connected_false(create_db):
    # Arrange
    fs.connect(DB_PATH)
    remove_database_marker(DB_PATH)
    # Act
    connected = fs.is_connected()
    # Assert
    assert not connected
    # Teardown
    fs.create_database(DB_PATH, errors="ignore", connect=True)
    fs.disconnect()


@pytest.mark.integration
def test_removing_database_marker_while_connected_raises_not_connected(create_db):
    # Arrange
    fs.connect(DB_PATH)
    remove_database_marker(DB_PATH)
    # Act / Assert
    with pytest.raises(NotConnectedError):
        fs.current_db()
    # Teardown
    fs.create_database(DB_PATH, errors="ignore", connect=True)
    fs.disconnect()
