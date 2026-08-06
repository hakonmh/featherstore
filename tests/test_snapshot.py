import os

import pytest

import featherstore as fs
from featherstore.exceptions import StoreAlreadyExistsError, TableAlreadyExistsError

from .fixtures import (
    DB_PATH,
    STORE_NAME,
    TABLE_NAME,
    assert_df_equals,
    assert_table_equals,
    get_partition_size,
    make_table,
)

SNAPSHOT_PATH = os.path.join(DB_PATH, "table_snapshot.tar.xz")


def test_table_snapshot(store):
    # Arrange
    original_df = make_table(astype="pandas")
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    table.create_snapshot(SNAPSHOT_PATH)
    table.drop_table()
    table_name = fs.snapshot.restore_table(STORE_NAME, SNAPSHOT_PATH)
    # Assert
    table = store.select_table(table_name)
    assert_table_equals(table, original_df)
    # Teardown
    os.remove(SNAPSHOT_PATH)


def test_store_snapshot(store):
    # Arrange
    store_name = store.name
    original_df1 = make_table(astype="pandas")
    original_df2 = make_table(astype="pandas")

    partition_size = get_partition_size(original_df1)
    store.write_table("df1", original_df1, partition_size=partition_size)
    store.write_table("df2", original_df2, partition_size=partition_size)
    # Act
    store.create_snapshot(SNAPSHOT_PATH)
    store.rename(to=f"{store_name}2")
    fs.snapshot.restore_store(SNAPSHOT_PATH)
    # Assert
    _assert_store_equal(store_name, f"{store_name}2")
    # Teardown
    os.remove(SNAPSHOT_PATH)


def _assert_store_equal(store_name1, store_name2):
    store1 = fs.Store(store_name1)
    store2 = fs.Store(store_name2)

    assert store1.list_tables() == store2.list_tables()

    for table_name in store1.list_tables():
        df1 = store1.read_pandas(table_name)
        df2 = store2.read_pandas(table_name)
        assert_df_equals(df1, df2)


def test_that_restoring_snapshot_cannot_overwrite_existing_table(store):
    # Arrange
    original_df = make_table(astype="pandas")

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    table.create_snapshot(SNAPSHOT_PATH)
    # Assert
    with pytest.raises(TableAlreadyExistsError):
        fs.snapshot.restore_table(STORE_NAME, SNAPSHOT_PATH)


def test_that_restoring_snapshot_cannot_overwrite_existing_store(store):
    original_df = make_table(astype="pandas")

    partition_size = get_partition_size(original_df)
    store.write_table(TABLE_NAME, original_df, partition_size=partition_size)
    # Act
    store.create_snapshot(SNAPSHOT_PATH)
    # Assert
    with pytest.raises(StoreAlreadyExistsError):
        fs.snapshot.restore_store(SNAPSHOT_PATH)


def test_restoring_snapshot_overwrites_existing_table(store):
    # Arrange
    original_df = make_table(rows=2, cols=2, astype="pandas")
    larger_df = make_table(rows=50, cols=2, astype="pandas")

    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=1024)
    table.create_snapshot(SNAPSHOT_PATH)

    store.drop_table(TABLE_NAME)
    partition_size = get_partition_size(larger_df, num_partitions=10)
    table.write(larger_df, partition_size=partition_size)
    # Act
    fs.snapshot.restore_table(STORE_NAME, SNAPSHOT_PATH, errors="ignore")
    # Assert
    table = store.select_table(TABLE_NAME)
    assert_table_equals(table, original_df)
    partition_files = [
        name for name in os.listdir(table._table_path) if name.endswith(".feather")
    ]
    assert len(partition_files) == len(table._partition_data.keys())
    # Teardown
    os.remove(SNAPSHOT_PATH)


def test_that_restoring_snapshot_rejects_invalid_errors_argument(store):
    # Arrange
    original_df = make_table(astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    table.create_snapshot(SNAPSHOT_PATH)
    # Act and Assert
    with pytest.raises(ValueError):
        fs.snapshot.restore_table(STORE_NAME, SNAPSHOT_PATH, errors="invalid")
    # Teardown
    os.remove(SNAPSHOT_PATH)
