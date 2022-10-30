import featherstore as fs
import pytest
from .fixtures import *

from pandas.testing import assert_frame_equal

SNAPSHOT_PATH = os.path.join(DB_PATH, 'table_snapshot.tar.xz')


def test_table_snapshot(store):
    # Arrange
    original_df = make_table(astype='pandas')
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    table.create_snapshot(SNAPSHOT_PATH)
    table.drop_table()
    table_name = fs.snapshot.restore_table(STORE_NAME, SNAPSHOT_PATH)
    # Assert
    table = store.select_table(table_name)
    df = table.read_pandas()
    assert_frame_equal(df, original_df, check_dtype=True)
    # Teardown
    os.remove(SNAPSHOT_PATH)


def test_store_snapshot(store):
    # Arrange
    store_name = store.store_name
    original_df1 = make_table(astype='pandas')
    original_df2 = make_table(astype='pandas')

    partition_size = get_partition_size(original_df1)
    store.write_table('df1', original_df1, partition_size=partition_size)
    store.write_table('df2', original_df2, partition_size=partition_size)
    # Act
    store.create_snapshot(SNAPSHOT_PATH)
    store.rename(to=f'{store_name}2')
    fs.snapshot.restore_store(SNAPSHOT_PATH)
    # Assert
    _assert_store_equal(store_name, f'{store_name}2')
    # Teardown
    os.remove(SNAPSHOT_PATH)


def _assert_store_equal(store_name1, store_name2):
    store1 = fs.Store(store_name1)
    store2 = fs.Store(store_name2)

    assert store1.list_tables() == store2.list_tables()

    for table_name in store1.list_tables():
        df1 = store1.read_pandas(table_name)
        df2 = store2.read_pandas(table_name)
        assert_frame_equal(df1, df2)


def test_that_restoring_snapshot_cannot_overwrite_existing_table(store):
    # Arrange
    original_df = make_table(astype='pandas')

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    table.create_snapshot(SNAPSHOT_PATH)
    # Assert
    with pytest.raises(FileExistsError):
        fs.snapshot.restore_table(STORE_NAME, SNAPSHOT_PATH)


def test_that_restoring_snapshot_cannot_overwrite_existing_store(store):
    original_df = make_table(astype='pandas')

    partition_size = get_partition_size(original_df)
    store.write_table(TABLE_NAME, original_df, partition_size=partition_size)
    # Act
    store.create_snapshot(SNAPSHOT_PATH)
    # Assert
    with pytest.raises(FileExistsError):
        fs.snapshot.restore_store(SNAPSHOT_PATH)
