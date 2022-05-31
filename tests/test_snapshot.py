from .fixtures import *
from pandas.testing import assert_frame_equal
from featherstore import snapshot


def test_table_snapshot(store):
    # Arrange
    SNAPSHOT_PATH = 'tests/db/table_snapshot'
    original_df = make_table(astype='pandas')
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.create_snapshot(SNAPSHOT_PATH)
    table.drop_table()
    snapshot.restore_table(STORE_NAME, SNAPSHOT_PATH)
    # Assert
    df = table.read_pandas()
    assert_frame_equal(df, original_df, check_dtype=True)


def test_store_snapshot(store):
    # Arrange
    SNAPSHOT_PATH = 'tests/db/store_snapshot'
    original_df1 = make_table(astype='pandas')
    original_df2 = make_table(astype='pandas')
    partition_size = get_partition_size(original_df1)
    store.write_table('df1', original_df1,
                      partition_size=partition_size)
    store.write_table('df2', original_df2,
                      partition_size=partition_size)
    # Act
    store.create_snapshot(SNAPSHOT_PATH)
    _drop_store(store)
    snapshot.restore_store(SNAPSHOT_PATH)
    # Assert
    df1 = store.read_pandas('df1')
    df2 = store.read_pandas('df2')
    assert_frame_equal(df1, original_df1, check_dtype=True)
    assert_frame_equal(df2, original_df2, check_dtype=True)


def _drop_store(store):
    for table_name in store.list_tables():
        store.drop_table(table_name)
    store.drop()
