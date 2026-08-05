import os

import pytest

import featherstore as fs

from .fixtures import (
    DB_PATH,
    STORE_NAME,
    TABLE_NAME,
    apply_operations_to_expected,
    assert_store_table_equal,
    build_e2e_operations,
    get_partition_size,
    make_hardcoded_table,
    shuffle_e2e_operations,
)

SNAPSHOT_PATH = os.path.join(DB_PATH, "e2e_table_snapshot.tar.xz")
RESTORE_STORE_NAME = f"{STORE_NAME}_restored"


@pytest.mark.e2e
def test_full_table_workflow(store):
    # Arrange
    df = make_hardcoded_table()
    operations = shuffle_e2e_operations(build_e2e_operations(), seed=0)
    expected = apply_operations_to_expected(df, operations)

    partition_size = get_partition_size(df)
    table = store.select_table(TABLE_NAME)
    table.write(df, partition_size=partition_size, warnings="ignore")

    # Act
    for operation in operations:
        operation.apply_to_table(table)

    table.create_snapshot(SNAPSHOT_PATH)
    fs.create_store(RESTORE_STORE_NAME)
    fs.snapshot.restore_table(RESTORE_STORE_NAME, SNAPSHOT_PATH)

    # Assert
    assert_store_table_equal(RESTORE_STORE_NAME, TABLE_NAME, expected)

    # Teardown
    os.remove(SNAPSHOT_PATH)
