import os

import pyarrow as pa
import pytest

import featherstore as fs

from .fixtures import (
    DB_PATH,
    STORE_NAME,
    TABLE_NAME,
    assert_store_table_equal,
    change_dtype,
    convert_table,
    default_index,
    get_partition_size,
    insert_column_names_at,
    make_table,
    split_table,
)

SNAPSHOT_PATH = os.path.join(DB_PATH, "e2e_table_snapshot.tar.xz")
RESTORE_STORE_NAME = f"{STORE_NAME}_restored"


@pytest.mark.e2e
def test_full_table_workflow(store):
    # Arrange
    inserted_column_names = ["n0", "n1"]
    inserted_column_idx = 2
    inserted_row_indices = [4, 7, 11]
    renamed_columns = {"c0": "a0", "c5": "a5"}
    cast_columns = ["a0"]
    cast_dtype = pa.float32()

    expected = make_table(default_index, rows=30, cols=7, astype="pandas", dtype="int")
    expected = insert_column_names_at(
        expected, inserted_column_names, inserted_column_idx
    )
    row_data, inserted_columns = split_table(expected, cols=inserted_column_names)
    row_data, inserted_rows = split_table(row_data, rows=inserted_row_indices)
    initial, appended_rows = split_table(row_data, rows={"after": 20})

    dropped_rows = [expected.index[0], expected.index[-1]]
    dropped_columns = ["c1"]
    expected, _ = split_table(expected, rows=dropped_rows, cols=dropped_columns)
    expected = expected.rename(columns=renamed_columns)
    reordered_columns = list(reversed(expected.columns))
    expected = expected[reordered_columns]
    expected = convert_table(expected, to="arrow")
    expected = change_dtype(expected, cast_dtype, cols=cast_columns)

    partition_size = get_partition_size(initial)
    table = store.select_table(TABLE_NAME)

    # Act
    table.write(initial, partition_size=partition_size, warnings="ignore")
    table.append(appended_rows, warnings="ignore")
    table.insert(inserted_rows, warnings="ignore")
    table.insert(inserted_columns, idx=inserted_column_idx, warnings="ignore")
    table.drop(rows=dropped_rows, cols=dropped_columns)
    table.rename_columns(renamed_columns)
    table.reorder_columns(reordered_columns)
    table.astype(cast_columns, to=[cast_dtype])
    table.create_snapshot(SNAPSHOT_PATH)
    fs.create_store(RESTORE_STORE_NAME)
    fs.snapshot.restore_table(RESTORE_STORE_NAME, SNAPSHOT_PATH)

    # Assert
    assert_store_table_equal(RESTORE_STORE_NAME, TABLE_NAME, expected)
