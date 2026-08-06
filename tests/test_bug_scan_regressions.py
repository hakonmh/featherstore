"""Regression tests for bugs found in the repository scan."""

import os
import shutil

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import featherstore as fs
from featherstore._metadata import METADATA_FOLDER_NAME
from featherstore._table._indexers import RowIndexer
from featherstore._table._table_utils import (
    INSERTION_BUFFER_LENGTH,
    convert_int_to_partition_id,
    convert_partition_id_to_float,
    convert_partition_id_to_int,
)
from featherstore._utils import filter_items_like_pattern
from featherstore.exceptions import ForbiddenTableNameError

from .fixtures import (
    DEFAULT_ARROW_INDEX_NAME,
    TABLE_NAME,
    assert_table_equals,
    get_partition_size,
    make_table,
)


def test_repeated_mid_table_inserts_preserve_all_rows(store):
    # Arrange — many partitions so inserts create mid-gap partition IDs
    original = make_table(rows=100, cols=3, astype="pandas")
    original.index = pd.Index(np.arange(100, dtype=float), name=original.index.name)
    first_insert = make_table(rows=10, cols=3, astype="pandas")
    first_insert.index = pd.Index(
        [x + 0.5 for x in range(10, 20)], name=original.index.name
    )
    second_insert = make_table(rows=10, cols=3, astype="pandas")
    second_insert.index = pd.Index(
        [x + 0.25 for x in range(10, 20)], name=original.index.name
    )

    expected = pd.concat([original, first_insert, second_insert]).sort_index()
    partition_size = get_partition_size(original, num_partitions=17)
    table = store.select_table(TABLE_NAME)
    table.write(original, partition_size=partition_size, warnings="ignore")
    # Act
    table.insert_rows(first_insert)
    table.insert_rows(second_insert)
    # Assert
    assert table.shape[0] == len(expected)
    assert_table_equals(table, expected)


def test_partition_id_float_roundtrip_is_lossless():
    for value in (1, 1.5, 1.25, 4 + 1 / 3, 17.999999):
        encoded = convert_int_to_partition_id(value)
        assert convert_partition_id_to_float(encoded) == pytest.approx(
            int(value * INSERTION_BUFFER_LENGTH) / INSERTION_BUFFER_LENGTH
        )


def test_partition_id_int_conversion_still_floors_for_append():
    encoded = convert_int_to_partition_id(1.5)
    assert convert_partition_id_to_int(encoded) == 1


def test_reorder_columns_keeps_index_in_metadata(store):
    # Arrange
    df = make_table(cols=3, astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(df)
    # Act
    table.reorder_columns(["c1", "c0", "c2"])
    # Assert
    assert table.columns[0] == DEFAULT_ARROW_INDEX_NAME
    assert table.columns[1:] == ["c1", "c0", "c2"]
    table.drop_columns(["c0"])
    assert table.columns == [DEFAULT_ARROW_INDEX_NAME, "c1", "c2"]


def test_cannot_rename_table_to_metadata_folder_name(store):
    df = make_table(astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(df)
    with pytest.raises(ForbiddenTableNameError):
        table.rename_table(to=METADATA_FOLDER_NAME)


def test_restore_table_ignore_removes_orphan_partitions(store):
    small = make_table(rows=2, cols=2, astype="pandas")
    large = make_table(rows=50, cols=2, astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(small, partition_size=1024)
    snapshot_path = os.path.join(store._store_path, "small_snap")
    table.create_snapshot(snapshot_path)

    store.drop_table(TABLE_NAME)
    table.write(large, partition_size=get_partition_size(large, num_partitions=10))
    feather_before = [
        name for name in os.listdir(table._table_path) if name.endswith(".feather")
    ]
    assert len(feather_before) > 1

    fs.snapshot.restore_table(store.name, snapshot_path, errors="ignore")
    table = store.select_table(TABLE_NAME)
    feather_after = [
        name for name in os.listdir(table._table_path) if name.endswith(".feather")
    ]
    assert len(feather_after) == table._table_data["num_partitions"]
    assert_table_equals(table, small)
    os.remove(f"{snapshot_path}.tar.xz")


def test_restore_rejects_invalid_errors_value(store):
    df = make_table(astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(df)
    snapshot_path = os.path.join(store._store_path, "err_snap")
    table.create_snapshot(snapshot_path)
    with pytest.raises(ValueError, match="errors"):
        fs.snapshot.restore_table(store.name, snapshot_path, errors="bogus")
    os.remove(f"{snapshot_path}.tar.xz")


def test_insert_and_drop_columns_update_shape(store):
    df = make_table(cols=3, astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(df)
    assert table.shape == (30, 4)

    new_col = pd.DataFrame({"c99": range(30)}, index=df.index)
    table.insert_columns(new_col)
    assert table.shape == (30, 5)
    assert len(table.columns) == 5

    table.drop_columns(["c99"])
    assert table.shape == (30, 4)


def test_astype_index_updates_index_dtype_metadata(store):
    df = make_table(cols=1, astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(df)
    index_name = table._table_data["index_name"]
    assert table._table_data["index_dtype"] == "int64"

    table.astype({index_name: pa.int32()})
    assert table._table_data["index_dtype"] == "int32"


def test_database_exists_expands_home_directory(create_db):
    home_db = os.path.expanduser("~/featherstore_bugscan_db_exists")
    if os.path.exists(home_db):
        shutil.rmtree(home_db)
    fs.create_database(home_db, connect=False)
    try:
        assert fs.database_exists(home_db)
        assert fs.database_exists("~/featherstore_bugscan_db_exists")
    finally:
        shutil.rmtree(home_db)


def test_row_indexer_keyword_defaults_to_none_for_unknown_keys():
    rows = RowIndexer({"not_a_keyword": 1})
    assert rows.keyword is None


def test_like_pattern_treats_regex_metacharacters_as_literals():
    items = ["col.name", "colXname", "other"]
    assert filter_items_like_pattern(items, like="col.name") == ["col.name"]


def test_like_pattern_supports_empty_string():
    assert filter_items_like_pattern(["", "a"], like="") == [""]
