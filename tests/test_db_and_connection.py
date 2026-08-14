import os
import warnings

import pytest

import featherstore as fs
from featherstore.exceptions import (
    DatabaseNotEmptyError,
    ForbiddenStoreNameError,
    IncompatibleDatabaseVersionError,
    NotADatabaseError,
    NotConnectedError,
    PopulatedDirectoryError,
    StoreNotEmptyError,
)

from .fixtures import DB_PATH, TABLE_NAME, make_table


def test_create_database(paths):
    # Arrange
    before_create_db = os.path.exists(DB_PATH)
    # Act
    fs.create_database(DB_PATH, connect=False)
    # Assert
    db_exists_after_create_db = os.path.exists(DB_PATH)
    db_folder_is_db = fs.database_exists(DB_PATH)
    assert db_exists_after_create_db and not before_create_db
    assert db_folder_is_db
    # Teardown
    paths.rmtree(DB_PATH)


def test_database_exists(create_db):
    assert fs.database_exists(DB_PATH)
    assert not fs.database_exists(os.path.join(DB_PATH, "missing"))


def test_database_exists_expands_home_dir_modifier(paths):
    # Arrange
    path = os.path.join("~", ".featherstore_test_db_exists")
    expanded = os.path.expanduser(path)
    paths.rmtree(expanded)
    fs.create_database(path, connect=False)
    # Act
    exists_with_modifier = fs.database_exists(path)
    exists_expanded = fs.database_exists(expanded)
    # Assert
    assert exists_with_modifier
    assert exists_expanded
    # Teardown
    paths.rmtree(expanded)


def test_connect(create_db):
    # Arrange
    was_connected = fs.is_connected()
    # Act
    fs.connect(DB_PATH)
    # Assert
    is_connected = fs.is_connected()
    assert not was_connected
    assert is_connected
    # Teardown
    fs.disconnect()


def test_disconnect(create_db, connect_to_db):
    # Arrange
    was_connected = fs.is_connected()
    # Act
    fs.disconnect()
    # Assert
    is_connected = fs.is_connected()
    assert was_connected
    assert not is_connected
    # Teardown
    fs.connect(DB_PATH)


def test_disconnect_raises_when_not_connected(create_db):
    # Act / Assert
    with pytest.raises(NotConnectedError):
        fs.disconnect()


def test_current_db_raises_when_not_connected(create_db):
    # Act / Assert
    with pytest.raises(NotConnectedError):
        fs.current_db()


def test_list_stores_raises_when_not_connected(create_db):
    # Act / Assert
    with pytest.raises(NotConnectedError):
        fs.list_stores()


def test_create_database_raises_when_directory_is_populated(empty_directory):
    # Arrange
    with open(os.path.join(DB_PATH, "dummy.txt"), "w", encoding="utf-8") as f:
        f.write("x")
    # Act / Assert
    with pytest.raises(PopulatedDirectoryError):
        fs.create_database(DB_PATH, connect=False)


def test_create_database_can_ignore_populated_directory(empty_directory):
    # Arrange
    with open(os.path.join(DB_PATH, "dummy.txt"), "w", encoding="utf-8") as f:
        f.write("x")
    # Act
    fs.create_database(DB_PATH, errors="ignore", connect=False)
    # Assert
    assert fs.database_exists(DB_PATH)


def test_create_database_ignore_does_not_rewrite_existing_marker(create_db):
    # Arrange
    marker = os.path.join(DB_PATH, ".featherstore")
    mtime_before = os.stat(marker).st_mtime_ns
    # Act
    fs.create_database(DB_PATH, errors="ignore", connect=False)
    # Assert
    assert os.stat(marker).st_mtime_ns == mtime_before


def test_create_database_rejects_invalid_errors_argument():
    # Act / Assert
    with pytest.raises(ValueError, match="'errors' must be either"):
        fs.create_database(DB_PATH, errors="invalid", connect=False)


def test_drop_database(create_db, connect_to_db):
    # Act
    fs.drop_database(DB_PATH)
    # Assert
    assert not fs.is_connected()
    assert not fs.database_exists(DB_PATH)
    assert not os.path.exists(DB_PATH)


def test_drop_database_raises_when_database_contains_stores(create_db, connect_to_db):
    # Arrange
    fs.create_store("test_store")
    # Act / Assert
    with pytest.raises(DatabaseNotEmptyError):
        fs.drop_database(DB_PATH)


def test_drop_database_raises_when_not_connected(create_db):
    # Act / Assert
    with pytest.raises(NotConnectedError):
        fs.drop_database(DB_PATH)


def test_drop_database_raises_when_path_is_not_current_database(
    create_db, connect_to_db
):
    # Act / Assert
    with pytest.raises(ValueError, match="currently connected database"):
        fs.drop_database(os.path.join(DB_PATH, "other"))


def test_drop_database_rejects_invalid_warnings_argument(create_db, connect_to_db):
    # Act / Assert
    with pytest.raises(ValueError, match="'warnings' must be either"):
        fs.drop_database(DB_PATH, warnings="invalid")


def test_create_store(create_db, connect_to_db):
    # Act
    fs.create_store("test_store")
    # Assert
    stores = fs.list_stores()
    assert stores == ["test_store"]


def test_create_store_warns_when_store_already_exists(create_db, connect_to_db):
    # Arrange
    fs.create_store("test_store")
    # Act / Assert
    with pytest.warns(UserWarning, match="already exists"):
        store = fs.create_store("test_store")
    assert store.name == "test_store"
    assert fs.list_stores() == ["test_store"]


def test_create_store_can_ignore_existing_store_warning(create_db, connect_to_db):
    # Arrange
    fs.create_store("test_store")
    # Act / Assert
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        store = fs.create_store("test_store", warnings="ignore")
    assert store.name == "test_store"


def test_drop_store(create_db, connect_to_db):
    # Arrange
    store = fs.create_store("test_store")
    stores_existed_before_delete = fs.store_exists(store.name)
    # Act
    fs.drop_store(store.name)
    # Assert
    assert stores_existed_before_delete
    assert not fs.store_exists(store.name)


def test_drop_store_warns_when_store_does_not_exist(create_db, connect_to_db):
    # Act / Assert
    with pytest.warns(UserWarning, match="doesn't exist"):
        fs.drop_store("missing_store")
    assert not fs.store_exists("missing_store")


def test_drop_store_can_ignore_missing_store_warning(create_db, connect_to_db):
    # Act / Assert
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        fs.drop_store("missing_store", warnings="ignore")


def test_drop_store_raises_when_store_contains_tables(store):
    # Arrange
    store.write_table(TABLE_NAME, make_table(astype="pandas"))
    # Act / Assert
    with pytest.raises(StoreNotEmptyError):
        fs.drop_store(store.name)


def test_store_drop(store):
    # Act
    store.drop()
    # Assert
    assert not fs.store_exists(store.name)


def test_store_rename(store):
    # Arrange
    store.rename(to="new_store_name")
    # Act
    stores = fs.list_stores()
    # Assert
    store_name = store.name
    assert stores == ["new_store_name"]
    assert store_name == "new_store_name"


def test_rename_store(store):
    # Arrange
    fs.rename_store(store.name, to="new_store_name")
    # Act
    stores = fs.list_stores()
    # Assert
    assert stores == ["new_store_name"]


def test_store_exists(create_db, connect_to_db):
    # Arrange
    store_existed_before_write = fs.store_exists("test_store")
    # Act
    fs.create_store("test_store")
    # Assert
    store_exists_after_write = fs.store_exists("test_store")
    assert not store_existed_before_write
    assert store_exists_after_write


def test_list_stores(create_db, connect_to_db):
    # Arrange
    fs.create_store("store")
    fs.create_store("bonds")
    fs.create_store("stocks")
    # Act
    stores = fs.list_stores(like="sto%")
    # Assert
    assert stores == ["stocks", "store"]


FORBIDDEN_STORE_NAMES = (
    "",
    ".",
    "..",
    "../outside_target",
    "..\\outside_target",
    "foo/bar",
    "foo\\bar",
    ".featherstore",
)


@pytest.mark.parametrize("store_name", FORBIDDEN_STORE_NAMES, ids=repr)
def test_create_store_rejects_forbidden_name(create_db, connect_to_db, store_name):
    # Act / Assert
    with pytest.raises(ForbiddenStoreNameError):
        fs.create_store(store_name)


@pytest.mark.parametrize("store_name", FORBIDDEN_STORE_NAMES, ids=repr)
def test_store_init_rejects_forbidden_name(create_db, connect_to_db, store_name):
    # Act / Assert
    with pytest.raises(ForbiddenStoreNameError):
        fs.Store(store_name)


@pytest.mark.parametrize("store_name", FORBIDDEN_STORE_NAMES, ids=repr)
def test_store_exists_rejects_forbidden_name(create_db, connect_to_db, store_name):
    # Act / Assert
    with pytest.raises(ForbiddenStoreNameError):
        fs.store_exists(store_name)


@pytest.mark.parametrize("store_name", FORBIDDEN_STORE_NAMES, ids=repr)
def test_drop_store_rejects_forbidden_name(create_db, connect_to_db, store_name):
    # Act / Assert
    with pytest.raises(ForbiddenStoreNameError):
        fs.drop_store(store_name)


@pytest.mark.parametrize("store_name", FORBIDDEN_STORE_NAMES, ids=repr)
def test_rename_store_rejects_forbidden_name(store, store_name):
    # Act / Assert
    with pytest.raises(ForbiddenStoreNameError):
        store.rename(to=store_name)


def test_create_store_allows_name_containing_double_dot(create_db, connect_to_db):
    # Act
    fs.create_store("foo..bar")
    # Assert
    assert fs.list_stores() == ["foo..bar"]


def test_table_rejects_forbidden_store_name(store):
    # Act / Assert
    with pytest.raises(ForbiddenStoreNameError):
        fs.Table("table_name", "..")


def test_create_database_allows_connect(paths):
    # Arrange
    paths.rmtree(DB_PATH)
    # Act
    fs.create_database(DB_PATH)
    # Assert
    assert fs.is_connected()
    assert fs.database_exists(DB_PATH)
    # Teardown
    fs.disconnect()
    paths.rmtree(DB_PATH)


@pytest.mark.parametrize(
    "incompatible_database",
    [
        "empty_legacy_marker",
        "invalid_json",
        "missing_metadata_schema_version",
        "non_integer_metadata_schema_version",
        "outdated_metadata_schema",
        "outdated_partition_layout",
        "newer_metadata_schema",
        "newer_partition_layout",
    ],
    indirect=True,
)
def test_connect_rejects_incompatible_database(incompatible_database):
    # Act / Assert
    with pytest.raises(IncompatibleDatabaseVersionError):
        fs.connect(DB_PATH)


@pytest.mark.parametrize(
    "incompatible_database", ["empty_legacy_marker"], indirect=True
)
def test_database_exists_does_not_verify_format_compatibility(incompatible_database):
    # Act
    exists = fs.database_exists(DB_PATH)
    # Assert
    assert exists


def test_connect_rejects_directory_without_database_marker(empty_directory):
    # Act / Assert
    with pytest.raises(NotADatabaseError):
        fs.connect(DB_PATH)


@pytest.mark.parametrize("incompatible_database", ["outdated_format"], indirect=True)
def test_connect_reports_stored_and_expected_versions_on_mismatch(
    incompatible_database,
):
    # Act / Assert
    with pytest.raises(IncompatibleDatabaseVersionError) as exc_info:
        fs.connect(DB_PATH)
    message = str(exc_info.value)
    assert "metadata_schema_version" in message
    assert "partition_layout_version" in message
    assert "expected" in message
