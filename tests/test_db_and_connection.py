import shutil
import os
from .fixtures import *
import featherstore as fs


def test_create_database():
    # Arrange
    before_create_db = os.path.exists(DB_PATH)
    # Act
    fs.create_database(DB_PATH)
    # Assert
    db_exists_after_create_db = os.path.exists(DB_PATH)
    db_folder_is_db = ".featherstore" in os.listdir(DB_PATH)
    assert db_exists_after_create_db and not before_create_db
    assert db_folder_is_db
    # Teardown
    shutil.rmtree(DB_PATH)


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


def test_create_store(create_db, connect_to_db):
    # Act
    fs.create_store("test_store")
    # Assert
    stores = fs.list_stores()
    assert stores == ["test_store"]


def test_drop_store(create_db, connect_to_db):
    # Arrange
    fs.create_store("test_store")
    stores_before_deletion = fs.list_stores()
    # Act
    fs.drop_store("test_store")
    # Assert
    stores_after_deletion = fs.list_stores()
    assert stores_after_deletion == []
    assert len(stores_before_deletion) > len(stores_after_deletion)


def test_rename_store(store):
    # Arrange
    store.rename(to="new_store_name")
    # Act
    stores = fs.list_stores()
    # Assert
    store_name = store.store_name
    assert stores == ["new_store_name"]
    assert store_name == "new_store_name"
