import shutil
import os
from .fixtures import *
import featherstore as fs


def test_create_database():
    # Arrange
    before_create_db = os.path.exists(DB_PATH)
    # Act
    fs.create_database(DB_PATH, connect=False)
    # Assert
    db_exists_after_create_db = os.path.exists(DB_PATH)
    db_folder_is_db = DB_MARKER_NAME in os.listdir(DB_PATH)
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
    store = fs.create_store("test_store")
    stores_existed_before_delete = fs.store_exists(store.store_name)
    # Act
    fs.drop_store(store.store_name)
    # Assert
    assert stores_existed_before_delete
    assert not fs.store_exists(store.store_name)


def test_store_drop(store):
    # Act
    store.drop()
    # Assert
    assert not fs.store_exists(store.store_name)


def test_store_rename(store):
    # Arrange
    store.rename(to="new_store_name")
    # Act
    stores = fs.list_stores()
    # Assert
    store_name = store.store_name
    assert stores == ["new_store_name"]
    assert store_name == "new_store_name"


def test_rename_store(store):
    # Arrange
    fs.rename_store(store.store_name, to="new_store_name")
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
    assert stores == ["stocks", 'store']
