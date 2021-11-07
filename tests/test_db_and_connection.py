import featherstore as fs
import os
import shutil

from tests.conftest import DB_PATH
from .fixtures import make_table, sorted_datetime_index


def test_create_database():
    # Arrange
    DATABASE_PATH = os.path.join('tests', 'test_db')
    fs.create_database(DATABASE_PATH)
    # Act
    db_folder_exists = os.path.exists(DATABASE_PATH)
    db_folder_is_db = ".featherstore" in os.listdir(DATABASE_PATH)
    # Assert
    assert db_folder_exists
    assert db_folder_is_db
    # Teardown
    shutil.rmtree(DATABASE_PATH)


def test_connect(basic_data, database, connection):
    # Arrange
    expected = os.path.abspath(basic_data["db_path"])
    # Act
    connection = fs.current_db()
    # Assert
    assert connection == expected


def test_create_store(database, connection):
    # Arrange
    fs.create_store("test_store")
    # Act
    stores = fs.list_stores()
    # Assert
    assert stores == ["test_store"]
    # Teardown
    fs.drop_store("test_store")


def test_drop_store(database, connection):
    # Arrange
    fs.create_store("test_store")
    stores_before_deletion = fs.list_stores()
    fs.drop_store("test_store")
    # Act
    stores_after_deletion = fs.list_stores()
    # Assert
    assert stores_after_deletion == []
    assert len(stores_before_deletion) > len(stores_after_deletion)


def test_rename_store(basic_data, database, connection, store):
    # Arrange
    store.rename(to="new_store_name")
    # Act
    stores = fs.list_stores()
    store_name = store.store_name
    # Assert
    assert stores == ["new_store_name"]
    assert store_name == "new_store_name"
    # Teardown
    store.rename(to=basic_data["store_name"])
