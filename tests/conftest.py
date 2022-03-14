import featherstore as fs

import pytest
import os
import shutil

DB_PATH = os.path.join('tests', 'db')
STORE_NAME = "test_store"
TABLE_NAME = "table_name"
TABLE_PATH = os.path.join(DB_PATH, STORE_NAME, TABLE_NAME)
NUMBER_OF_PARTITIONS = 5


@pytest.fixture(scope="function", name="store")
def setup_db():
    with SetupDB() as store:
        yield store


class SetupDB:
    def __enter__(self):
        # Setup
        fs.create_database(DB_PATH)
        fs.connect(DB_PATH)
        fs.create_store(STORE_NAME)
        self._store = fs.Store(STORE_NAME)
        return self._store

    def __exit__(self, exception_type, exception_value, traceback):
        # Teardown
        for table in self._store.list_tables():
            self._store.drop_table(table)
        fs.drop_store(STORE_NAME)
        fs.disconnect()
        shutil.rmtree(DB_PATH, ignore_errors=False)


@pytest.fixture(scope="function")
def create_db():
    # Setup
    fs.create_database(DB_PATH)
    # Test
    yield
    # Teardown
    shutil.rmtree(DB_PATH, ignore_errors=False)


@pytest.fixture(scope="function")
def connect_to_db():
    # Setup
    fs.connect(DB_PATH)
    # Test
    yield
    # Teardown
    fs.disconnect()


@pytest.fixture(scope="session")
def basic_data():
    data = {}
    data["db_path"] = DB_PATH
    data["store_name"] = STORE_NAME
    data["table_name"] = TABLE_NAME
    data["table_path"] = TABLE_PATH
    data["num_partitions"] = NUMBER_OF_PARTITIONS
    return data
