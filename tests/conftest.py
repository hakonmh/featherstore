import shutil

import pytest
import featherstore as fs
from .fixtures import DB_PATH, STORE_NAME


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
