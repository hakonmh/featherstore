import pytest
from .fixtures import DB_PATH, STORE_NAME, MD_NAME

import os
import shutil
import featherstore as fs
from featherstore import _metadata


@pytest.fixture(scope="function", name="store")
def setup_db():
    with SetupDB() as store:
        yield store


class SetupDB:
    def __enter__(self):
        # Setup
        if os.path.exists(DB_PATH):
            shutil.rmtree(DB_PATH, ignore_errors=False)
        fs.create_database(DB_PATH, connect=False)
        fs.connect(DB_PATH)
        fs.create_store(STORE_NAME)
        return fs.Store(STORE_NAME)

    def __exit__(self, exception_type, exception_value, traceback):
        # Teardown
        for store_name in fs.list_stores():
            store = fs.Store(store_name)
            for table in store.list_tables():
                store.drop_table(table)
            fs.drop_store(store_name, errors='ignore')
        fs.disconnect()
        shutil.rmtree(DB_PATH, ignore_errors=False)


@pytest.fixture(scope="function")
def create_db():
    # Setup
    fs.create_database(DB_PATH, connect=False)
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


@pytest.fixture(scope="function", name="metadata")
def setup_md():
    with SetupMetadata() as md:
        yield md


class SetupMetadata:
    def __enter__(self):
        # Setup
        if os.path.exists(DB_PATH):
            shutil.rmtree(DB_PATH, ignore_errors=False)
        md = _metadata.Metadata(DB_PATH, MD_NAME)
        md.create()
        return md

    def __exit__(self, exception_type, exception_value, traceback):
        # Teardown
        shutil.rmtree(DB_PATH, ignore_errors=False)
