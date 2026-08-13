import os

import pytest

import featherstore as fs
from featherstore import _metadata

from .fixtures import DB_PATH, MD_NAME, STORE_NAME
from .fixtures.database import setup_incompatible_database
from .fixtures.misc import paths as path_ops


@pytest.fixture
def paths():
    return path_ops


@pytest.fixture(scope="function", name="store")
def setup_db(paths):
    with SetupDB(paths) as store:
        yield store


class SetupDB:
    def __init__(self, paths):
        self._paths = paths

    def __enter__(self):
        # Setup
        self._paths.rmtree(DB_PATH)
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
            fs.drop_store(store_name, warnings="ignore")
        fs.disconnect()
        self._paths.rmtree(DB_PATH)


@pytest.fixture(scope="function")
def create_db(paths):
    # Setup
    paths.rmtree(DB_PATH)
    fs.create_database(DB_PATH, connect=False)
    # Test
    yield
    # Teardown
    paths.rmtree(DB_PATH)


@pytest.fixture(scope="function")
def connect_to_db():
    # Setup
    fs.connect(DB_PATH)
    # Test
    yield
    # Teardown
    if fs.is_connected():
        fs.disconnect()


@pytest.fixture(scope="function")
def empty_directory(paths):
    paths.rmtree(DB_PATH)
    os.makedirs(DB_PATH)
    yield
    paths.rmtree(DB_PATH)


@pytest.fixture(scope="function")
def incompatible_database(request, paths):
    paths.rmtree(DB_PATH)
    setup_incompatible_database(DB_PATH, request.param)
    yield request.param
    paths.rmtree(DB_PATH)


@pytest.fixture(scope="function", name="metadata")
def setup_md(paths):
    with SetupMetadata(paths) as md:
        yield md


class SetupMetadata:
    def __init__(self, paths):
        self._paths = paths

    def __enter__(self):
        # Setup
        self._paths.rmtree(DB_PATH)
        md = _metadata.Metadata(DB_PATH, MD_NAME)
        md.create()
        return md

    def __exit__(self, exception_type, exception_value, traceback):
        # Teardown
        self._paths.rmtree(DB_PATH)
