import featherstore as fs

import pytest
import shutil
import pyarrow
import numpy as np
import pandas as pd
from pandas._testing import rands_array

DB_PATH = 'tests/db'
STORE_NAME = 'test_store'
TABLE_NAME = 'table_name'
TABLE_PATH = f'{DB_PATH}/{STORE_NAME}/{TABLE_NAME}'
NUMBER_OF_PARTITIONS = 5


@pytest.fixture(scope='session')
def database():
    # Setup
    fs.create_database(DB_PATH)
    # Test
    yield
    # Teardown
    shutil.rmtree(DB_PATH)


@pytest.fixture(scope='session')
def connection():
    # Setup
    fs.connect(DB_PATH)
    # Test
    yield
    # Teardown
    fs.disconnect()


@pytest.fixture(scope='function')
def store():
    # Setup
    fs.create_store(STORE_NAME)
    store = fs.Store(STORE_NAME)
    # Test
    yield store
    # Teardown
    for table in store.list_tables():
        store.drop_table(table)
    fs.drop_store(STORE_NAME)


@pytest.fixture(scope='session')
def basic_data():
    data = {}
    data['db_path'] = DB_PATH
    data['store_name'] = STORE_NAME
    data['table_name'] = TABLE_NAME
    data['table_path'] = TABLE_PATH
    data['num_partitions'] = NUMBER_OF_PARTITIONS
    return data
