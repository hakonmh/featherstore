from .fixtures import *

import os
import shutil
from featherstore._metadata import Metadata, METADATA_FOLDER_NAME


def test_create():
    # Arrange
    MD_PATH = os.path.join(DB_PATH, METADATA_FOLDER_NAME)
    metadata = Metadata(DB_PATH, METADATA_FOLDER_NAME)
    # Act
    metadata.create()
    # Assert
    assert os.path.exists(MD_PATH)
    # Teardown
    shutil.rmtree(DB_PATH, ignore_errors=False)


def test_io(metadata):
    # Arrange
    items = {f'a{i}': i for i in range(100)}
    # Act
    metadata.write(items)
    # Assert
    assert metadata.read() == items


def test_overwrite(metadata):
    # Arrange
    items = {f'a{i}': i for i in range(100)}
    metadata.write(items)
    new_items = {f'a{i}': str(i) for i in range(100)}
    # Act
    metadata.write(new_items)
    # Assert
    assert metadata.read() == new_items
    assert metadata.read() != items


def test_init(metadata):
    # Arrange
    SIZE = 100
    items = {f'a{i}': i for i in range(SIZE)}
    metadata.write(items)
    # Act
    metadata = Metadata(DB_PATH, 'db')
    # Assert
    assert len(metadata) == SIZE
    assert metadata.keys() == sorted(items.keys())
    assert metadata.read() == items


def test_keys(metadata):
    # Arrange
    items = {f'a{i}': i for i in range(20)}
    metadata.write(items)
    # Act and Assert
    assert metadata.keys() == sorted(items.keys())


def test_getitem(metadata):
    # Arrange
    KEY = 'a3'
    items = {f'a{i}': i for i in range(5)}
    metadata.write(items)
    # Act and Assert
    assert metadata[KEY] == items[KEY]


def test_setitem(metadata):
    # Arrange
    KEY = 'a3'
    items = {f'a{i}': i for i in range(5)}
    metadata.write(items)
    # Act
    metadata[KEY] = 1000
    # Assert
    assert metadata[KEY] == 1000


def test_delitem(metadata):
    # Arrange
    KEY = 'a3'
    items = {f'a{i}': i for i in range(5)}
    metadata.write(items)
    # Act
    del metadata[KEY]
    # Assert
    assert KEY not in metadata.read()


def test_compact(metadata):
    SIZE = 10
    items = {f'a{i}': i for i in range(SIZE)}
    new_items = {f'a{i}': None for i in range(SIZE + 1)}

    metadata.write(items)
    # Act
    size_before_compact = len(metadata)
    metadata.write(new_items)
    metadata.write(new_items)
    size_after_compact = len(metadata)
    # Assert
    assert size_before_compact == SIZE
    assert size_after_compact == SIZE + 1
    assert metadata.read() == new_items
