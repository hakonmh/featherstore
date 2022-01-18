import tarfile
import os
import pickle
from datetime import datetime
from featherstore.connection import current_db

METADATA_FILE_NAME = 'metadata.pkl'


def restore_table(store, source):
    """Restores a table in to the currently selected db

    Parameters
    ----------
    store : str
        The name of the store to restore the table into.
    source : str
        Path to the snapshot file.
    """
    _can_restore_table(store, source)
    db_path = current_db()
    store_path = os.path.join(db_path, store)
    _extract_snapshot(store_path, source)


def restore_store(source):
    """Restores a store in to the currently selected db.

    Parameters
    ----------
    source : str
        Path to the snapshot file.
    """
    _can_restore_store(source)
    db_path = current_db()
    _extract_snapshot(db_path, source)


def _extract_snapshot(output_path, source):
    if '.tar.xz' not in source:
        source = f'{source}.tar.xz'
    with tarfile.open(source, "r") as tar:
        members = tar.getnames()
        members.remove(METADATA_FILE_NAME)
        for member in members:
            tar.extract(member, output_path)


def _can_restore_table(store, source):
    current_db()
    __raise_if_store_is_not_str(store)
    __raise_if_store_doesnt_exist(store)
    __raise_if_source_path_is_not_str(source)
    __raise_if_snapshot_not_found(source)
    __raise_if_not_snapshot_of_table(source)


def _can_restore_store(source):
    current_db()
    __raise_if_source_path_is_not_str(source)
    __raise_if_snapshot_not_found(source)
    __raise_if_not_snapshot_of_store(source)


def __raise_if_store_is_not_str(store):
    if not isinstance(store, str):
        raise TypeError(f"'store' must be of type str (is type {type(store)})")


def __raise_if_store_doesnt_exist(store):
    db_path = current_db()
    store_path = os.path.join(db_path, store)
    if not os.path.exists(store_path):
        raise FileNotFoundError(f"'store' not found")


def __raise_if_source_path_is_not_str(source):
    if not isinstance(source, str):
        raise TypeError(f"'source' must be of type str (is type {type(source)})")


def __raise_if_snapshot_not_found(source):
    if '.tar.xz' not in source:
        source = f'{source}.tar.xz'
    if not os.path.exists(source):
        raise FileNotFoundError("Snapshot not found")


def __raise_if_not_snapshot_of_table(source):
    if '.tar.xz' not in source:
        source = f'{source}.tar.xz'

    with tarfile.open(source, "r") as tar:
        metadata = tar.extractfile(METADATA_FILE_NAME).read()
        metadata = pickle.loads(metadata)
    if metadata['type'] != 'table':
        raise ValueError("File is not a snapshot of a table")


def __raise_if_not_snapshot_of_store(source):
    if '.tar.xz' not in source:
        source = f'{source}.tar.xz'

    with tarfile.open(source, "r") as tar:
        metadata = tar.extractfile(METADATA_FILE_NAME).read()
        metadata = pickle.loads(metadata)
    if metadata['type'] != 'store':
        raise ValueError("File is not a snapshot of a store")


# ----------------- create snapshot ------------------


def _create_snapshot(output_path, input_path, input_type):
    dir_path = os.path.split(output_path)[0]
    metadata_path = os.path.join(dir_path, METADATA_FILE_NAME)

    __create_snapshot_metadata(metadata_path, input_type)
    __write_snapshot(output_path, input_path, metadata_path)
    __delete_snapshot_metadata_file(metadata_path)


def __create_snapshot_metadata(metadata_path, source_type):
    snapshot_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    metadata = {'type': source_type,
                'snapshot_time': snapshot_time}
    with open(metadata_path, 'wb') as f:
        f.write(pickle.dumps(metadata))


def __write_snapshot(output_path, input_path, metadata_path):
    if 'tar.xz' not in output_path:
        output_path = f'{output_path}.tar.xz'

    with tarfile.open(output_path, "w:xz") as tar:
        tar.add(input_path, arcname=os.path.basename(input_path))
        tar.add(metadata_path, arcname=os.path.basename(metadata_path))


def __delete_snapshot_metadata_file(metadata_path):
    os.remove(metadata_path)
