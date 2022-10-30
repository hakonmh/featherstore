import tarfile
import os
import pickle
from datetime import datetime
from featherstore.connection import Connection, current_db

METADATA_FILE_NAME = 'metadata.pkl'


def restore_table(store_name, source, errors='raise'):
    """Restores a table in to the currently selected db.

    Parameters
    ----------
    store_name : str
        The name of the store to restore the table into.
    source : str
        Path to the snapshot file.
    errors : str
        Whether or not to raise an error if a table with the same name already
        exist. Can be either `raise` or `ignore`, `ignore` overwrites existing
        table, by default `raise`.

    Returns
    -------
    str
        The name of the table restored.
    """
    _can_restore_table(store_name, source, errors)
    store_path = os.path.join(current_db(), store_name)
    table_name = _extract_snapshot(store_path, source)
    return table_name


def restore_store(source, errors='raise'):
    """Restores a store in to the currently selected db.

    Parameters
    ----------
    source : str
        Path to the snapshot file.
    errors : str
        Whether or not to raise an error if a store with the same name already
        exist. Can be either `raise` or `ignore`, `ignore` overwrites existing
        store, by default `raise`.

    Returns
    -------
    str
        The name of the store restored.
    """
    _can_restore_store(source, errors)
    store_name = _extract_snapshot(current_db(), source)
    return store_name


def _extract_snapshot(output_path, source):
    if '.tar.xz' not in source:
        source = f'{source}.tar.xz'
    with tarfile.open(source, "r") as tar:
        members = tar.getnames()
        members.remove(METADATA_FILE_NAME)
        name = members[0]
        for member in members:
            tar.extract(member, output_path)
    return name


def _can_restore_table(store, source, errors):
    Connection._raise_if_not_connected()
    __raise_if_store_is_not_str(store)
    __raise_if_store_doesnt_exist(store)
    __raise_if_source_path_is_not_str(source)
    __raise_if_snapshot_not_found(source)
    __raise_if_not_snapshot_of_table(source)
    __raise_if_table_already_exists(store, source, errors)


def _can_restore_store(source, errors):
    Connection._raise_if_not_connected()
    __raise_if_source_path_is_not_str(source)
    __raise_if_snapshot_not_found(source)
    __raise_if_not_snapshot_of_store(source)
    __raise_if_store_already_exists(source, errors)


def __raise_if_store_is_not_str(store):
    if not isinstance(store, str):
        raise TypeError(f"'store' must be of type str (is type {type(store)})")


def __raise_if_store_doesnt_exist(store):
    store_path = os.path.join(current_db(), store)
    if not os.path.exists(store_path):
        raise FileNotFoundError("'store' not found")


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


def __raise_if_table_already_exists(store, source, errors):
    if errors == 'raise':
        store_path = os.path.join(current_db(), store)

        if '.tar.xz' not in source:
            source = f'{source}.tar.xz'
        table_name = __get_name(source)

        if table_name in os.listdir(store_path):
            raise FileExistsError(f"A table with name {table_name} already exists")


def __raise_if_store_already_exists(source, errors):
    if '.tar.xz' not in source:
        source = f'{source}.tar.xz'
    store_name = __get_name(source)

    if store_name in os.listdir(current_db()) and errors == 'raise':
        raise FileExistsError(f"A store with name {store_name} already exists")


def __raise_if_not_snapshot_of_store(source):
    if '.tar.xz' not in source:
        source = f'{source}.tar.xz'

    with tarfile.open(source, "r") as tar:
        metadata = tar.extractfile(METADATA_FILE_NAME).read()
        metadata = pickle.loads(metadata)
    if metadata['type'] != 'store':
        raise ValueError("File is not a snapshot of a store")


def __get_name(source):
    with tarfile.open(source, "r") as tar:
        name = tar.getnames()[0]
    return name

# ----------------- create snapshot ------------------


def _create_snapshot(output_path, input_path, input_type):
    _can_create_snapshot(output_path, input_path, input_type)
    dir_path = os.path.split(output_path)[0]
    metadata_path = os.path.join(dir_path, METADATA_FILE_NAME)

    __create_snapshot_metadata(metadata_path, input_type)
    __write_snapshot(output_path, input_path, metadata_path)
    __delete_snapshot_metadata_file(metadata_path)


def _can_create_snapshot(output_path, input_path, input_type):
    Connection._raise_if_not_connected()
    __raise_if_source_path_is_not_str(output_path)
    __raise_if_input_doesnt_exists(input_path)
    __raise_if_input_type_is_not_valid(input_type)


def __raise_if_input_doesnt_exists(input_path):
    if not os.path.exists(input_path):
        raise FileNotFoundError("Snapshot target not found")


def __raise_if_input_type_is_not_valid(input_type):
    if input_type not in ('table', 'store'):
        raise ValueError("Input type should be either 'table' or 'store")


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
