import os
import json
import pickle

from lsm import LSM
import pyarrow as pa

from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

METADATA_FOLDER_NAME = ".metadata"


class Metadata:
    def __init__(self, path, file_name=None):
        _can_init_metadata(path, file_name)
        self._metadata_folder = os.path.join(path, METADATA_FOLDER_NAME)
        self.db_path = os.path.join(self._metadata_folder, file_name)
        self._connect()

    def create(self):
        if not os.path.exists(self._metadata_folder):
            os.makedirs(self._metadata_folder)
            _utils.mark_as_hidden(self._metadata_folder)
        self.db = LSM(self.db_path, binary=True, use_log=False)
        self.db.open()
        metadata_exists = os.path.exists(self.db_path)
        if not metadata_exists:
            pass

    def _connect(self):
        metadata_exists = os.path.exists(self.db_path)
        if not hasattr(self, 'db') and metadata_exists:
            self.db = LSM(self.db_path, binary=True)
            self.db.open()

    def read(self):
        self._connect()
        values = {key.decode(): pickle.loads(value) for key, value in self.db.items()}
        return values

    def keys(self):
        self._connect()
        keys = [key.decode() for key in self.db.keys()]
        return keys

    def write(self, new_data: dict):
        _can_write_metadata(new_data)
        self._connect()
        new_data = {key.encode(): pickle.dumps(value) for key, value in new_data.items()}
        self.db.update(new_data)
        self.db.flush()

    def __getitem__(self, key: str):
        self._connect()
        key = key.encode()
        value = self.db[key]
        value = pickle.loads(value)
        return value

    def __setitem__(self, key: str, value):
        self._connect()
        key = key.encode()
        value = pickle.dumps(value)
        self.db[key] = value

    def __delitem__(self, key: str):
        self._connect()
        key = key.encode()
        del self.db[key]

    def __repr__(self):
        return self.db_path


def get_partition_attr(table_path, item=None):
    partition_reader = Metadata(table_path, 'partition')
    partitions_names = partition_reader.keys()
    metadata = []
    for name in partitions_names:
        data = partition_reader[name][item]
        metadata.append(data)
    return metadata


def make_table_metadata(df, collected_data):
    partition_names = list(df.keys())
    df = tuple(df.values())
    partition_byte_size, partition_size_in_rows = collected_data
    index_name = _get_index_name(df)

    metadata = {
        "num_rows": _get_num_rows(df),
        "num_columns": _get_num_cols(df),
        "num_partitions": len(df),
        "rows_per_partition": partition_size_in_rows,
        "partition_byte_size": int(partition_byte_size),
        "index_name": index_name,
        "index_column_position": _get_index_position(df, index_name),
        "index_dtype": _get_index_dtype(df),
        "has_default_index": _has_default_index(df, index_name),
        "columns": _get_column_names(df),
    }
    return metadata


def make_partition_metadata(df):
    metadata = {}
    index_col_name = _get_index_name(df)
    for name, partition in df.items():
        data = {
            'min': _get_index_min(partition, index_col_name),
            'max': _get_index_max(partition, index_col_name),
            'num_rows': partition.num_rows
        }
        metadata[name] = data
    return metadata


def update_table_metadata(table_metadata,
                          new_partition_metadata,
                          old_partition_metadata):
    old_num_rows = [
        item['num_rows'] for item in old_partition_metadata.values()
    ]
    new_num_rows = [
        item['num_rows'] for item in new_partition_metadata.values()
    ]

    table_metadata = {
        "num_partitions": table_metadata['num_partitions'] - len(old_partition_metadata) + len(new_partition_metadata),
        "num_rows": table_metadata['num_rows'] - sum(old_num_rows) + sum(new_num_rows)
    }
    return table_metadata


def update_table_metadata2(df, partition_metadata, old_partition_names,
                           table_path):
    old_partition_metadata = _fetch_old_partition_metadata(
        table_path, old_partition_names)
    partition_names = _reorder_partition_names(df, old_partition_names)

    old_num_rows = [
        int(item['num_rows']) for item in old_partition_metadata.values()
    ]
    new_num_rows = [
        int(item['num_rows']) for item in partition_metadata.values()
    ]
    num_rows = old_num_rows + new_num_rows

    table_metadata = {
        "num_partitions": len(partition_names),
        "num_rows": sum(num_rows)
    }
    return table_metadata


def _can_init_metadata(base_path, db_name):
    if not isinstance(base_path, str):
        raise TypeError("Metadata 'base_path' must be of type 'str'")

    if not isinstance(db_name, (str, type(None))):
        raise TypeError("Metadata 'db_name' must be of type 'str' or 'None'")


def _can_write_metadata(data):
    if not isinstance(data, dict):
        raise TypeError("Metadata must be of type 'dict'")


def _has_default_index(df, index_name):
    has_index_name = index_name != DEFAULT_ARROW_INDEX_NAME

    if has_index_name or _index_was_sorted(df):
        has_default_index = False
    else:
        index = pa.Table.from_batches(df)[index_name]
        rangeindex = pa.compute.sort_indices(index)
        IS_NOT_THE_SAME_TYPE = pa.lib.ArrowNotImplementedError
        try:
            is_rangeindex = all(pa.compute.equal(index, rangeindex))
        except IS_NOT_THE_SAME_TYPE:
            is_rangeindex = False

        if is_rangeindex:
            has_default_index = True
        else:
            has_default_index = False

    return has_default_index


def _index_was_sorted(df):
    featherstore_metadata = df[0].schema.metadata[b"featherstore"]
    metadata_dict = json.loads(featherstore_metadata)
    was_sorted = metadata_dict["sorted"]
    return was_sorted


def _get_num_rows(df):
    num_rows = 0
    for partition in df:
        num_rows += partition.num_rows
    return num_rows


def _get_num_cols(df):
    num_cols = df[0].num_columns
    return num_cols


def _get_index_name(df):
    if isinstance(df, dict):
        partition = tuple(df.values())[0]
    else:
        partition = df[0]
    schema = partition.schema
    index_name, = schema.pandas_metadata["index_columns"]
    no_index_name = not isinstance(index_name, str)
    if no_index_name:
        index_name = "index"
    return index_name


def _get_index_position(df, index_name):
    schema = df[0].schema
    index_position = schema.get_field_index(index_name)
    return index_position


def _get_index_dtype(df):
    schema = df[0].schema
    # A better solution for when format_table is refactored:
    # str(df[0].field(index_position).type)
    index_dtype = schema.pandas_metadata["columns"][-1]["pandas_type"]
    if index_dtype == "datetime":
        index_dtype = "datetime64"
    return index_dtype


def _get_column_names(df):
    schema = df[0].schema
    cols = schema.names
    return cols


def _get_index_min(df, index_name):
    first_index_value = df[index_name][0].as_py()
    return first_index_value


def _get_index_max(df, index_name):
    last_index_value = df[index_name][-1].as_py()
    return last_index_value


def _fetch_old_partition_metadata(table_path, partition_names):
    partition_data = Metadata(table_path, 'partition').read()
    partition_data = {key: partition_data[key] for key in partition_names}
    return partition_data


def _reorder_partition_names(df, partition_names):
    # TODO: Make logic for inserts and deletes
    new_partition_names = list(df.keys())
    partition_names = partition_names + new_partition_names
    return partition_names
