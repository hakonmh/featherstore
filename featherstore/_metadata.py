import os
import shelve
import json

import pyarrow as pa

from . import _utils
from ._utils import DEFAULT_ARROW_INDEX_NAME

METADATA_FOLDER_NAME = ".metadata"


class Metadata:
    def __init__(self, base_path, file_name=None):
        self._folder = f"{base_path}/{METADATA_FOLDER_NAME}"
        self.path = f"{self._folder}/{file_name}.mda"

    def create(self):
        if not os.path.exists(self._folder):
            os.mkdir(self._folder)
            _utils.mark_as_hidden(self._folder)

    def read(self):
        with shelve.open(self.path, "r") as f:
            data = dict(f.items())
        return data

    def write(self, new_data: dict):
        _can_write_metadata(new_data)
        with shelve.open(self.path, "c") as f:
            f.update(new_data)

    def __getitem__(self, name: str):
        with shelve.open(self.path, "r") as f:
            data = f[name]
        return data

    def __setitem__(self, name: str, value):
        with shelve.open(self.path, "w") as f:
            f[name] = value

    def __delitem__(self, name: str):
        with shelve.open(self.path, "w") as f:
            del f[name]

    def __missing__(self, key):
        raise KeyError(f"{key} not in {self.path}")

    def __repr__(self):
        return str(self.read())


def read_metadata(base_path, file_name, item=None):
    _can_read_metadata(base_path, file_name, item)
    data_reader = Metadata(base_path, file_name)
    if item:
        metadata = data_reader[item]
    else:
        metadata = data_reader.read()
    return metadata


def _can_write_metadata(data):
    if not isinstance(data, dict):
        raise TypeError("Metadata must be of type 'dict'")


def _can_read_metadata(base_path, file_name, item):
    if not isinstance(base_path, str):
        raise TypeError

    if not isinstance(file_name, str):
        raise TypeError

    if not isinstance(item, (str, type(None))):
        raise TypeError

    metadata_obj = Metadata(base_path, file_name)
    if not os.path.exists(metadata_obj.path):
        raise FileNotFoundError(f"Metadata file doesn't exist: {metadata_obj.path}")


def append_metadata(df, table_path):
    partition_data = Metadata(table_path, "partition")
    for name, items in make_partition_metadata(df).items():
        partition_attr = partition_data[name]
        del partition_attr[-1]
        partition_attr.extend(items)
        partition_data[name] = partition_attr
    table_data = Metadata(table_path, "table")
    table_data["num_partitions"] = len(partition_data["name"])
    table_data["num_rows"] = sum(partition_data["num_rows"])


def make_table_metadata(df, collected_data):
    df = tuple(df.values())
    partition_byte_size, partition_size_in_rows = collected_data

    metadata = dict()
    metadata["num_rows"] = _get_num_rows(df)
    metadata["num_columns"] = _get_num_cols(df)
    metadata["num_partitions"] = len(df)
    metadata["rows_per_partition"] = partition_size_in_rows
    metadata["partition_byte_size"] = partition_byte_size
    metadata["index_name"] = index_name = _get_index_name(df)
    metadata["index_column_position"] = index_position = _get_index_position(
        df, index_name
    )
    metadata["index_dtype"] = _get_index_dtype(df, index_position)
    metadata["has_default_index"] = _has_default_index(df, index_name)
    metadata["columns"] = _get_column_names(df)
    return metadata


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


def make_partition_metadata(df):
    metadata = {"name": [], "min": [], "max": [], "num_rows": []}
    index_col_name = _get_index_name(df)
    for name, partition in df.items():
        metadata["name"].append(name)
        metadata["min"].append(_get_index_min(partition, index_col_name))
        metadata["max"].append(_get_index_max(partition, index_col_name))
        metadata["num_rows"].append(partition.num_rows)
    return metadata


def _get_index_min(df, index_name):
    first_index_value = str(df[index_name][0])
    return first_index_value


def _get_index_max(df, index_name):
    last_index_value = str(df[index_name][-1])
    return last_index_value


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
    (index_name,) = schema.pandas_metadata["index_columns"]
    no_index_name = not isinstance(index_name, str)
    if no_index_name:
        index_name = "index"
    return index_name


def _get_index_position(df, index_name):
    schema = df[0].schema
    index_position = schema.get_field_index(index_name)
    return index_position


def _get_index_dtype(df, index_position):
    schema = df[0].schema
    index_dtype = schema.pandas_metadata["columns"][index_position]["pandas_type"]
    if index_dtype == "datetime":
        index_dtype = "datetime64"
    return index_dtype


def _get_column_names(df):
    schema = df[0].schema
    cols = schema.names
    return cols
