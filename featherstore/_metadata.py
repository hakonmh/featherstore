import os
import json

import msgpack
import pyarrow as pa

from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

METADATA_FOLDER_NAME = ".metadata"


class Metadata:
    def __init__(self, base_path, db_name=None):
        _can_init_metadata(base_path, db_name)
        metadata_folder = os.path.join(base_path, METADATA_FOLDER_NAME)
        if db_name:
            self.db = os.path.join(metadata_folder, db_name)
        else:
            self.db = metadata_folder

    def create(self):
        if not os.path.exists(self.db):
            os.makedirs(self.db)
            _utils.mark_as_hidden(self.db)

    def read(self):
        return {key: self[key] for key in os.listdir(self.db)}

    def write(self, new_data: dict):
        _can_write_metadata(new_data)
        for key, value in new_data.items():
            self[key] = value

    def __getitem__(self, name: str):
        path = os.path.join(self.db, name)
        with open(path, "rb") as f:
            value = msgpack.loads(f.read())
        return value

    def __setitem__(self, name: str, value):
        path = os.path.join(self.db, name)
        with open(path, "wb") as f:
            f.write(msgpack.dumps(value))

    def __delitem__(self, name: str):
        path = os.path.join(self.db, name)
        os.remove(path)

    def __repr__(self):
        return str(self.read())


def get_partition_attr(table_path, item=None):
    partitions_ordering = Metadata(table_path, 'table')['partitions']
    partition_reader = Metadata(table_path, 'partition')
    partition_data = partition_reader.read()

    metadata = []
    for key in partitions_ordering:
        metadata.append(partition_data[key][item])
    return metadata


def make_table_metadata(df, collected_data):
    partition_names = list(df.keys())
    df = tuple(df.values())
    partition_byte_size, partition_size_in_rows = collected_data

    metadata = dict()
    metadata["num_rows"] = _get_num_rows(df)
    metadata["num_columns"] = _get_num_cols(df)
    metadata["num_partitions"] = len(df)
    metadata["rows_per_partition"] = partition_size_in_rows
    metadata["partition_byte_size"] = int(partition_byte_size)
    metadata["index_name"] = index_name = _get_index_name(df)
    metadata["index_column_position"] = _get_index_position(df, index_name)
    metadata["index_dtype"] = _get_index_dtype(df)
    metadata["has_default_index"] = _has_default_index(df, index_name)
    metadata["columns"] = _get_column_names(df)
    metadata["partitions"] = partition_names
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


def update_table_metadata(df, partition_metadata, old_partition_names, table_path):
    old_partition_metadata = _fetch_old_partition_metadata(
        table_path, old_partition_names
    )
    partition_names = _reorder_partition_names(df, old_partition_names)

    old_num_rows = [int(item['num_rows']) for item in old_partition_metadata.values()]
    new_num_rows = [int(item['num_rows']) for item in partition_metadata.values()]
    num_rows = old_num_rows + new_num_rows

    table_metadata = {
        "partitions": partition_names,
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
    (index_name,) = schema.pandas_metadata["index_columns"]
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
    first_index_value = str(df[index_name][0])
    return first_index_value


def _get_index_max(df, index_name):
    last_index_value = str(df[index_name][-1])
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
