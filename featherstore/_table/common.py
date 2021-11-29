import os
import json

import pyarrow as pa
import pandas as pd
import polars as pl

from featherstore.connection import Connection
from featherstore._metadata import Metadata
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME, filter_items_like_pattern
from featherstore._table import _table_utils
from featherstore._table import _raise_if
from featherstore import store

PARTITION_NAME_LENGTH = 14
INSERTION_BUFFER_LENGTH = 10**6


def can_init_table(table_name, store_name):
    Connection.is_connected()
    store._raise_if_store_name_is_str(store_name)
    store._raise_if_store_not_exists(store_name)

    _raise_if.table_name_is_not_str(table_name)
    _raise_if.table_name_is_forbidden(table_name)


def can_rename_table(new_table_name, table_path, new_table_path):
    Connection.is_connected()

    _raise_if.table_name_is_not_str(new_table_name)
    _raise_if.table_name_is_forbidden(new_table_path)
    _raise_if.table_already_exists(new_table_path)


def combine_partitions(partitions):
    full_table = pa.concat_tables(partitions)
    return full_table


def filter_table_cols(cols, table_columns):
    if cols:
        keyword = str(cols[0]).lower()
        if keyword == "like":
            pattern = cols[1]
            cols = filter_items_like_pattern(table_columns, like=pattern)
    return cols


def format_rows(rows, index_type):
    if rows is not None:
        keyword = str(rows[0]).lower()
        if keyword in {"between", "before", "after"}:
            rows[1:] = [_table_utils._convert_row(item, to=index_type) for item in rows[1:]]
        else:
            rows = [_table_utils._convert_row(item, to=index_type) for item in rows]
    return rows


def format_table(df, index, warnings):
    df = _convert_to_pandas(df)
    df = _set_index(df, index)
    _raise_if.index_values_contains_duplicates(df.index)
    _raise_if.index_is_not_supported_dtype(df.index)

    index_is_sorted = df.index.is_monotonic_increasing
    if not index_is_sorted:
        df = _sort_index(df, warnings)
        new_metadata = json.dumps({"sorted": True})
    else:
        new_metadata = json.dumps({"sorted": False})

    formatted_df = pa.Table.from_pandas(df, preserve_index=True)
    formatted_df = _make_index_first_column(formatted_df)
    formatted_df = _add_schema_metadata(formatted_df, new_metadata)

    return formatted_df


def _convert_to_pandas(df):
    if isinstance(df, pd.DataFrame):
        pd_df = df
    elif isinstance(df, pd.Series):
        pd_df = df.to_frame()
    elif isinstance(df, pa.Table):
        pd_df = df.to_pandas()
    elif isinstance(df, pl.DataFrame):
        pd_df = df.to_pandas()
        if DEFAULT_ARROW_INDEX_NAME in pd_df.columns:
            pd_df = pd_df.set_index(DEFAULT_ARROW_INDEX_NAME)
            pd_df.index.name = None
    return pd_df


def _set_index(df, index_name):
    user_has_provided_index = bool(index_name)
    index_name_not_index = index_name != df.index.name
    if user_has_provided_index and index_name_not_index and index_name in df.columns:
        df = df.set_index(index_name)
    if df.index.name == DEFAULT_ARROW_INDEX_NAME:
        df.index.name = None
    return df


def _sort_index(df, warnings):
    if warnings == "warn":
        import warnings
        warnings.warn("Index is unsorted and will be sorted before storage")
    df = df.sort_index()
    return df


def _make_index_first_column(df):
    index_name = df.schema.pandas_metadata["index_columns"]
    column_names = df.column_names[:-1]
    columns = index_name + column_names
    df = df.select(columns)
    return df


def _add_schema_metadata(df, new_metadata):
    old_metadata = df.schema.metadata
    combined_metadata = {**old_metadata, b"featherstore": new_metadata}
    df = df.replace_schema_metadata(combined_metadata)
    return df


def assign_ids_to_partitions(df, ids):
    if len(df) != len(ids):
        raise IndexError("Num partitions doesn't match num partiton names")
    id_mapping = {}
    for identifier, partition in zip(ids, df):
        id_mapping[identifier] = partition
    return id_mapping


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


def _get_index_min(df, index_name):
    first_index_value = df[index_name][0].as_py()
    return first_index_value


def _get_index_max(df, index_name):
    last_index_value = df[index_name][-1].as_py()
    return last_index_value


def update_table_metadata(table_metadata, new_partition_metadata,
                          old_partition_metadata):
    old_num_rows = [
        item['num_rows'] for item in old_partition_metadata.values()
    ]
    new_num_rows = [
        item['num_rows'] for item in new_partition_metadata.values()
    ]

    table_metadata = {
        "num_partitions":
        table_metadata['num_partitions'] - len(old_partition_metadata) + len(new_partition_metadata),
        "num_rows":
        table_metadata['num_rows'] - sum(old_num_rows) + sum(new_num_rows)
    }
    return table_metadata


def delete_partition(table_path, partition_name):
    partition_path = os.path.join(table_path, f'{partition_name}.feather')
    try:
        os.remove(partition_path)
    except PermissionError:
        raise PermissionError('File still opened by memory-map')


def delete_partition_metadata(table_path, partition_name):
    partition_data = Metadata(table_path, 'partition')
    partition_names = partition_data.keys()
    partition_names = partition_names.remove(partition_name)
    del partition_data[partition_name]
