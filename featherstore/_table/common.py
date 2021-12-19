import os
import json

import pyarrow as pa
import pandas as pd
import polars as pl

from featherstore.connection import Connection
from featherstore._metadata import Metadata
from featherstore import _utils
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME
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


def can_rename_table(new_table_name, new_table_path):
    Connection.is_connected()

    _raise_if.table_name_is_not_str(new_table_name)
    _raise_if.table_name_is_forbidden(new_table_path)
    _raise_if.table_already_exists(new_table_path)


def filter_cols_if_like_provided(cols, table_cols):
    cols_are_provided = bool(cols)
    if cols_are_provided:
        keyword = str(cols[0]).lower()
        like_is_provided = keyword == "like"
        if like_is_provided:
            pattern = cols[1]
            cols = _utils.filter_items_like_pattern(table_cols, like=pattern)
    return cols


def format_rows_arg_if_provided(rows, index_type):
    rows_is_provided = rows is not None
    if rows_is_provided:
        rows = _format_rows_arg(rows, index_type)
    return rows


def _format_rows_arg(rows, index_type):
    rows = list(rows)
    keyword = str(rows[0]).lower()
    if keyword in {"between", "before", "after"}:
        rows[1:] = _coerce_row_dtypes(rows[1:], to=index_type)
    else:
        rows = _coerce_row_dtypes(rows, to=index_type)
    return rows


def _coerce_row_dtypes(rows, *, to):
    rows = [_convert_row(item, to) for item in rows]
    return rows


def _convert_row(row, to):
    if _table_utils.str_is_temporal_dtype(to):
        row = pd.to_datetime(row)
    elif _table_utils.str_is_string_dtype(to):
        row = str(row)
    elif _table_utils.str_is_int_dtype(to):
        row = int(row)
    return row


def format_table(df, index_name, warnings):
    df = _table_utils.convert_to_arrow(df)

    if index_name is None:
        index_name = _table_utils.get_index_name(df)
    if index_name not in df.column_names:
        df = _make_default_index(df, index_name)

    df = _sort_table_if_unsorted(df, index_name, warnings)
    df = _format_pd_metadata(df, index_name)
    return df


def _make_default_index(df, index_name):
    index = pa.array(pd.RangeIndex(len(df)))
    df = df.append_column(index_name, index)
    return df


def _sort_table_if_unsorted(df, index_name, warnings):
    pd_index = pd.Index(df[index_name])
    index_is_unordered = not pd_index.is_monotonic_increasing

    if index_is_unordered:
        df = _sort_arrow_table(df, index_name, warnings)
    new_metadata = json.dumps({"sorted": index_is_unordered})
    df = _add_featherstore_metadata(df, new_metadata)
    return df


def _sort_arrow_table(df, index_name, warnings="ignore"):
    if warnings == "warn":
        warnings.warn("Index is unsorted and will be sorted before storage")
    schema = df.schema

    df = pl.from_arrow(df, rechunk=False)
    df = df.sort(index_name)
    df = df.to_arrow()

    df = df.cast(schema)
    return df


def _format_pd_metadata(df, index_name):
    metadata = _make_pd_schema(df, index_name)
    df = _add_pd_metadata(df, metadata)
    df = _make_index_first_column(df)
    return df


def _make_pd_schema(df, index_name):
    first_row = _table_utils.get_first_row(df)
    first_row = _table_utils.convert_to_pandas(first_row)
    first_row = __set_index(first_row, index_name)

    table_schema = pa.Schema.from_pandas(first_row, preserve_index=True)
    metadata = table_schema.metadata
    return metadata


def __set_index(df, index_name):
    has_provided_index = bool(index_name)
    index_name_is_not_index = index_name != df.index.name
    cols = df.columns
    if has_provided_index and index_name_is_not_index and index_name in cols:
        df = df.set_index(index_name)
    if df.index.name == DEFAULT_ARROW_INDEX_NAME:
        df.index.name = None
    return df


def _add_pd_metadata(df, metadata):
    old_metadata = df.schema.metadata
    old_metadata[b'pandas'] = metadata[b'pandas']
    df = df.replace_schema_metadata(old_metadata)
    return df


def _make_index_first_column(df):
    index_name = df.schema.pandas_metadata["index_columns"][0]
    cols = df.column_names
    cols.remove(index_name)
    cols.insert(0, index_name)
    df = df.select(cols)
    return df


def _add_featherstore_metadata(df, new_metadata):
    old_metadata = df.schema.metadata
    if old_metadata:
        combined_metadata = {**old_metadata, b"featherstore": new_metadata}
    else:
        combined_metadata = {b"featherstore": new_metadata}
    df = df.replace_schema_metadata(combined_metadata)
    return df


def calculate_rows_per_partition(df, target_size):
    num_rows = df.shape[0]
    table_size_in_bytes = df.nbytes
    rows_per_partition = num_rows * target_size / table_size_in_bytes
    rows_per_partition = int(round(rows_per_partition, 0))
    return rows_per_partition


def assign_ids_to_partitions(df, ids):
    if len(df) != len(ids):
        raise IndexError("Num partitions doesn't match num partiton names")
    id_mapping = {}
    for identifier, partition in zip(ids, df):
        id_mapping[identifier] = partition
    return id_mapping


def update_metadata(df, table_path, old_partition_names):
    new_partition_metadata = _make_partition_metadata(df)
    table_metadata = update_table_metadata(table_path, new_partition_metadata,
                                           old_partition_names)
    return table_metadata, new_partition_metadata


def _make_partition_metadata(df):
    metadata = {}

    first_partition = tuple(df.values())[0]
    index_col_name = _table_utils.get_index_name(first_partition)
    for name, partition in df.items():
        data = {
            'min': _get_index_min(partition, index_col_name),
            'max': _get_index_max(partition, index_col_name),
            'num_rows': partition.num_rows
        }
        metadata[name] = data
    return metadata


def _get_index_min(df, index_name):
    first_index_value = df[index_name][0].as_py()
    return first_index_value


def _get_index_max(df, index_name):
    last_index_value = df[index_name][-1].as_py()
    return last_index_value


def update_table_metadata(table_path, new_partitions_data,
                          dropped_partitions):
    # TODO: Clean up, new name, generalize(?)
    dropped_partitions_data = _get_dropped_partitions_data(dropped_partitions,
                                                           table_path)
    num_rows = _update_num_rows(table_path,
                                dropped_partitions_data,
                                new_partitions_data)
    num_partitions = _update_num_partitions(table_path,
                                            dropped_partitions_data,
                                            new_partitions_data)

    table_metadata = {
        "num_partitions": num_partitions,
        "num_rows": num_rows
    }
    return table_metadata


def _get_dropped_partitions_data(partition_names, table_path):
    partition_data = Metadata(table_path, 'partition')
    metadata = {name: partition_data[name] for name in partition_names}
    return metadata


def _update_num_rows(table_path, dropped_partitions_data, new_partitions_data):
    current = Metadata(table_path, 'table')['num_rows']
    dropped = [item['num_rows'] for item in dropped_partitions_data.values()]
    added = [item['num_rows'] for item in new_partitions_data.values()]
    updated_num_rows = current + sum(added) - sum(dropped)
    return updated_num_rows


def _update_num_partitions(table_path, dropped_partitions_data, new_partitions_data):
    current = Metadata(table_path, 'table')['num_partitions']
    dropped = len(dropped_partitions_data)
    added = len(new_partitions_data)
    updated_num_partitions = current + added - dropped
    return updated_num_partitions
