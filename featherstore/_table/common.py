import os
import json
from numbers import Integral

import pyarrow as pa
import pandas as pd
import polars as pl

from featherstore._metadata import Metadata
from featherstore._utils import DEFAULT_ARROW_INDEX_NAME, like_pattern_matching
from featherstore._table import _cando

PARTITION_NAME_LENGTH = 14
INSERTION_BUFFER_LENGTH = 10**6


def can_init_table(table_name, store_name):
    _cando.check_if_connected()
    _cando.check_if_store_name_is_str(store_name)
    _cando.check_if_table_name_is_str(table_name)
    _cando.check_if_table_name_is_forbidden(table_name)
    _cando.check_if_store_exists(store_name)


def can_rename_table(new_table_name, table_path, new_table_path):
    _cando.check_if_table_name_is_str(new_table_name)
    _cando.check_if_table_name_is_forbidden(new_table_path)
    _cando.check_if_table_exists(table_path)
    _cando.check_if_table_already_exists(new_table_path)


def combine_partitions(partitions):
    full_table = pa.concat_tables(partitions)
    return full_table


def format_cols(cols, table_columns):
    if cols:
        keyword = str(cols[0]).lower()
        if keyword == "like":
            like = cols[1]
            cols = like_pattern_matching(like, table_columns)
    return cols


def format_rows(rows, index_type):
    if rows is not None:
        keyword = str(rows[0]).lower()
        if keyword in {"between", "before", "after"}:
            rows[1:] = [_convert_row(item, to=index_type) for item in rows[1:]]
        else:
            rows = [_convert_row(item, to=index_type) for item in rows]
    return rows


def _convert_row(row, *, to):
    if to == "datetime64":
        try:
            row = pd.to_datetime(row)
        except Exception:
            raise TypeError("'row' dtype doesn't match index dtype")
    elif to == "string" or to == "unicode":
        if not isinstance(row, str):
            raise TypeError("'row' dtype doesn't match index dtype")
        row = str(row)
    elif to == "int64":
        if not isinstance(row, Integral):
            raise TypeError("'row' dtype doesn't match index dtype")
        row = int(row)
    return row


def format_table(df, index, warnings):
    df = _convert_to_pandas(df)
    df = _set_index(df, index)
    _check_index_constraints(df.index)

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


def assign_ids_to_partitions(df, ids):
    if len(df) != len(ids):
        raise IndexError("Num partitions doesn't match num partiton names")
    id_mapping = {}
    for identifier, partition in zip(ids, df):
        id_mapping[identifier] = partition
    return id_mapping


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


def _convert_to_pyarrow_table(df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    return df


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


# ---------------------------------------------------------


def _get_cols(df, has_default_index):
    if isinstance(df, pd.DataFrame):
        cols = df.columns.tolist()
        if df.index.name is not None:
            cols.append(df.index.name)
        else:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pd.Series):
        cols = [df.name]
        if df.index.name is not None:
            cols.append(df.index.name)
        else:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pa.Table):
        cols = df.column_names
        if has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    elif isinstance(df, pl.DataFrame):
        cols = df.columns
        if has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
            cols.append(DEFAULT_ARROW_INDEX_NAME)
    return cols


def _check_index_constraints(index):
    if index.has_duplicates:
        raise IndexError("Values in Table.index must be unique")
    index_type = index.inferred_type
    if index_type not in {"integer", "datetime64", "string"}:
        raise TypeError("Table.index type must be either int, str or datetime")


def _check_column_constraints(cols):
    cols = pd.Index(cols)
    if cols.has_duplicates:
        raise IndexError("Column names must be unique")
    if "like" in cols.str.lower():
        raise IndexError("df contains invalid column name 'like'")


def _rows_dtype_matches_index(rows, index_dtype):
    try:
        _convert_row(rows[-1], to=index_dtype)
        row_type_matches = True
    except TypeError:
        row_type_matches = False
    return row_type_matches


def _coerce_column_dtypes(df, *, to):
    cols = df.columns
    dtypes = to[cols].dtypes
    try:
        df = df.astype(dtypes)
    except ValueError:
        raise TypeError("New and old column dtypes do not match")
    return df


def _convert_to_partition_id(partition_id):
    partition_id = int(partition_id * INSERTION_BUFFER_LENGTH)
    format_string = f'0{PARTITION_NAME_LENGTH}d'
    partition_id = format(partition_id, format_string)
    return partition_id


def _convert_partition_id_to_int(partition_id):
    return int(partition_id) // INSERTION_BUFFER_LENGTH


def _get_index_dtype(df):
    schema = df[0].schema
    # A better solution for when format_table is refactored:
    # str(df[0].field(index_position).type)
    index_dtype = schema.pandas_metadata["columns"][-1]["pandas_type"]
    if index_dtype == "datetime":
        index_dtype = "datetime64"
    return index_dtype
