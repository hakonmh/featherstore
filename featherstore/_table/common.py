import json
import warnings as _warnings

import pandas as pd
import pyarrow as pa
from pyarrow import pandas_compat as pc

from featherstore._table import _table_utils
from featherstore._table._indexers import ColIndexer, RowIndexer

HAS_MULTI_TYPE_COLUMN = (pa.lib.ArrowTypeError, pa.lib.ArrowInvalid)


def format_cols_arg(cols, *, like=None):
    cols = ColIndexer(cols)
    if like:
        cols = cols.like(like)
    return cols


def format_rows_arg(rows, *, to_dtype=None):
    rows = RowIndexer(rows)
    if to_dtype:
        rows = rows.convert_types(to=to_dtype)
    return rows


def format_cols_and_to_args(cols, to):
    if isinstance(cols, dict):
        to = cols.values()
        cols = cols.keys()

    formatted_cols = ColIndexer(to)
    formatted_cols.set_keys(cols)
    return formatted_cols


def format_table(df, index_name, warnings):
    try:
        df = _table_utils.convert_to_arrow(df)
        new_metadata = json.dumps({"transposed": False})
        df = _add_featherstore_metadata(df, new_metadata)
    except HAS_MULTI_TYPE_COLUMN:
        df = _transpose_table_and_convert_to_arrow(df)

    if index_name is None:
        index_name = _table_utils.get_index_name(df)
    if index_name not in df.column_names:
        df = _make_default_index(df, index_name)

    df = _sort_table_if_unsorted(df, index_name, warnings)
    df = _format_pd_metadata(df, index_name)
    return df


def _transpose_table_and_convert_to_arrow(df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    df = df.T
    df = df.infer_objects()
    try:
        df = _table_utils.convert_to_arrow(df)
    except HAS_MULTI_TYPE_COLUMN:
        raise ValueError("Cannot convert table with multiple dtypes in same column to arrow.")
    new_metadata = json.dumps({"transposed": True})
    df = _add_featherstore_metadata(df, new_metadata)
    return df


def _make_default_index(df, index_name):
    index = pa.array(pd.RangeIndex(len(df)))
    df = df.add_column(0, index_name, index)
    return df


def _sort_table_if_unsorted(df, index_name, warnings):
    was_unsorted = not _is_sorted(df, index_name)
    if was_unsorted:
        if warnings == "warn":
            _warnings.warn("Index is unsorted and will be sorted before storage")
        df = _table_utils.sort_arrow_table(df, by=index_name)
    new_metadata = json.dumps({"sorted": was_unsorted})
    df = _add_featherstore_metadata(df, new_metadata)
    return df


def _add_featherstore_metadata(df, new_metadata):
    old_metadata = df.schema.metadata
    if old_metadata:
        if b"featherstore" in old_metadata:
            fs_data = json.loads(old_metadata[b"featherstore"])
            fs_data.update(json.loads(new_metadata))
            new_metadata = json.dumps(fs_data)

        combined_metadata = {**old_metadata, b"featherstore": new_metadata}
    else:
        combined_metadata = {b"featherstore": new_metadata}
    df = df.replace_schema_metadata(combined_metadata)
    return df


def _is_sorted(df, index_name=None):
    if index_name:
        index = df[index_name]
    else:
        index = df

    is_transposed = _table_utils.is_transposed(df)
    is_unordered = pa.compute.any(pa.compute.greater(index[:-1], index[1:])).as_py()
    return not is_unordered or is_transposed


def _format_pd_metadata(df, index_name):
    metadata = _make_pd_metadata(df, index_name)
    df = _add_pd_metadata(df, metadata)
    df = _make_index_first_column(df)
    return df


def _make_pd_metadata(df, index_name):
    metadata = dict()

    metadata['index_columns'] = [index_name]
    metadata['column_indexes'] = [{'name': None, 'field_name': None,
                                   'pandas_type': 'unicode',
                                   'numpy_type': 'object',
                                   'metadata': {'encoding': 'UTF-8'}}
                                  ]
    metadata['columns'] = [__make_column_metadata(df, col) for col in df.column_names]
    metadata = json.dumps(metadata)
    metadata = {b'pandas': metadata}
    return metadata


def __make_column_metadata(df, col):
    dtype = df[col].type
    pd_dtype = pc.get_logical_type(dtype)
    np_dtype, extra_data = __get_numpy_dtype_info(dtype)

    metadata = {"name": col,
                "field_name": col,
                "pandas_type": pd_dtype,
                "numpy_type": np_dtype,
                "metadata": extra_data,
                }
    return metadata


def __get_numpy_dtype_info(dtype):
    pd_dtype = pc.get_logical_type(dtype)

    if pd_dtype == 'decimal':
        numpy_dtype = 'object'
        extra_metadata = {
            'precision': dtype.precision,
            'scale': dtype.scale,
        }
    elif pd_dtype in ('date', 'time'):  # Numpy doesn't support date types
        resolution = str(dtype).split('[')[-1].split(']')[0]
        if resolution == 'day':
            resolution = 'D'
        numpy_dtype = f'datetime64[{resolution}]'
        extra_metadata = None
    elif hasattr(dtype, 'tz'):  # Store timezone info if exists for dtime types
        resolution = str(dtype).split('[')[-1].split(']')[0]
        numpy_dtype = f'datetime64[{resolution}]'
        try:
            extra_metadata = {'timezone': pa.lib.tzinfo_to_string(pd_dtype.tz)}
        except Exception:
            extra_metadata = {'timezone': None}
    elif pd_dtype[:4] == 'list':
        numpy_dtype = 'object'
        extra_metadata = None
    elif pd_dtype == 'categorical':
        numpy_dtype = str(dtype.index_type)
        extra_metadata = {'ordered': dtype.ordered}
    else:
        numpy_dtype = str(dtype)
        extra_metadata = None
        if numpy_dtype in ('large_string', 'binary', 'large_binary'):
            numpy_dtype = 'string'
    return numpy_dtype, extra_metadata


def _add_pd_metadata(df, metadata):
    old_metadata = df.schema.metadata
    if old_metadata:
        old_metadata[b'pandas'] = metadata[b'pandas']
    else:
        old_metadata = {b'pandas': metadata[b'pandas']}
    df = df.replace_schema_metadata(old_metadata)
    return df


def _make_index_first_column(df):
    index_name = df.schema.pandas_metadata["index_columns"][0]
    cols = df.column_names
    cols.remove(index_name)
    cols.insert(0, index_name)
    df = df.select(cols)
    return df


def compute_rows_per_partition(df, target_size):
    num_rows = df.shape[0]
    table_size_in_bytes = max(df.nbytes, 1)
    if target_size == -1:
        rows_per_partition = -1
    else:
        rows_per_partition = num_rows * target_size / table_size_in_bytes
        rows_per_partition = int(round(rows_per_partition, 0))
        rows_per_partition = max(rows_per_partition, 1)
    return rows_per_partition


def update_metadata(table, df, old_partition_names, **kwargs):
    new_partition_metadata = _make_partition_metadata(df)
    table_metadata = _update_table_metadata(table, new_partition_metadata,
                                            old_partition_names)
    for key, value in kwargs.items():
        table_metadata[key] = value
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
    try:
        first_index_value = df[index_name][0].as_py()
    except IndexError:
        first_index_value = None
    return first_index_value


def _get_index_max(df, index_name):
    try:
        last_index_value = df[index_name][-1].as_py()
    except IndexError:
        last_index_value = None
    return last_index_value


def _update_table_metadata(table, new_partitions_data,
                           dropped_partitions):
    # TODO: Clean up, new name, generalize(?)
    dropped_partitions_data = _get_dropped_partitions_data(table._partition_data,
                                                           dropped_partitions)
    num_rows = table._table_data['num_rows']
    num_rows += _update_num_rows(dropped_partitions_data,
                                 new_partitions_data)
    num_partitions = table._table_data['num_partitions']
    num_partitions += _update_num_partitions(dropped_partitions_data,
                                             new_partitions_data)

    table_metadata = {
        "num_partitions": num_partitions,
        "num_rows": num_rows
    }
    return table_metadata


def _get_dropped_partitions_data(partition_data, partition_names):
    metadata = {name: partition_data[name] for name in partition_names}
    return metadata


def _update_num_rows(dropped_partitions_data, new_partitions_data):
    dropped = (item['num_rows'] for item in dropped_partitions_data.values())
    added = (item['num_rows'] for item in new_partitions_data.values())
    return sum(added) - sum(dropped)


def _update_num_partitions(dropped_partitions_data, new_partitions_data):
    dropped = len(dropped_partitions_data)
    added = len(new_partitions_data)
    return added - dropped


def index_is_default(index):
    if not pa.types.is_integer(index.type):
        return False
    if len(index) == 0:
        return True

    default_rangeindex = pa.array(range(len(index)))
    try:
        return index.equals(default_rangeindex)
    except TypeError:  # index is chunked array
        default_rangeindex = pa.chunked_array([default_rangeindex])
        return index.equals(default_rangeindex)
