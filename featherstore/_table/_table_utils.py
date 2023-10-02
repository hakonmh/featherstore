from collections.abc import Iterable, Set

import pyarrow as pa
from pyarrow import compute
import pandas as pd
import polars as pl

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

PARTITION_NAME_LENGTH = 14
INSERTION_BUFFER_LENGTH = 10**6


def concat_arrow_tables(*dfs):
    main_df = dfs[0]
    dfs = _sort_cols(dfs, cols=main_df.column_names)
    try:
        dfs = _coerce_arrow_col_types(dfs, schema=main_df.schema)
        full_table = pa.concat_tables(dfs)
    except pa.ArrowInvalid:
        raise TypeError("New and old column types doesn't match")
    return full_table


def _sort_cols(dfs, cols):
    sorted_dfs = []
    for df in dfs:
        cols_not_sorted = df.column_names != cols
        if cols_not_sorted:
            df = df.select(cols)
        sorted_dfs.append(df)
    return sorted_dfs


def _coerce_arrow_col_types(dfs, schema):
    coerced_dfs = []
    for df in dfs:
        df = df.cast(schema)
        coerced_dfs.append(df)
    return coerced_dfs


def is_sorted(df, index_name=None):
    if index_name:
        index = df[index_name]
    else:
        index = df

    is_unordered = compute.any(compute.greater(index[:-1], index[1:]))
    return not is_unordered.as_py()


def sort_arrow_table(df, *, by):
    schema = df.schema
    df = convert_to_polars(df)

    df = df.sort(by)

    df = convert_to_arrow(df)
    df = df.cast(schema)
    return df


def get_col_names(df, has_default_index):
    if isinstance(df, pd.DataFrame):
        cols = df.columns.tolist()
    elif isinstance(df, pl.DataFrame):
        cols = df.columns
    elif isinstance(df, pa.Table):
        cols = df.column_names
    else:
        cols = [df.name]

    if isinstance(df, (pd.DataFrame, pd.Series)):
        index_name = df.index.name
        index_name = index_name if index_name else DEFAULT_ARROW_INDEX_NAME
        cols.append(index_name)
    elif has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
        cols.append(DEFAULT_ARROW_INDEX_NAME)

    return cols


def convert_to_arrow(df, as_array=False):
    if isinstance(df, (pl.Series, pd.Series, pd.Index)):
        if as_array:
            df = pa.array(df)
        else:
            df = df.to_frame()

    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    return df


def convert_to_polars(df, as_array=False):
    if isinstance(df, (pd.Series, pd.DataFrame, pd.Index)):
        df = convert_to_arrow(df, as_array=as_array)
    if isinstance(df, (pa.Table, pa.Array, pa.ChunkedArray)):
        df = pl.from_arrow(df, rechunk=False)
    return df


def convert_to_pandas(df):
    if isinstance(df, pd.DataFrame):
        pd_df = df
    elif isinstance(df, pd.Series):
        pd_df = df.to_frame()
    elif isinstance(df, (pa.Table, pl.DataFrame)):
        pd_df = df.to_pandas()

    if DEFAULT_ARROW_INDEX_NAME in pd_df.columns:
        pd_df = pd_df.set_index(DEFAULT_ARROW_INDEX_NAME)
        pd_df.index.name = None

    return pd_df


def get_previous_item(item, sequence):
    idx = sequence.index(item)
    is_not_first_item = idx > 0
    if is_not_first_item:
        return sequence[idx - 1]


def get_next_item(item, sequence):
    idx = sequence.index(item)
    is_not_last_item = idx < (len(sequence) - 1)
    if is_not_last_item:
        return sequence[idx + 1]


def make_partitions(df, rows_per_partition):
    df = df.combine_chunks()
    if rows_per_partition == -1:
        partitions = _make_single_partition(df)
    else:
        partitions = df.to_batches(rows_per_partition)
        partitions = _combine_small_partitions(partitions, rows_per_partition)
        if len(partitions) == 0:
            partitions = [pa.RecordBatch.from_pylist(df.to_pylist(), df.schema)]
    return partitions


def _make_single_partition(df):
    return df.to_batches()


def _combine_small_partitions(partitions, partition_size):
    has_multiple_partitions = len(partitions) > 1
    try:
        size_of_last_partition = partitions[-1].num_rows
    except IndexError:
        size_of_last_partition = 0
    min_partition_size = partition_size * 0.5

    if has_multiple_partitions and size_of_last_partition < min_partition_size:
        new_last_partition = _combine_last_two_partitions(partitions)
        partitions = _replace_last_two_partitions(new_last_partition,
                                                  partitions)
    return partitions


def _combine_last_two_partitions(partitions):
    last_partition = pa.Table.from_batches(partitions[-2:])
    last_partition = last_partition.combine_chunks()
    return last_partition.to_batches()


def _replace_last_two_partitions(new_last_partition, partitions):
    partitions = partitions[:-2]
    partitions.extend(new_last_partition)
    return partitions


def convert_int_to_partition_id(partition_id):
    partition_id = int(partition_id * INSERTION_BUFFER_LENGTH)
    format_string = f'0{PARTITION_NAME_LENGTH}d'
    partition_id = format(partition_id, format_string)
    return partition_id


def convert_partition_id_to_int(partition_id):
    return int(partition_id) // INSERTION_BUFFER_LENGTH


def add_new_partition_ids(partitions, partition_ids):
    partition_ids = partition_ids.copy()
    num_new_partition_ids = len(partitions) - len(partition_ids) + 1
    partition_ids = append_new_partition_ids(num_new_partition_ids, partition_ids)
    return sorted(partition_ids)


def append_new_partition_ids(num_partitions, partition_ids):
    last_partition_id = partition_ids[-1]

    range_start = convert_partition_id_to_int(last_partition_id) + 1
    range_end = range_start + num_partitions - 1

    for partition_num in range(range_start, range_end):
        partition_id = convert_int_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids


def assign_ids_to_partitions(df, ids):
    if len(df) != len(ids):
        raise IndexError("Num partitions doesn't match num partition names")
    id_mapping = {}
    for identifier, partition in zip(ids, df):
        id_mapping[identifier] = partition
    return id_mapping


def get_first_stored_index_value(partition_metadata):
    first_partition = partition_metadata.keys()[0]
    first_stored_value = partition_metadata[first_partition]['min']
    return first_stored_value


def get_last_stored_index_value(partition_metadata):
    last_partition = partition_metadata.keys()[-1]
    last_stored_value = partition_metadata[last_partition]['max']
    return last_stored_value


def get_index_name(df):
    pd_metadata = df.schema.pandas_metadata
    if pd_metadata is None:
        no_index_name = True
    else:
        index_name, = pd_metadata["index_columns"]
        no_index_name = not isinstance(index_name, str)

    if no_index_name:
        index_name = DEFAULT_ARROW_INDEX_NAME

    return index_name


def get_index_dtype(df):
    # Uses the fact that index should be first col
    index_dtype = str(df.field(0).type)
    return index_dtype


def typestring_is_temporal(index_dtype):
    return "time" in index_dtype or "date" in index_dtype


def typestring_is_string(index_dtype):
    return "string" in index_dtype


def typestring_is_int(index_dtype):
    return "int" in index_dtype


def get_index_if_exists(df, index_name):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        index = convert_to_arrow(df.index, as_array=True)
    else:
        try:
            index = df[index_name]
        except Exception:
            index = None
    return convert_to_arrow(index, as_array=True)


def filter_arrow_table(df, rows, index_col_name):
    index = df[index_col_name]
    if not rows.keyword:
        df = _fetch_rows_in_list(df, index, rows.values())
    elif rows.keyword == 'before':
        df = _fetch_rows_before(df, index, rows[0])
    elif rows.keyword == 'after':
        df = _fetch_rows_after(df, index, rows[0])
    elif rows.keyword == 'between':
        df = _fetch_rows_between(df, index, low=rows[0], high=rows[1])
    return df


def _fetch_rows_in_list(df, index, rows):
    if not rows:
        return pa.table([[]] * len(df.column_names), schema=df.schema)
    row_indices = pa.compute.index_in(rows, value_set=index)
    _raise_if_rows_not_in_table(row_indices)
    df = pa.compute.take(df, row_indices, boundscheck=False)
    return df


def _raise_if_rows_not_in_table(row_indices):
    contains_null = row_indices.null_count > 0
    if contains_null:
        raise IndexError('Trying to access a row not found in table')


def _fetch_rows_before(df, index, row):
    upper_bound = _compute_upper_bound(row, index)
    df = df[:upper_bound]
    return df


def _fetch_rows_after(df, index, row):
    lower_bound = _compute_lower_bound(row, index)
    df = df[lower_bound:]
    return df


def _fetch_rows_between(df, index, low, high):
    lower_bound = _compute_lower_bound(low, index)
    upper_bound = _compute_upper_bound(high, index)
    df = df[lower_bound:upper_bound]
    return df


def _compute_lower_bound(row, index):
    lower_bound = __fetch_row_idx(row, index)
    return lower_bound


def _compute_upper_bound(row, index):
    upper_bound = __fetch_row_idx(row, index, is_upper_bound=True)
    return upper_bound


def __fetch_row_idx(row, index, is_upper_bound=False):
    row_idx = _fetch_exact_row_idx(row, index, is_upper_bound)

    no_row_idx_found = row_idx is None
    if no_row_idx_found:
        row_idx = _fetch_closest_row_idx(row, index)

    no_close_row_idx_found = row_idx is None
    if no_close_row_idx_found:
        row_idx = _fetch_last_row_idx(index)
    return row_idx


def _fetch_exact_row_idx(row, index, is_upper_bound):
    row_idx = index.index(row)
    row_idx = row_idx.as_py()
    if row_idx == -1:
        row_idx = None
    elif is_upper_bound:
        row_idx += 1
    return row_idx


def _fetch_closest_row_idx(row, index):
    mask = pa.compute.less_equal(row, index)
    row_idx = mask.index(True)
    row_idx = row_idx.as_py()
    if row_idx == -1:
        row_idx = None
    return row_idx


def _fetch_last_row_idx(index):
    return len(index)


def is_collection(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes))


def is_list_like(obj):
    is_iterable = isinstance(obj, Iterable)
    is_not_string = not isinstance(obj, (str, bytes))
    is_not_set = not isinstance(obj, Set)
    return is_iterable and is_not_string and is_not_set
