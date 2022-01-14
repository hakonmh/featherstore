import pyarrow as pa
import pandas as pd
import polars as pl

from featherstore._utils import DEFAULT_ARROW_INDEX_NAME

PARTITION_NAME_LENGTH = 14
INSERTION_BUFFER_LENGTH = 10**6


def get_first_row(df):
    if isinstance(df, pd.DataFrame):
        return df.iloc[:1]
    else:
        return df[:1]


def get_col_names(df, has_default_index):
    first_row = get_first_row(df)
    first_row = convert_to_arrow(first_row)
    cols = first_row.column_names
    if has_default_index and DEFAULT_ARROW_INDEX_NAME not in cols:
        cols.append(DEFAULT_ARROW_INDEX_NAME)
    return cols


def coerce_col_dtypes(df, *, to):
    cols = df.columns
    dtypes = to[cols].dtypes
    try:
        df = df.astype(dtypes)
    except ValueError:
        raise TypeError("New and old column dtypes do not match")
    return df


def convert_to_arrow(df):
    if isinstance(df, pd.Series):
        df = df.to_frame()
    if isinstance(df, pd.DataFrame):
        df = pa.Table.from_pandas(df, preserve_index=True)
    elif isinstance(df, pl.DataFrame):
        df = df.to_arrow()
    return df


def convert_to_polars(df):
    if isinstance(df, (pd.Series, pd.DataFrame)):
        df = convert_to_arrow(df)
    if isinstance(df, pa.Table):
        df = pl.from_arrow(df)
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


def make_partitions(df, rows_per_partition):
    df = df.combine_chunks()
    partitions = df.to_batches(rows_per_partition)
    partitions = _combine_small_partitions(partitions, rows_per_partition)
    return partitions


def _combine_small_partitions(partitions, partition_size):
    has_multiple_partitions = len(partitions) > 1
    size_of_last_partition = partitions[-1].num_rows
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


def assign_ids_to_partitions(df, ids):
    if len(df) != len(ids):
        raise IndexError("Num partitions doesn't match num partition names")
    id_mapping = {}
    for identifier, partition in zip(ids, df):
        id_mapping[identifier] = partition
    return id_mapping


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


def str_is_temporal_dtype(index_dtype):
    return "time" in index_dtype or "date" in index_dtype


def str_is_string_dtype(index_dtype):
    return "string" in index_dtype


def str_is_int_dtype(index_dtype):
    return "int" in index_dtype


def get_pd_index_if_exists(df, index_name):
    if isinstance(df, (pd.DataFrame, pd.Series)):
        index = df.index
    else:
        index = _get_index_if_index_in_table(df, index_name)
    return index


def _get_index_if_index_in_table(df, index_name):
    INDEX_NOT_IN_DF = (KeyError, RuntimeError, TypeError)
    try:
        pd_index = _get_index_as_pd_index(df, index_name)
    except INDEX_NOT_IN_DF:
        pd_index = None
    return pd_index


def _get_index_as_pd_index(df, index_name):
    index = df[index_name]
    if isinstance(index, pl.Series):
        index = index.to_arrow()
    return pd.Index(index)


def filter_arrow_table(df, rows, index_col_name):
    keyword = str(rows[0]).lower()
    index = df[index_col_name]
    if keyword not in ('before', 'after', 'between'):
        df = _fetch_rows_in_list(df, index, rows)
    elif keyword == 'before':
        df = _fetch_rows_before(df, index, rows[1])
    elif keyword == 'after':
        df = _fetch_rows_after(df, index, rows[1])
    elif keyword == 'between':
        df = _fetch_rows_between(df, index, low=rows[1], high=rows[2])
    return df


def _fetch_rows_in_list(df, index, rows):
    rows_indices = pa.compute.index_in(rows, value_set=index)
    df = df.take(rows_indices)
    return df


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
    lower_bound = __fetch_row_idx(row, index) - 1
    return lower_bound


def _compute_upper_bound(row, index):
    upper_bound = __fetch_row_idx(row, index)
    return upper_bound


def __fetch_row_idx(row, index):
    row_idx = __fetch_exact_row_idx(row, index)

    no_row_idx_found = row_idx is None
    if no_row_idx_found:
        row_idx = __fetch_closest_row_idx(row, index)

    no_close_row_idx_found = row_idx is None
    if no_close_row_idx_found:
        row_idx = __fetch_last_row_idx(index)
    return row_idx


def __fetch_exact_row_idx(row, index):
    row_idx = pa.compute.index_in(row, value_set=index)
    row_idx = row_idx.as_py()
    if row_idx is not None:
        row_idx += 1
    return row_idx


def __fetch_closest_row_idx(row, index):
    TRUE = 1
    mask = pa.compute.less_equal(row, index)
    row_idx = pa.compute.index_in(TRUE, value_set=mask)
    row_idx = row_idx.as_py()
    return row_idx


def __fetch_last_row_idx(index):
    return len(index)
