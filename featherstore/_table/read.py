import datetime
import os

import pyarrow as pa
from pyarrow import feather
import pandas as pd
import polars as pl

from featherstore._metadata import Metadata, get_partition_attr
from featherstore._utils import like_pattern_matching


def can_read_table(cols, rows, table_exists, table_metadata):
    if not table_exists:
        raise FileNotFoundError("Table doesn't exixst")

    is_valid_col_format = isinstance(cols, (list, type(None)))
    if not is_valid_col_format:
        raise TypeError("'cols' must be either list or None")

    is_valid_row_format = isinstance(rows, (list, pd.Index, type(None)))
    if not is_valid_row_format:
        raise TypeError("'rows' must be either List, or None")

    index_dtype = table_metadata["index_dtype"]
    if rows and not _rows_dtype_matches_index(rows, index_dtype):
        raise TypeError("'rows' type doesn't match table index")


def _rows_dtype_matches_index(rows, index_dtype):
    try:
        _convert_row(rows[-1], to=index_dtype)
        row_type_matches = True
    except Exception:
        row_type_matches = False
    return row_type_matches


def format_cols(cols, table_data):
    if cols:
        keyword = str(cols[0]).lower()
        if keyword == "like":
            like = cols[1]
            table_columns = table_data["columns"]
            cols = like_pattern_matching(like, table_columns)
    return cols


def format_rows(rows, index_type):
    if rows:
        keyword = str(rows[0]).lower()
        if keyword in {"between", "before", "after"}:
            rows[1:] = [_convert_row(item, to=index_type) for item in rows[1:]]
        else:
            rows = [_convert_row(item, to=index_type) for item in rows]
    return rows


def _convert_row(row, *, to):
    if to == "datetime64":
        row = pd.to_datetime(row)
    elif to == "string" or to == "unicode":
        row = str(row)
    elif to == "int64":
        row = int(row)
    return row


def get_partition_names(rows, table_path):
    if rows:
        partition_names = _predicate_filtering(rows, table_path)
    else:
        partition_names = Metadata(table_path, "table")["partitions"]
    return partition_names


def _predicate_filtering(rows, table_path):
    partition_stats = _get_partition_stats(table_path)
    keyword = str(rows[0]).lower()
    if keyword == "before":
        mask = rows[1] >= partition_stats
        mask = mask.max(axis=1)
    elif keyword == "after":
        mask = rows[1] <= partition_stats
        mask = mask.max(axis=1)
    elif keyword == "between":
        lower_bound = rows[1] <= partition_stats["max"]
        higher_bound = rows[2] >= partition_stats["min"]
        mask = higher_bound & lower_bound
    else:  # When a list of rows is provided
        max_ = max(rows)
        min_ = min(rows)
        lower_bound = min_ <= partition_stats["max"]
        higher_bound = max_ >= partition_stats["min"]
        mask = higher_bound & lower_bound

    filtered_partition_stats = partition_stats[mask]
    partition_names = filtered_partition_stats.index.tolist()
    return partition_names


def _get_partition_stats(table_path):
    index_type = Metadata(table_path, "table")["index_dtype"]

    partition_stats = dict()
    partition_stats["name"] = Metadata(table_path, 'table')['partitions']
    partition_stats["min"] = get_partition_attr(table_path, 'min')
    partition_stats["max"] = get_partition_attr(table_path, 'max')

    partition_stats = pd.DataFrame(partition_stats)
    partition_stats.set_index("name", inplace=True)
    for col in partition_stats:
        partition_stats[col] = partition_stats[col].astype(index_type)
    return partition_stats


def read_partitions(partition_names, table_path, columns):
    if columns is not None:
        index_col = Metadata(table_path, "table")["index_name"]
        columns = columns.copy()
        columns.append(index_col)

    partitions = []
    for partition_name in partition_names:
        partition_path = os.path.join(table_path, f"{partition_name}.feather")
        partition = feather.read_table(partition_path, columns=columns, memory_map=True)
        partitions.append(partition)
    return partitions


def filter_table_rows(df, rows, index_col_name):
    should_be_filtered = rows is not None
    if should_be_filtered:
        index = df[index_col_name]
        if isinstance(df, pa.Table):
            mask = _make_arrow_filter_mask(index, rows)
            df = df.filter(mask)
        elif isinstance(df, pl.DataFrame):
            index = _convert_polars_index_to_arrow(index, rows)
            mask = _make_arrow_filter_mask(index, rows)
            mask = _convert_arrow_mask_to_polars(mask)
            df = df[mask]
    return df


def _convert_polars_index_to_arrow(index, rows):
    index = index.to_arrow()
    if isinstance(rows[-1], datetime.datetime):
        index = index.cast(pa.date64())
    return index


def _make_arrow_filter_mask(index, rows):
    keyword = str(rows[0]).lower()
    if keyword == "before":
        mask = pa.compute.greater_equal(rows[1], index)
    elif keyword == "after":
        mask = pa.compute.less_equal(rows[1], index)
    elif keyword == "between":
        lower_bound = pa.compute.less_equal(rows[1], index)
        higher_bound = pa.compute.greater_equal(rows[2], index)
        mask = pa.compute.and_(lower_bound, higher_bound)
    else:  # When a list of rows is provided
        row_array = pa.compute.SetLookupOptions(value_set=pa.array(rows))
        mask = pa.compute.is_in(index, options=row_array)
    return mask


def _convert_arrow_mask_to_polars(mask):
    mask = pl.from_arrow(mask)
    mask = pl.arg_where(mask)
    return mask


def drop_default_index(df, index_col_name):
    df = df.drop([index_col_name])
    return df


def can_be_converted_to_series(df):
    num_cols = df.shape[1]
    return num_cols == 1


def can_be_converted_to_rangeindex(df):
    is_already_rangeindex = isinstance(df.index, pd.RangeIndex)
    if is_already_rangeindex:
        can_be_converted = False
    else:
        corresponding_rangeindex = pd.RangeIndex(start=0, stop=len(df.index))
        is_equal_to_rangeindex = df.index.equals(corresponding_rangeindex)
        if is_equal_to_rangeindex:
            can_be_converted = True
        else:
            can_be_converted = False
    return can_be_converted


def convert_to_rangeindex(df):
    index_name = df.index.name
    df = df.reset_index(drop=True)
    df.index.name = index_name
    return df


def convert_partitions_to_polars(partitions):
    for idx, partition in enumerate(partitions):
        partitions[idx] = pl.from_arrow(partition, rechunk=False)
    return partitions
