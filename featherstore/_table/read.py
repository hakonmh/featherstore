import pyarrow as pa
from pyarrow import feather
import pandas as pd

from .._metadata import Metadata
from .._utils import like_pattern_matching


def can_read_table(cols, rows, table_exists):
    if not table_exists:
        raise FileNotFoundError("Table doesn't exixst")

    is_valid_col_format = isinstance(cols, (list, type(None)))
    if not is_valid_col_format:
        raise TypeError("'cols' must be either list or None")

    is_valid_row_format = isinstance(rows, (list, pd.Index, type(None)))
    if not is_valid_row_format:
        raise AttributeError("'rows' must be either List, or None")


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
    elif to == "string":
        row = str(row)
    elif to == "int64":
        row = int(row)
    return row


def get_partition_names(table, rows):
    if rows:
        partition_names = _predicate_filtering(table, rows)
    else:
        partition_names = table._partition_data["name"]
    return partition_names


def _predicate_filtering(table, rows):
    index_type = table._table_data["index_dtype"]
    partition_stats = _get_partition_stats(table, index_type)
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


def _get_partition_stats(table, index_type):
    partition_stats = dict()
    partition_stats["name"] = table._partition_data["name"]
    partition_stats["min"] = table._partition_data["min"]
    partition_stats["max"] = table._partition_data["max"]

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
        partition_path = f"{table_path}/{partition_name}.feather"
        partition = feather.read_table(partition_path, columns=columns, memory_map=True)
        partitions.append(partition)
    return partitions


def filter_table_rows(df, rows, index_col_name):
    should_be_filtered = rows is not None
    if should_be_filtered:
        df = _row_filtering(df, rows, index_col_name)
    return df


def _row_filtering(df, rows, index_col_name):
    index = df[index_col_name]
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
    df = df.filter(mask)
    return df


def drop_default_index(df, index_col_name):
    df = df.drop([index_col_name])
    return df


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
