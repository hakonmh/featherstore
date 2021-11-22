import pandas as pd
import pyarrow as pa

from featherstore._metadata import Metadata
from featherstore._table.common import (
    _check_index_constraints,
    _check_column_constraints,
    _coerce_column_dtypes,
    _convert_to_partition_id,
    _convert_partition_id_to_int,
    _get_index_dtype,
    _rows_dtype_matches_index
)
from featherstore._table.read import _make_arrow_filter_mask


def can_drop_rows_from_table(rows, table_path, table_exists):
    if not table_exists:
        raise FileNotFoundError("Table doesn't exist")

    if not isinstance(rows, list):
        raise TypeError(
            f"'rows' must be a list (is type {type(rows)})")
    else:
        rows = pd.Index(rows)

    _check_index_constraints(rows)

    index_dtype = Metadata(table_path, "table")["index_dtype"]
    if rows is not None and not _rows_dtype_matches_index(rows, index_dtype):
        raise TypeError("'rows' type doesn't match table index")


def drop_rows_from_data(df, rows, index_col_name):
    index = df[index_col_name]
    _check_if_rows_isin_index(index, rows)
    mask = _make_arrow_filter_mask(index, rows)
    mask = pa.compute.invert(mask)
    df = df.filter(mask)
    return df


def _check_if_rows_isin_index(index, rows):
    keyword = str(rows[0]).lower()
    if keyword not in ("before", "after", "between"):
        row_array = pa.array(rows)
        index = pa.compute.SetLookupOptions(value_set=index)
        mask = pa.compute.is_in(row_array, options=index)

    rows_not_in_old_df = not pa.compute.min(mask).as_py()
    if rows_not_in_old_df:
        raise ValueError(f"Some rows not in stored table")
