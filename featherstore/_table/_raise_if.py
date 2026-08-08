import os
from decimal import Decimal
from numbers import Integral, Real

import pandas as pd
import pyarrow as pa

from featherstore._metadata import METADATA_FOLDER_NAME
from featherstore._table import _table_utils, common
from featherstore._table._indexers import ColIndexer, RowIndexer
from featherstore.connection import Connection
from featherstore.exceptions import (
    ColumnAlreadyExistsError,
    ColumnLengthMismatchError,
    ColumnMismatchError,
    ColumnNotFoundError,
    DuplicateColumnNamesError,
    DuplicateIndexValuesError,
    ForbiddenTableNameError,
    IndexNameInColumnsError,
    IndexNameMismatchError,
    IndexTypeMismatchError,
    RowAlreadyExistsError,
    RowNotFoundError,
    TableAlreadyExistsError,
    TableNotFoundError,
    UnsupportedIndexTypeError,
)

_STRING_ARROW_TYPES = {"string", "utf8", "large_string", "large_utf8"}

NoneType = type(None)


def table_not_exists(table):
    if not table.exists():
        raise TableNotFoundError(f"Table '{table.name}' not found")


def table_already_exists(table_path):
    table_name = table_path.rsplit("/")[-1]
    if os.path.exists(table_path):
        raise TableAlreadyExistsError(
            f"A table with name '{table_name}' already exists"
        )


def table_name_is_not_str(table_name):
    if not isinstance(table_name, str):
        raise TypeError(f"'table_name' must be a str (is type {type(table_name)})")


def table_name_is_forbidden(table_name):
    if table_name == METADATA_FOLDER_NAME:
        raise ForbiddenTableNameError(
            f"Table name '{METADATA_FOLDER_NAME}' is forbidden"
        )


def df_is_not_table_type(df, allowed_types):
    if not isinstance(df, allowed_types):
        raise TypeError(f"'df' must be a supported DataFrame type (is type {type(df)})")


def df_is_not_pandas_table(df):
    if not isinstance(df, (pd.DataFrame, pd.Series)):
        raise TypeError(
            f"'df' must be a pd.DataFrame or pd.Series (is type {type(df)})"
        )


def rows_argument_is_not_collection(rows):
    is_collection = _table_utils.is_collection(rows)
    if not is_collection:
        raise TypeError(f"'rows' must be a collection (is type {type(rows)})")


def rows_argument_is_not_collection_or_none(rows):
    is_collection_or_none = _table_utils.is_collection(rows) or rows is None
    if not is_collection_or_none:
        raise TypeError(f"'rows' must be a collection or None (is type {type(rows)})")


def to_argument_is_not_list_like(to):
    is_list_like = _table_utils.is_list_like(to)
    if not is_list_like:
        raise TypeError(f"'to' must be list like (is type {type(to)})")


def cols_argument_is_not_list_like(cols):
    is_list_like = _table_utils.is_list_like(cols)
    if not is_list_like:
        raise TypeError(f"'cols' must be list like (is type {type(cols)})")


def cols_argument_is_not_collection(cols):
    is_collection = _table_utils.is_collection(cols)
    if not is_collection:
        raise TypeError(f"'cols' must be a collection (is type {type(cols)})")


def cols_argument_is_not_collection_or_none(cols):
    is_collection_or_none = _table_utils.is_collection(cols) or cols is None
    if not is_collection_or_none:
        raise TypeError(f"'cols' must be a collection or None (is type {type(cols)})")


def cols_argument_items_is_not_str_or_none(cols):
    col_types = set(map(type, cols))
    col_elements_are_str = True
    for col_type in col_types:
        if col_type not in {str, NoneType}:
            col_elements_are_str = False
    if not col_elements_are_str:
        raise TypeError("Elements in 'cols' must be of type str")


def length_of_cols_and_to_doesnt_match(cols, to):
    if len(cols) != len(to):
        raise ValueError(
            f"Length of 'cols' != length of 'to' ({len(cols)} != {len(to)})"
        )


def cols_does_not_match(df, table_data):
    stored_data_cols = table_data["columns"]
    has_default_index = table_data["has_default_index"]
    new_data_cols = _table_utils.get_col_names(df, has_default_index)

    if sorted(new_data_cols) != sorted(stored_data_cols):
        raise ColumnMismatchError(
            "New and old columns doesn't match "
            f"(new={sorted(new_data_cols)}, stored={sorted(stored_data_cols)})"
        )


def cols_not_in_table(cols, table_data):
    cols, stored_cols = _cols_like_stored(cols, table_data)
    missing = sorted(set(cols) - set(stored_cols))
    if missing:
        raise ColumnNotFoundError(
            f"Trying to access columns not found in table ({missing})"
        )


def cols_already_in_table(cols, table_data):
    cols, stored_cols = _cols_like_stored(cols, table_data)
    existing = sorted(set(cols) & set(stored_cols))
    if existing:
        raise ColumnAlreadyExistsError(
            f"Column names already exist in table ({existing})"
        )


def _cols_like_stored(cols, table_data):
    stored_cols = table_data["columns"]
    if not isinstance(cols, ColIndexer):
        cols = ColIndexer(cols)
    return cols.like(stored_cols), stored_cols


def index_values_in_stored_data(old_df, df, index_name, *, all_must_be_in):
    index = df[index_name]
    old_index = old_df[index_name]
    is_in = pa.compute.is_in(index, value_set=old_index)
    if all_must_be_in:
        if not pa.compute.all(is_in).as_py():
            missing = index.filter(pa.compute.invert(is_in)).to_pylist()
            raise RowNotFoundError(f"Some rows not in stored table ({missing})")
    elif pa.compute.any(is_in).as_py():
        raise RowAlreadyExistsError("Some rows already in stored table")


def to_is_provided_twice(cols, to):
    cols_is_dict = isinstance(cols, dict)
    to_is_provided = to is not None
    if cols_is_dict and to_is_provided:
        raise AttributeError(
            "'to' is provided twice, use either "
            "'cols={<COL>: <TO>, ...}, to=None' "
            "or 'cols=[<COL>, ...], to=[<TO>, ...]'"
        )


def to_not_provided(cols, to):
    cols_is_dict = isinstance(cols, dict)
    to_is_provided = to is not None
    if not cols_is_dict and not to_is_provided:
        raise AttributeError("'to' is not provided")


def rows_items_not_all_same_type(rows):
    try:
        rows = rows.values()
        if rows is not None:
            pa.array(rows)
    except (TypeError, pa.ArrowInvalid, pa.ArrowTypeError):
        raise TypeError("'rows' items not all of same type")


def rows_argument_items_type_not_same_as_index(rows, table_data):
    index_dtype = table_data["index_dtype"]
    if rows and not _rows_type_matches_index(rows, index_dtype):
        raise IndexTypeMismatchError(
            f"'rows' type doesn't match table index dtype (index_dtype={index_dtype})"
        )


def _rows_type_matches_index(rows, index_dtype):
    row = rows[0]
    checks = (
        _check_if_row_and_index_is_temporal,
        _check_if_row_and_index_is_duration,
        _check_if_row_and_index_is_str,
        _check_if_row_and_index_is_binary,
        _check_if_row_and_index_is_int,
        _check_if_row_and_index_is_float,
        _check_if_row_and_index_is_decimal,
    )
    return any(check(row, index_dtype) for check in checks)


def _check_if_row_and_index_is_temporal(row, index_dtype):
    if _table_utils.typestring_is_temporal(index_dtype):
        return _isinstance_temporal(row)
    return False


def _check_if_row_and_index_is_duration(row, index_dtype):
    if _table_utils.typestring_is_duration(index_dtype):
        return _isinstance_duration(row)
    return False


def _check_if_row_and_index_is_str(row, index_dtype):
    if _table_utils.typestring_is_string(index_dtype):
        return _isinstance_str(row)
    return False


def _check_if_row_and_index_is_binary(row, index_dtype):
    if _table_utils.typestring_is_binary(index_dtype):
        return _isinstance_binary(row)
    return False


def _check_if_row_and_index_is_int(row, index_dtype):
    if _table_utils.typestring_is_int(index_dtype):
        return _isinstance_int(row)
    return False


def _check_if_row_and_index_is_float(row, index_dtype):
    if _table_utils.typestring_is_float(index_dtype):
        return _isinstance_float(row)
    return False


def _check_if_row_and_index_is_decimal(row, index_dtype):
    if _table_utils.typestring_is_decimal(index_dtype):
        return _isinstance_decimal(row)
    return False


def _isinstance_temporal(obj):
    try:
        if isinstance(obj, str):
            _ = pd.Timestamp(obj)
        elif isinstance(obj, pd.Timestamp):
            pass
        else:
            _ = pd.to_datetime(obj)
        is_temporal = True
    except (ValueError, TypeError, pd.errors.OutOfBoundsDatetime):
        is_temporal = False
    return is_temporal


def _isinstance_str(obj):
    try:
        is_str = pa.types.is_string(obj) or pa.types.is_large_string(obj)
    except AttributeError:
        is_str = isinstance(obj, str)
    return is_str


def _isinstance_duration(obj):
    return isinstance(obj, pd.Timedelta)


def _isinstance_int(obj):
    try:
        is_int = pa.types.is_integer(obj)
    except AttributeError:
        is_int = isinstance(obj, Integral)
    return is_int


def _isinstance_float(obj):
    return isinstance(obj, Real) and not isinstance(obj, (Integral, bool))


def _isinstance_decimal(obj):
    return isinstance(obj, Decimal)


def _isinstance_binary(obj):
    return isinstance(obj, (bytes, bytearray))


def index_type_not_supported(index_or_dtype):
    if index_or_dtype is None:
        return
    if isinstance(index_or_dtype, pa.DataType):
        index_type = index_or_dtype
    elif hasattr(index_or_dtype, "type"):
        index_type = index_or_dtype.type
    else:
        index_type = index_or_dtype
    if not _table_utils.index_type_is_supported(index_type):
        raise UnsupportedIndexTypeError(
            f"Table.index type is not supported (is type {index_type})"
        )


def index_type_not_same_as_stored_index(df, table_data):
    index_name = table_data["index_name"]
    index = _table_utils.get_index_if_exists(df, index_name)
    if index is not None:
        index_type = _normalize_index_dtype(index.type)
        stored_index_type = _normalize_index_dtype(table_data["index_dtype"])
        if index_type != stored_index_type:
            raise IndexTypeMismatchError(
                "New and old index types do not match "
                f"(new={index_type}, stored={stored_index_type})"
            )


def _normalize_index_dtype(dtype):
    dtype = str(dtype)
    if dtype in _STRING_ARROW_TYPES:
        return "string"
    if dtype.startswith("timestamp"):
        return "timestamp"
    return dtype


def index_name_not_same_as_stored_index(df, table_data):
    stored_index_name = table_data["index_name"]
    has_default_index = table_data["has_default_index"]
    cols = _table_utils.get_col_names(df, has_default_index=has_default_index)
    if stored_index_name not in cols:
        raise IndexNameMismatchError(
            "New and old index names do not match "
            f"(stored_index_name={stored_index_name!r})"
        )


def index_in_cols(cols, table_data):
    index_name = table_data["index_name"]
    if index_name in cols:
        raise IndexNameInColumnsError(
            f"Index name in 'cols' (index_name={index_name!r})"
        )


def col_names_contains_duplicates(cols):
    col_list = list(cols)
    duplicates = sorted({col for col in col_list if col_list.count(col) > 1})
    if duplicates:
        raise DuplicateColumnNamesError(
            f"Column names must be unique (duplicates={duplicates})"
        )


def index_values_contains_duplicates(index):
    if index is not None:
        index = _table_utils.convert_to_polars(index, as_array=True)
        contains_duplicates = index.n_unique() < index.shape[0]
        if contains_duplicates:
            raise DuplicateIndexValuesError("Index values must be unique")


def not_connected():
    Connection._raise_if_not_connected()


def not_connected_or_table_not_exists(table):
    not_connected()
    table_not_exists(table)


def incoming_index_schema_incompatible_with_stored(
    df, table_data, cols, *, index=None, check_index_values=True
):
    index_name_not_same_as_stored_index(df, table_data)
    col_names_contains_duplicates(cols)
    index_type_not_same_as_stored_index(df, table_data)
    if check_index_values:
        if index is None:
            index = _table_utils.get_index_if_exists(df, table_data["index_name"])
        index_values_contains_duplicates(index)


def row_count_does_not_match_stored(df, table_data):
    stored_row_count = table_data["num_rows"]
    incoming_row_count = len(df)
    if incoming_row_count != stored_row_count:
        raise ColumnLengthMismatchError(
            f"Number of rows in new columns ({incoming_row_count}) doesn't match "
            f"stored table ({stored_row_count})"
        )


def rows_argument_is_not_valid(rows, table_data, *, allow_none=False):
    if allow_none:
        rows_argument_is_not_collection_or_none(rows)
    else:
        rows_argument_is_not_collection(rows)

    rows = RowIndexer(rows)
    rows_items_not_all_same_type(rows)
    rows_argument_items_type_not_same_as_index(rows, table_data)
    return rows


def cols_and_to_arguments_are_not_valid(cols, to):
    cols_argument_is_not_collection(cols)
    to_is_provided_twice(cols, to)
    to_not_provided(cols, to)

    if not isinstance(cols, dict):
        to_argument_is_not_list_like(to)
        length_of_cols_and_to_doesnt_match(cols, to)
    return common.format_cols_and_to_args(cols, to)
