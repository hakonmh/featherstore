import pytest
from .fixtures import *

import numpy as np
import pyarrow as pa
import polars as pl


TYPE_MAP = {
    pa.float16(): 'float',
    pa.float32(): 'float',
    pa.float64(): 'float',
    pa.int16(): 'int',
    pa.int32(): 'int',
    pa.int64(): 'int',
    pa.uint32(): 'uint',
    pa.bool_(): 'bool',
    pa.date32(): 'datetime',
    pa.date64(): 'datetime',
    pa.time32('ms'): 'datetime',
    pa.time64('us'): 'datetime',
    pa.timestamp('us'): 'datetime',
    pa.string(): 'string',
    pa.large_string(): 'string',
    pa.binary(): 'string',
    pa.large_binary(): 'string',
}
kwargs_as_list = True


@pytest.mark.parametrize(
    ("from_dtype", "to_dtype"),
    [
        (pa.float32(), pa.float64()),
        (pa.float32(), pa.decimal128(5, 5)),
        (pa.int64(), pa.int32()),
        (pa.int32(), pa.float64()),
        (pa.int32(), pa.decimal128(19, 0)),
        (pa.uint32(), pa.int32()),
        (pa.bool_(), pa.int16()),
        (pa.bool_(), pa.string()),
        (pa.int64(), pa.bool_()),
        (pa.date32(), pa.date64()),
        (pa.date32(), pa.timestamp('us')),
        (pa.date64(), pa.date32()),
        (pa.timestamp('us'), pa.date64()),
        (pa.timestamp('us'), pa.time32('ms')),
        (pa.time32('ms'), pa.time64('us')),
        (pa.binary(), pa.large_binary()),
        (pa.binary(), pa.string()),
        (pa.string(), pa.binary()),
        (pa.large_string(), pa.string()),
        (np.int64, np.int32),
        (np.float32, np.float64),
        (np.int64, np.int32),
        (np.int32, np.float64),
        (np.uint32, np.int32),
        (bool, np.int16),
        (np.int64, bool),
    ]
)
def test_change_pa_dtype(store, from_dtype, to_dtype):
    # Arrange
    COLS = ['c1', 'c2']
    original_df = make_table(rows=60, cols=3, astype="arrow",
                             dtype=_get_dtype_str(from_dtype))
    original_df = _change_dtype(original_df, from_dtype)
    expected = _change_dtype(original_df, to_dtype, cols=COLS)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    cols, dtype = _format_cols_and_to_key_args(COLS, to_dtype)
    # Act
    table.astype(cols, to=dtype)
    # Assert
    assert_table_equals(table, expected, astype='all')


def _get_dtype_str(dtype):
    dtype = __convert_to_arrow_dtype(dtype)
    return TYPE_MAP[dtype]


def _change_dtype(df, to, index_name='index', cols=None):
    arrow_dtype = __convert_to_arrow_dtype(to)
    schema = df.schema
    cols = cols if cols else schema.names
    for idx, field in enumerate(schema):
        if field.name in cols and field.name != index_name:
            field = field.with_type(arrow_dtype)
            schema = schema.set(idx, field)
    df = df.cast(schema)
    return df


def __convert_to_arrow_dtype(dtype):
    try:
        dtype = pa.from_numpy_dtype(dtype)
    except Exception:
        pass
    return dtype


def _format_cols_and_to_key_args(cols, to):
    """Selects every other test to format arguments as one of either:
        * cols=['c1', 'c2'], to=[dtype1, dtype2]
        * cols = {'c1': dtype1, 'c2': dtype2}
    """
    global kwargs_as_list
    kwargs_as_list = not kwargs_as_list
    kwarg_format = 'list' if kwargs_as_list else 'dict'

    if kwarg_format == 'dict':
        cols = {col: to for col in cols}
        to = None
    elif kwarg_format == 'list':
        to = [to] * len(cols)
    return cols, to


NEW_DTYPES_PROVIDED_TWICE = [{'c0': pa.int16()}, [pa.int16()]]
NEW_DTYPES_NOT_PROVIDED = [['c0', 'c1'], None]
INVALID_COLS_ARGUMENT_DTYPE = ['c0', [pa.int16()]]
INVALID_COLS_KEYS_ARGUMENT_DTYPE = [{1: pa.int32}, None]
INVALID_COLS_VALUES_ARGUMENT_DTYPE = [{'c0': 123}, None]
INVALID_TO_ARGUMENT_DTYPE = [['c0'], pa.int16()]
INVALID_TO_ITEMS_DTYPE = [['c0'], [123]]
NUMBER_OF_COLS_AND_DTYPES_DONT_MATCH = [['c0', 'c1'], [pa.int16()]]
FORBIDDEN_INDEX_DTYPE = [[DEFAULT_ARROW_INDEX_NAME], [pa.float32()]]
NON_ARROW_DTYPE = [['c0'], [pl.Float32()]]
INVALID_NEW_DTYPE = [['c0'], [pa.float32()]]
DUPLICATE_COL_NAMES = [['c0', 'c0'], [pa.int16(), pa.int32()]]


@pytest.mark.parametrize(
    ("args", "exception"),
    [
        (NEW_DTYPES_PROVIDED_TWICE, AttributeError),
        (NEW_DTYPES_NOT_PROVIDED, AttributeError),
        (NUMBER_OF_COLS_AND_DTYPES_DONT_MATCH, ValueError),
        (INVALID_COLS_ARGUMENT_DTYPE, TypeError),
        (INVALID_COLS_KEYS_ARGUMENT_DTYPE, TypeError),
        (INVALID_COLS_VALUES_ARGUMENT_DTYPE, TypeError),
        (INVALID_TO_ARGUMENT_DTYPE, TypeError),
        (INVALID_TO_ITEMS_DTYPE, TypeError),
        (FORBIDDEN_INDEX_DTYPE, TypeError),
        (NON_ARROW_DTYPE, TypeError),
        (INVALID_NEW_DTYPE, pa.ArrowInvalid),
        (DUPLICATE_COL_NAMES, IndexError),
    ],
    ids=[
        "NEW_DTYPES_PROVIDED_TWICE",
        "NEW_DTYPES_NOT_PROVIDED",
        "NUMBER_OF_COLS_AND_DTYPES_DONT_MATCH",
        "INVALID_COLS_ARGUMENT_DTYPE",
        "INVALID_COLS_KEYS_ARGUMENT_DTYPE",
        "INVALID_COLS_VALUES_ARGUMENT_DTYPE",
        "INVALID_TO_ARGUMENT_DTYPE",
        "INVALID_TO_ITEMS_DTYPE",
        "FORBIDDEN_INDEX_DTYPE",
        "NON_ARROW_DTYPE",
        "INVALID_NEW_DTYPE",
        "DUPLICATE_COL_NAMES"
    ]
)
def test_can_change_dtype(store, args, exception):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    col_names = args[0]
    dtypes = args[1]
    # Act and Assert
    with pytest.raises(exception):
        table.astype(col_names, to=dtypes)
