from collections import namedtuple
import pytest
from .fixtures import *

import pyarrow as pa
from pandas.testing import assert_frame_equal

TypeMapper = namedtuple('TypeMapper', 'arrow_dtype pandas_dtype')
TYPE_MAP = {
    'float16': TypeMapper(pa.float16(), 'float'),
    'float32': TypeMapper(pa.float32(), 'float'),
    'float64': TypeMapper(pa.float64(), 'float'),
    'decimal128(5, 5)': TypeMapper(pa.decimal128(5, 5), 'float'),
    'decimal128(19, 0)': TypeMapper(pa.decimal128(19, 0), 'float'),
    'int16': TypeMapper(pa.int16(), 'int'),
    'int32': TypeMapper(pa.int32(), 'int'),
    'int64': TypeMapper(pa.int64(), 'int'),
    'uint32': TypeMapper(pa.uint32(), 'uint'),
    'bool': TypeMapper(pa.bool_(), 'bool'),
    'date32': TypeMapper(pa.date32(), 'datetime'),
    'date64': TypeMapper(pa.date64(), 'datetime'),
    'time32': TypeMapper(pa.time32('ms'), 'datetime'),
    'time64': TypeMapper(pa.time64('us'), 'datetime'),
    'timestamp': TypeMapper(pa.timestamp('us'), 'datetime'),
    'string': TypeMapper(pa.string(), 'string'),
    'large_string': TypeMapper(pa.large_string(), 'string'),
    'binary': TypeMapper(pa.binary(), 'string'),
    'large_binary': TypeMapper(pa.large_binary(), 'string'),
}

KWARG_FORMATS = ['dict', 'list']
kwarg_format_idx = 0


@pytest.mark.parametrize(
    ("from_dtype", "to_dtype"),
    [
        ('float32', 'float64'),
        ('float32', 'decimal128(5, 5)'),
        ('int64', 'int32'),
        ('int32', 'float64'),
        ('int32', 'decimal128(19, 0)'),
        ('uint32', 'int32'),
        ('bool', 'int16'),
        ('bool', 'string'),
        ('int64', 'bool'),
        ('date32', 'date64'),
        ('date32', 'timestamp'),
        ('date64', 'date32'),
        ('timestamp', 'date64'),
        ('timestamp', 'time32'),
        ('time32', 'time64'),
        ('binary', 'large_binary'),
        ('binary', 'string'),
        ('string', 'binary'),
        ('large_string', 'string'),
    ]
)
def test_change_dtype(store, from_dtype, to_dtype):
    # Arrange
    original_df = _make_table(dtype=from_dtype, rows=60, cols=3)
    expected = _change_dtype(original_df, to_dtype, cols=['c1', 'c2'])

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    kwargs = _format_kwargs(to_dtype)
    # Act
    table.astype(**kwargs)
    # Assert
    _assert_frame_equal(table, expected)


def _make_table(dtype, rows=30, cols=3):
    pandas_dtype = TYPE_MAP[dtype].pandas_dtype
    df = make_table(rows=rows, cols=cols, astype="arrow", dtype=pandas_dtype)
    df = _change_dtype(df, dtype)
    return df


def _change_dtype(df, to_dtype, cols=None):
    arrow_dtype = TYPE_MAP[to_dtype].arrow_dtype
    schema = df.schema
    cols = cols if cols else schema.names
    for idx, field in enumerate(schema):
        if field.name in cols:
            field = field.with_type(arrow_dtype)
            schema = schema.set(idx, field)
    df = df.cast(schema)
    return df


def _format_kwargs(to_dtype):
    """Selects every other test to format arguments as one of either:
        * cols=['c1', 'c2'], to=[dtype1, dtype2]
        * cols = {'c1': dtype1, 'c2': dtype2}
    """
    global kwarg_format_idx
    kwarg_format_idx = (kwarg_format_idx + 1) % 2
    kwarg_format = KWARG_FORMATS[kwarg_format_idx]

    arrow_dtype = TYPE_MAP[to_dtype].arrow_dtype
    if kwarg_format == 'dict':
        kwargs = {'cols': {'c1': arrow_dtype, 'c2': arrow_dtype}}
    elif kwarg_format == 'list':
        kwargs = {'cols': ['c1', 'c2'], 'to': [arrow_dtype, arrow_dtype]}
    return kwargs


def _assert_frame_equal(table, expected):
    _assert_arrow(table, expected)
    _assert_polars(table, expected)
    _assert_pandas(table, expected)


def _assert_arrow(table, expected):
    df = table.read_arrow()
    assert df.equals(expected)


def _assert_polars(table, expected):
    df = table.read_polars()
    expected = pl.from_arrow(expected)
    assert df.frame_equal(expected)


def _assert_pandas(table, expected):
    df = table.read_pandas()
    expected = expected.to_pandas(date_as_object=False)
    expected = __convert_object_cols_to_string(expected)
    assert_frame_equal(df, expected, check_dtype=True)


def __convert_object_cols_to_string(df):
    for col in df.columns:
        if df[col].dtype.name == 'object':
            if isinstance(df[col][0], str):
                df[col] = df[col].astype('string')
    return df


def _new_dtypes_provided_twice():
    return {'c0': pa.int16()}, [pa.int16()]


def _new_dtypes_not_provided():
    return ['c0', 'c1'], None


def _num_cols_and_num_dtypes_doesnt_match():
    return ['c0', 'c1'], [pa.int16()]


def _wrong_dtypes_dtype():
    return ['c0'], [123]


def _invalid_index_dtype():
    return ['__index_level_0__'], [pa.float32()]


def _invalid_new_dtype():
    return ['c0'], [pa.float32()]


def _duplicate_col_names():
    return ['c0', 'c0'], [pa.int16(), pa.int32()]


@pytest.mark.parametrize(
    ("args", "exception"),
    [
        (_new_dtypes_provided_twice(), AttributeError),
        (_new_dtypes_not_provided(), AttributeError),
        (_num_cols_and_num_dtypes_doesnt_match(), ValueError),
        (_wrong_dtypes_dtype(), TypeError),
        (_invalid_index_dtype(), TypeError),
        (_invalid_new_dtype(), pa.ArrowInvalid),
        (_duplicate_col_names(), IndexError),
    ],
    ids=[
        "_new_dtypes_provided_twice",
        "_new_dtypes_not_provided",
        "_num_cols_and_num_dtypes_doesnt_match",
        "_wrong_dtypes_dtype",
        "_invalid_index_dtype",
        "_invalid_new_dtype",
        "_duplicate_col_names"
    ]
)
def test_can_change_dtype(store, args, exception):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        col_names = args[0]
        dtypes = args[1]
        table.astype(col_names, to=dtypes)
    # Assert
    assert isinstance(e.type(), exception)
