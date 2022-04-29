import pytest
from pandas.testing import assert_frame_equal
from .fixtures import *
import pyarrow as pa
from decimal import Decimal
from datetime import date


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
def test_can_change_dtype(args, exception, store):
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


def test_change_dtype_to_float_and_int(store):
    COLS = {'c1': pa.float32(), 'c2': pa.int32()}

    original_df = make_table(rows=60, cols=4, astype="pandas")
    expected = original_df.copy()
    expected = expected.astype({'c1': 'float32', 'c2': 'int32'})

    partition_size = get_partition_size(
        original_df, num_partitions=NUMBER_OF_PARTITIONS)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    table.astype(COLS)
    # Assert
    df = table.read_pandas()
    assert_frame_equal(df, expected, check_dtype=True)


def test_change_dtype_to_decimal(store):
    COLS = ['c1', 'c2']
    DTYPES = [pa.decimal128(5, 5), pa.decimal128(19, 0)]

    original_df = make_table(rows=60, cols=4, astype="pandas")
    expected = original_df.copy()
    expected['c1'] = expected['c1'].apply(_convert_to_decimal, args=[5, 5])
    expected['c2'] = expected['c2'].apply(_convert_to_decimal, args=[19, 0])

    partition_size = get_partition_size(
        original_df, num_partitions=NUMBER_OF_PARTITIONS)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    table.astype(COLS, to=DTYPES)
    # Assert
    df = table.read_pandas()
    assert_frame_equal(df, expected, check_dtype=False)


def _convert_to_decimal(x, precision, scale):
    x = format(x, f'{precision}.{scale}f')
    return Decimal(x)


def test_change_dtype_to_timestamp(store):
    COL = ['c3']
    DTYPE = [pa.date64()]

    original_df = make_table(rows=60, cols=4, astype="arrow")
    expected = _convert_to_date64(original_df, col_idx=3)

    partition_size = get_partition_size(
        original_df, num_partitions=NUMBER_OF_PARTITIONS)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    table.astype(COL, to=DTYPE)
    # Assert
    df = table.read_arrow()
    assert df.equals(expected)


def _convert_to_date64(df, col_idx):
    schema = df.schema
    field = schema.field(col_idx)
    field = field.with_type(pa.date64())
    schema = schema.set(col_idx, field)
    df = df.cast(schema)
    return df
