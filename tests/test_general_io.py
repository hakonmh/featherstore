from contextlib import nullcontext
import pytest
from .fixtures import *

import numpy as np


def _invalid_table_dtype():
    df = make_table(astype='pandas')
    args = [TABLE_NAME, df.values]
    kwargs = dict()
    return args, kwargs


def _invalid_index_dtype():
    df = make_table(astype='pandas')
    index = np.random.random(size=30)
    df = df.set_index(index)

    args = [TABLE_NAME, df]
    kwargs = dict()
    return args, kwargs


def _duplicate_index():
    df = make_table(astype='pandas', rows=15)
    df1 = make_table(astype='pandas', rows=15)
    df = pd.concat([df, df1])

    args = [TABLE_NAME, df]
    kwargs = dict()
    return args, kwargs


def _index_not_in_cols():
    df = make_table(cols=2, astype='polars')
    df.column_names = ['c0', 'c1']

    args = [TABLE_NAME, df]
    kwargs = dict(index='c2')
    return args, kwargs


def _invalid_col_names_dtype():
    df = make_table(astype='pandas')
    df.columns = ['c0', 'c1', 'c2', 3, 'c4']

    args = [TABLE_NAME, df]
    kwargs = dict()
    return args, kwargs


def _duplicate_col_names():
    df = make_table(cols=2, astype='pandas')
    df.columns = ['c0', 'c0']

    args = [TABLE_NAME, df]
    kwargs = dict()
    return args, kwargs


def _invalid_warnings_arg():
    df = make_table()
    args = [TABLE_NAME, df]
    kwargs = dict(warnings='abcd')
    return args, kwargs


def _invalid_errors_arg():
    df = make_table()
    args = [TABLE_NAME, df]
    kwargs = dict(errors='abcd')
    return args, kwargs


def _invalid_partition_size_dtype():
    df = make_table()
    args = [TABLE_NAME, df]
    kwargs = dict(partition_size=3.15)
    return args, kwargs


@pytest.mark.parametrize(
    ("arguments", "exception"),
    [
        (_invalid_table_dtype(), TypeError),
        (_invalid_index_dtype(), TypeError),
        (_duplicate_index(), IndexError),
        (_index_not_in_cols(), IndexError),
        (_invalid_col_names_dtype(), TypeError),
        (_duplicate_col_names(), IndexError),
        (_invalid_warnings_arg(), ValueError),
        (_invalid_errors_arg(), ValueError),
        (_invalid_partition_size_dtype(), TypeError),
    ],
    ids=[
        "_invalid_table_dtype",
        "_invalid_index_dtype",
        "_duplicate_index",
        "_index_not_in_cols",
        "_invalid_col_names_dtype",
        "_duplicate_col_names",
        "_invalid_warnings_arg",
        "_invalid_errors_arg",
        "_invalid_partition_size_dtype",
    ],
)
def test_can_write(store, arguments, exception):
    # Arrange
    arguments, kwargs = arguments
    # Act and Assert
    with pytest.raises(exception):
        store.write_table(*arguments, **kwargs)


@pytest.mark.parametrize(("errors", "exception"),
                         [('raise', pytest.raises(FileExistsError)),
                          ('ignore', nullcontext())]
                         )
def test_trying_to_overwrite_existing_table(store, errors, exception):
    # Arrange
    table = store.select_table(TABLE_NAME)
    table.write(make_table())
    expected = make_table()
    # Act and Assert
    with exception:
        table.write(expected, errors=errors)
        df = table.read_arrow()
        assert df.equals(expected)


INVALID_TABLE_NAME_DTYPE = [21, dict()]
INVALID_ROW_DTYPE = [TABLE_NAME, {'rows': 14}]
ROW_ELEMENTS_NOT_ALL_SAME_DTYPE = [TABLE_NAME, {'rows': [5, 'ab', 7.13]}]
ROWS_NOT_SAME_DTYPE_AS_INDEX = [TABLE_NAME, {'rows': ['index', 'not', 'string']}]
ROWS_NOT_IN_TABLE = [TABLE_NAME, {'rows': [0, 1, 3334]}]
INVALID_COL_DTYPE = [TABLE_NAME, {'cols': 14}]
INVALID_COL_ELEMENTS_DTYPE = [TABLE_NAME, {'cols': ['c1', 'C2', 12]}]
COLS_NOT_IN_TABLE = [TABLE_NAME, {'cols': ['c0', 'c1', 'c3334']}]


@pytest.mark.parametrize(
    ("arguments", "exception"),
    [
        (INVALID_TABLE_NAME_DTYPE, TypeError),
        (INVALID_ROW_DTYPE, TypeError),
        (ROW_ELEMENTS_NOT_ALL_SAME_DTYPE, TypeError),
        (ROWS_NOT_SAME_DTYPE_AS_INDEX, TypeError),
        (ROWS_NOT_IN_TABLE, IndexError),
        (INVALID_COL_DTYPE, TypeError),
        (INVALID_COL_ELEMENTS_DTYPE, TypeError),
        (COLS_NOT_IN_TABLE, IndexError),
    ],
    ids=[
        "INVALID_TABLE_NAME_DTYPE",
        "INVALID_ROW_DTYPE",
        "ROW_ELEMENTS_NOT_ALL_SAME_DTYPE",
        "ROWS_NOT_SAME_DTYPE_AS_INDEX",
        "ROWS_NOT_IN_TABLE",
        "INVALID_COL_DTYPE",
        "INVALID_COL_ELEMENTS_DTYPE",
        "COLS_NOT_IN_TABLE",
    ],
)
def test_can_read(store, arguments, exception):
    # Arrange
    table_name, kwargs = arguments
    df = make_table()
    store.write_table(TABLE_NAME, df)
    # Act and Assert
    with pytest.raises(exception):
        store.read_pandas(table_name, **kwargs)


def test_read_when_no_table_exists(store):
    # Arrange
    table = store.select_table(TABLE_NAME)
    # Act
    with pytest.raises(FileNotFoundError):
        table.read_pandas()
    # Assert
    assert not table.exists()
