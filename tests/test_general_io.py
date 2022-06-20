import pytest
from .fixtures import *

import numpy as np


def _invalid_table_type():
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


def _forbidden_col_name():
    df = make_table(cols=1, astype='pandas')
    df.columns = ['like']

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
    kwargs = dict(partition_size='abcd')
    return args, kwargs


@pytest.mark.parametrize(
    ("arguments", "exception"),
    [
        (_invalid_table_type(), TypeError),
        (_invalid_index_dtype(), TypeError),
        (_duplicate_index(), IndexError),
        (_index_not_in_cols(), IndexError),
        (_invalid_col_names_dtype(), TypeError),
        (_forbidden_col_name(), ValueError),
        (_duplicate_col_names(), IndexError),
        (_invalid_warnings_arg(), ValueError),
        (_invalid_errors_arg(), ValueError),
        (_invalid_partition_size_dtype(), TypeError),
    ],
    ids=[
        "_invalid_table_type",
        "_invalid_index_dtype",
        "_duplicate_index",
        "_index_not_in_cols",
        "_invalid_col_names_dtype",
        "_forbidden_col_name",
        "_duplicate_col_names",
        "_invalid_warnings_arg",
        "_invalid_errors_arg",
        "_invalid_partition_size_dtype",
    ],
)
def test_can_write(store, arguments, exception):
    # Arrange
    arguments, kwargs = arguments
    # Act
    with pytest.raises(exception) as e:
        store.write_table(*arguments, **kwargs)
    # Assert
    assert isinstance(e.type(), exception)


def test_trying_to_overwrite_existing_table(store):
    # Arrange
    EXCEPTION = FileExistsError
    original_df = make_table()
    store.write_table(TABLE_NAME, make_table())
    table = store.select_table(TABLE_NAME)
    # Act
    with pytest.raises(EXCEPTION)as e:
        table.write(original_df, errors='raise')
    # Assert
    assert isinstance(e.type(), EXCEPTION)


def test_overwriting_existing_table(store):
    # Arrange
    original_df = make_table()
    store.write_table(TABLE_NAME, make_table())
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, errors='ignore')
    # Assert
    df = table.read_arrow()
    assert df.equals(original_df)


def _invalid_table_name_dtype():
    return 21, dict()


def _invalid_row_dtype():
    return TABLE_NAME, {'rows': 14}


def _invalid_row_elements_dtype():
    return TABLE_NAME, {'rows': [5, 'ab', 7.13]}


def _rows_not_in_table():
    return TABLE_NAME, {'rows': [0, 1, 3334]}


def _invalid_col_dtype():
    return TABLE_NAME, {'cols': 14}


def _invalid_col_elements_dtype():
    return TABLE_NAME, {'cols': ['c1', 7.13]}


def _cols_not_in_table():
    return TABLE_NAME, {'cols': ['c0', 'c1', 'c3334']}


@pytest.mark.parametrize(
    ("arguments", "exception"),
    [
        (_invalid_table_name_dtype(), TypeError),
        (_invalid_row_dtype(), TypeError),
        (_invalid_row_elements_dtype(), TypeError),
        (_rows_not_in_table(), IndexError),
        (_invalid_col_dtype(), TypeError),
        (_invalid_col_elements_dtype(), TypeError),
        (_cols_not_in_table(), IndexError),
    ],
    ids=[
        "_invalid_table_name_dtype",
        "_invalid_row_dtype",
        "_invalid_row_elements_dtype",
        "_rows_not_in_table",
        "_invalid_col_dtype",
        "_invalid_col_elements_dtype",
        "_cols_not_in_table",
    ],
)
def test_can_read(store, arguments, exception):
    # Arrange
    table_name, kwargs = arguments
    df = make_table()
    store.write_table(TABLE_NAME, df)
    # Act
    with pytest.raises(exception) as e:
        store.read_pandas(table_name, **kwargs)
    # Assert
    assert isinstance(e.type(), exception)


def test_read_when_no_table_exists(store):
    table = store.select_table(TABLE_NAME)
    EXCEPTION = FileNotFoundError
    # Act
    with pytest.raises(EXCEPTION) as e:
        table.read_pandas()
    # Assert
    assert not table.exists()
    assert isinstance(e.type(), EXCEPTION)
