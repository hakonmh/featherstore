import pytest
from .fixtures import *


def _new_names_provided_twice():
    return {'c0': 'd0'}, ['d0']


def _new_names_not_provided():
    return ['c0', 'c1'], None


def _num_cols_doesnt_match():
    return ['c0', 'c1'], ['d0']


def _wrong_col_dtype():
    return ['c0'], [1]


def _col_name_already_in_table():
    return {'c0': 'c1'}, None


def _forbidden_col_name():
    return {'c0': 'like'}, None


def _duplicate_col_names():
    return ['c0', 'c1'], ['d0', 'd0']


@pytest.mark.parametrize(
    ("args", "exception"),
    [
        (_new_names_provided_twice(), AttributeError),
        (_new_names_not_provided(), AttributeError),
        (_num_cols_doesnt_match(), ValueError),
        (_wrong_col_dtype(), TypeError),
        (_col_name_already_in_table(), IndexError),
        (_forbidden_col_name(), ValueError),
        (_duplicate_col_names(), IndexError),
    ],
    ids=[
        "_wrong_signature",
        "_new_names_not_provided",
        "_num_cols_doesnt_match",
        "_wrong_col_dtype",
        "_col_name_already_in_table",
        "_forbidden_col_name",
        "_duplicate_col_names"
    ]
)
def test_can_add_cols(args, exception, basic_data, database,
                      connection, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(basic_data["table_name"])
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        col_names = args[0]
        new_col_names = args[1]
        table.rename_columns(col_names, to=new_col_names)
    # Assert
    assert isinstance(e.type(), exception)


@pytest.mark.parametrize(
    ("columns", "to", "result"),
    [
        (['c0', 'c2'], ['d0', 'd2'], ['d0', 'c1', 'd2', 'c3']),
        ({'c0': 'd0', 'c2': 'd2'}, None, ['d0', 'c1', 'd2', 'c3']),
        (['c2', 'c3'], ['c3', 'c2'], ['c0', 'c1', 'c3', 'c2'])
    ],
)
def test_rename_cols(columns, to, result, basic_data, database, connection, store):
    # Arrange
    original_df = make_table(rows=30, cols=4, astype="pandas")
    expected = original_df.copy()
    expected.columns = result

    store.write_table(basic_data["table_name"], original_df, warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.rename_columns(columns, to=to)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)