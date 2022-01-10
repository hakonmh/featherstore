import pytest
from .fixtures import *


def _wrong_df_type():
    return make_table(cols=1, astype='arrow')


def _col_name_already_in_table():
    return make_table(cols=2, astype='pandas')


def _forbidden_col_name():
    df = make_table(cols=1, astype='pandas')
    df.columns = ['like']
    return df


def _wrong_index_dtype():
    df = make_table(index=sorted_string_index, cols=2, astype='pandas')
    df.columns = ['new_c1', 'new_c2']
    return df


def _num_rows_dont_match():
    df = make_table(rows=42, cols=1, astype='pandas')
    df.columns = ['new_c1']
    return df


def _wrong_index_values():
    df = make_table(index=unsorted_int_index, cols=1, astype='pandas')
    df.columns = ['new_c1']
    return df


@pytest.mark.parametrize(
    ("df", "exception"),
    [
        (_wrong_df_type(), TypeError),
        (_col_name_already_in_table(), IndexError),
        (_forbidden_col_name(), IndexError),
        (_wrong_index_dtype(), TypeError),
        (_num_rows_dont_match(), IndexError),
        (_wrong_index_values(), ValueError),
    ],
    ids=[
        "_wrong_df_type",
        "_col_name_already_in_table",
        "_forbidden_col_name",
        "_wrong_index_dtype",
        "_num_rows_dont_match",
        "_wrong_index_values"
    ]
)
def test_can_add_cols(df, exception, basic_data, database,
                      connection, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(basic_data["table_name"])
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.add_columns(df)
    # Assert
    assert isinstance(e.type(), exception)
