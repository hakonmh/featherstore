import pytest
from .fixtures import *


@pytest.mark.parametrize(["index", "num_cols", "col_names", "col_idx"],
                         [[unsorted_int_index, 2, ['n0', 'n1'], 3],
                          [continuous_datetime_index, 1, ['n0'], -1],
                          [default_index, 1, ['n0'], 0]
                          ]
                         )
def test_add_cols(store, index, num_cols, col_names, col_idx):
    # Arrange
    original_df = _make_df(index)
    new_cols = _make_df(index, cols=num_cols, col_names=col_names)
    expected = _add_cols(new_cols, to=original_df, col_idx=col_idx)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.add_columns(new_cols, idx=col_idx)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)


def _make_df(index, rows=30, cols=5, col_names=None):
    df = make_table(index=index, rows=rows, cols=cols, astype="pandas")
    if col_names:
        df.columns = col_names
    return df


def _add_cols(df, *, to, col_idx):
    full_df = to.join(df)

    cols = to.columns.tolist()
    if col_idx != -1:
        for col in reversed(df.columns):
            cols.insert(col_idx, col)
    else:
        cols = full_df.columns

    full_df = full_df[cols]
    full_df = full_df.sort_index()
    return full_df


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


def _num_rows_doesnt_match():
    df = make_table(rows=42, cols=1, astype='pandas')
    df.columns = ['new_c1']
    return df


def _wrong_index_values():
    df = make_table(cols=1, astype='pandas')
    df.index += 50
    df.columns = ['new_c1']
    return df


@pytest.mark.parametrize(
    ("df", "exception"),
    [
        (_wrong_df_type(), TypeError),
        (_col_name_already_in_table(), IndexError),
        (_forbidden_col_name(), ValueError),
        (_wrong_index_dtype(), TypeError),
        (_num_rows_doesnt_match(), IndexError),
        (_wrong_index_values(), ValueError),
    ],
    ids=[
        "_wrong_df_type",
        "_col_name_already_in_table",
        "_forbidden_col_name",
        "_wrong_index_dtype",
        "_num_rows_doesnt_match",
        "_wrong_index_values"
    ]
)
def test_can_add_cols(store, df, exception):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.add_columns(df)
    # Assert
    assert isinstance(e.type(), exception)
