import pytest
from .fixtures import *


@pytest.mark.parametrize(["index", "col_names", "col_idx"],
                         [[unsorted_int_index, ['n0', 'n1'], 3],
                          [continuous_datetime_index, ['n0'], -1],
                          [unsorted_string_index, ['n0', 'n1'], -1],
                          [default_index, ['n0'], 0]
                          ]
                         )
def test_add_cols(store, index, col_names, col_idx):
    # Arrange
    num_cols = 5 + len(col_names)
    df = make_table(index=index, cols=num_cols, astype="pandas")
    expected = _change_cols(df, col_names, col_idx)
    original_df, new_cols = split_table(expected, cols=col_names)
    expected = sort_table(expected)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.add_columns(new_cols, idx=col_idx)
    # Assert
    assert_table_equals(table, expected)


def _change_cols(df, col_names, col_idx):
    num_cols = len(col_names)
    cols = df.columns.tolist()
    end = col_idx + num_cols
    if col_idx < 0:
        col_idx = -len(col_names)
        end = None
    cols[col_idx:end] = col_names
    df.columns = cols
    return df


def _wrong_table_type():
    df = make_table(cols=1, astype='arrow')
    df = df.rename_columns(['new_c1'])
    return df


def _col_name_already_in_table():
    return make_table(cols=2, astype='pandas')


def _add_col_named_same_as_index():
    df = make_table(cols=1, astype='pandas')
    df.columns = [DEFAULT_ARROW_INDEX_NAME]
    return df


def _new_cols_contain_duplicate_names():
    df = make_table(cols=2, astype='pandas')
    df.columns = ['new_c1', 'new_c1']
    return df


def _non_matching_index_dtype():
    df = make_table(index=sorted_string_index, cols=2, astype='pandas')
    df.columns = ['new_c1', 'new_c2']
    return df


def _num_rows_doesnt_match():
    df = make_table(rows=42, cols=1, astype='pandas')
    df.columns = ['new_c1']
    return df


def _non_matching_index_values():
    df = make_table(cols=1, astype='pandas')
    df.index += 50
    df.columns = ['new_c1']
    return df


@pytest.mark.parametrize(
    ("add_cols_df", "exception"),
    [
        (_wrong_table_type, TypeError),
        (_col_name_already_in_table, IndexError),
        (_add_col_named_same_as_index, ValueError),
        (_new_cols_contain_duplicate_names, IndexError),
        (_non_matching_index_dtype, TypeError),
        (_num_rows_doesnt_match, IndexError),
        (_non_matching_index_values, ValueError),
    ],
    ids=[
        "_wrong_table_type",
        "_col_name_already_in_table",
        "_add_col_named_same_as_index",
        "_new_cols_contain_duplicate_names",
        "_non_matching_index_dtype",
        "_num_rows_doesnt_match",
        "_non_matching_index_values"
    ]
)
def test_can_add_cols(store, add_cols_df, exception):
    # Arrange
    add_cols_df = add_cols_df()
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.add_columns(add_cols_df)
