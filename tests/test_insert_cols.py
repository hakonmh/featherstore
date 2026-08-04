import warnings

import pytest
from .fixtures import *


@pytest.mark.parametrize(["index", "col_names", "col_idx"],
                         [[unsorted_int_index, ['n0', 'n1'], 3],
                          [continuous_datetime_index, ['n0'], -1],
                          [unsorted_string_index, ['n0', 'n1'], -1],
                          [default_index, ['n0'], 0]
                          ]
                         )
@pytest.mark.parametrize("astype", ["pandas", "pandas[series]", "polars", "arrow"])
def test_insert_cols(store, index, col_names, col_idx, astype):
    if astype == "pandas[series]" and len(col_names) != 1:
        pytest.skip("Series input requires a single column")

    # Arrange
    num_cols = 5 + len(col_names)
    df = make_table(index=index, cols=num_cols, astype="pandas")
    expected_pd = _change_cols(df, col_names, col_idx)
    expected_pd = sort_table(expected_pd)
    original_pd, new_cols_pd = split_table(expected_pd, cols=col_names)
    original_df, new_cols, expected = convert_col_edit_tables(
        original_pd, new_cols_pd, expected_pd, astype=astype)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore',
                index=write_index_for_astype(expected_pd, astype,
                                             original_df=original_df))
    # Act
    table.insert_columns(new_cols, idx=col_idx, warnings='ignore')
    # Assert
    assert_table_equals(table, expected)


def _change_cols(df, col_names, col_idx):
    cols = df.columns.tolist()
    end = col_idx + len(col_names)
    if col_idx < 0:
        col_idx = -len(col_names)
        end = None
    cols[col_idx:end] = col_names
    df.columns = cols
    return df


@pytest.mark.parametrize("warnings_arg", ["warn", "ignore"])
def test_insert_cols_unsorted_index_warning(store, warnings_arg):
    # Arrange
    df = make_table(cols=6, astype="pandas")
    expected = _change_cols(df.copy(), ['n0'], -1)
    original_df, new_cols = split_table(expected, cols=['n0'])
    new_cols = new_cols.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings='ignore')
    # Act and Assert
    if warnings_arg == "warn":
        with pytest.warns(UserWarning, match="unsorted"):
            table.insert_columns(new_cols, warnings=warnings_arg)
    else:
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            table.insert_columns(new_cols, warnings=warnings_arg)


def _wrong_table_type():
    return make_table(cols=1, astype='polars[series]').rename('new_c1')


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
    ("insert_cols_df", "exception"),
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
        "_non_matching_index_values",
    ],
)
def test_can_insert_cols(store, insert_cols_df, exception):
    # Arrange
    insert_cols_df = insert_cols_df()
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.insert_columns(insert_cols_df)
