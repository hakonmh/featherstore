import pytest
from .fixtures import *

import pandas as pd


@pytest.mark.parametrize("index",
                         [default_index, sorted_datetime_index, sorted_string_index])
@pytest.mark.parametrize("astype", ["arrow", "polars", "pandas"])
def test_append_table(store, index, astype):
    # Arrange
    expected = make_table(index, astype=astype)
    original_df, append_df = split_table(expected, rows={'after': 20}, iloc=True)
    append_df = shuffle_cols(append_df)

    partition_size = get_partition_size(original_df)
    index_name = get_index_name(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, index=index_name)
    # Act
    table.append(append_df, warnings='ignore')
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize("astype", ["polars[series]", "pandas[series]", "arrow"])
def test_append_series(store, astype):
    expected = make_table(astype=astype, cols=1)
    original_df, append_df = split_table(expected, rows={'after': 20})
    partition_size = get_partition_size(original_df)

    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    table.append(append_df)
    # Assert
    assert_table_equals(table, expected)


def test_store_append_table(store):
    expected = make_table(default_index, astype='pandas')
    original_df, append_df = split_table(expected, rows={'after': 20})

    store.write_table(TABLE_NAME, original_df)
    # Act
    store.append_table(TABLE_NAME, append_df, warnings='ignore')
    # Assert
    df = store.read_pandas(TABLE_NAME)
    assert_df_equals(df, expected)


def test_append_custom_values_to_default_index(store):
    df = make_table(default_index, astype='pandas')
    original_df, extra_df = split_table(df, rows={'after': 20})
    append_df1, append_df2 = split_table(extra_df, rows={'after': 25})
    append_df2.index = [29, 27, 37, 33, 31]

    expected = pd.concat([original_df, append_df1, append_df2])
    expected = sort_table(expected)
    expected = convert_table(expected, to='arrow')
    expected = format_arrow_table(expected)

    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    table.append(append_df1, warnings='ignore')
    table.append(append_df2, warnings='ignore')
    # Assert
    assert_table_equals(table, expected)


def _non_matching_index_dtype():
    df = make_table(sorted_string_index, astype="pandas")
    return df


def _non_matching_column_dtypes():
    df = make_table(rows=15, cols=5, astype="pandas")
    df.columns = ['c2', 'c4', 'c3', 'c0', 'c1']
    df = df[['c0', 'c1', 'c2', 'c3', 'c4']]
    df = df.tail(5)
    return df


def _index_not_ordered_after_stored_data():
    df = make_table(rows=5, astype="pandas")
    df.index = [-5, -4, -3, -2, -1]
    return df


def _index_value_already_in_stored_data():
    df = make_table(rows=3, astype="pandas")
    df.index = [9, 10, 11]
    return df


def _column_name_not_in_stored_data():
    df = make_table(cols=5, astype="pandas")
    df = df.tail(5)
    df.columns = ['c0', 'c1', 'c2', 'c3', 'invalid_col_name']
    return df


def _index_name_not_the_same_as_stored_index():
    df = make_table(astype="pandas")
    df = df.tail(5)
    df.index.name = 'new_index_name'
    return df


def _duplicate_index_values():
    df = make_table(rows=3, astype="pandas")
    df = df.head(5)
    df.index = [11, 12, 12]
    return df


def _duplicate_column_names():
    df = make_table(cols=5, astype="pandas")
    df = df.tail(5)
    df.columns = ['c0', 'c1', 'c2', 'c2', 'c4']
    return df


def _num_cols_doesnt_match():
    df = make_table(cols=1, astype="polars[series]")
    df = df.tail(5)
    return df


@pytest.mark.parametrize(
    ("append_df", "exception"),
    [
        (_non_matching_index_dtype, TypeError),
        (_non_matching_column_dtypes, TypeError),
        (_index_not_ordered_after_stored_data, ValueError),
        (_index_value_already_in_stored_data, ValueError),
        (_column_name_not_in_stored_data, ValueError),
        (_index_name_not_the_same_as_stored_index, ValueError),
        (_duplicate_index_values, IndexError),
        (_duplicate_column_names, IndexError),
        (_num_cols_doesnt_match, ValueError),
    ],
    ids=[
        "_non_matching_index_dtype",
        "_non_matching_column_dtypes",
        "_index_not_ordered_after_stored_data",
        "_index_value_already_in_stored_data",
        "_column_name_not_in_stored_data",
        "_index_name_not_the_same_as_stored_index",
        "_duplicate_index_values",
        "_duplicate_column_names",
        "_num_cols_doesnt_match",
    ],
)
def test_can_append_table(store, append_df, exception):
    # Arrange
    append_df = append_df()
    original_df = make_table(rows=10, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.append(append_df)
