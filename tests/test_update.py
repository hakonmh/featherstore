import pytest
from .fixtures import *

import pandas as pd


@pytest.mark.parametrize(
    ["index", "rows", "cols", "num_cols"],
    [
        (default_index, [10, 13, 14, 21], ['c1', 'c3', 'c2'], 5),
        (default_index, None, ['c1', 'c3', 'c2'], 5),
        (default_index, None, ['c0'], 3),
        (continuous_string_index, ["al", "aj", "ba", "af"], ['c0'], 1),
        (continuous_datetime_index, ["2021-01-01", "2021-01-16", "2021-01-07"], ['c0'], 1)
    ]
)
@pytest.mark.parametrize("astype", ["pandas", "pandas[series]", "polars", "arrow"])
def test_update_table(store, index, rows, cols, num_cols, astype):
    if astype == "pandas[series]" and num_cols != 1:
        pytest.skip("Series tables require a single column")

    # Arrange
    original_pd = make_table(index, cols=num_cols, astype="pandas")
    if astype == "pandas[series]":
        original_pd = original_pd.squeeze(axis=1)

    _, update_pd = split_table(original_pd, rows=rows, cols=cols)
    update_pd = update_values(update_pd)
    expected_pd = expected_after_update(original_pd, update_pd)
    original_df, update_df, expected = convert_row_edit_tables(
        original_pd, update_pd, expected_pd, astype=astype, drop_default_index=True)

    table = store.select_table(TABLE_NAME)
    table.write(original_df, index=write_index_for_astype(original_pd, astype,
                                                         original_df=original_df))
    # Act
    table.update(update_df)
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize(["num_partitions", "rows"], [(7, 30), (3, 125), (27, 36)])
def test_partition_structure_after_update_table(store, num_partitions, rows):
    # Arrange
    original_df = make_table(rows=rows, astype='pandas')
    original_df.index.name = 'index'
    _, update_df = split_table(original_df, rows=(10, 13, 14, 21), cols=['c2', 'c0'])
    update_df = update_values(update_df)
    expected = expected_after_update(original_df, update_df)

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    partition_names = table._partition_data.keys()
    partition_data = table._partition_data.read()
    # Act
    table.update(update_df)
    # Assert
    assert_table_equals(table, expected)
    _assert_that_partitions_are_the_same(table, partition_names, partition_data)


def _assert_that_partitions_are_the_same(table, partition_names, partition_data):
    df = table.read_arrow()
    index = df['index']
    for partition, partition_name in zip(index.chunks, partition_names):
        metadata = partition_data[partition_name]

        index_start = partition[0].as_py()
        index_end = partition[-1].as_py()
        num_rows = len(partition)

        assert index_start == metadata['min']
        assert index_end == metadata['max']
        assert num_rows == metadata['num_rows']


def _update_table_not_supported_type():
    return make_table(astype="polars[series]")


def _non_matching_index_dtype():
    df = make_table(sorted_string_index, astype="pandas")
    return df


def _non_matching_column_dtypes():
    df = make_table(sorted_string_index, cols=1, astype="pandas")
    df = df.reset_index()
    df.columns = ['c1', 'c2']
    df = df.head(5)
    return df


def _index_not_in_table():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 459]
    return df


def _column_name_not_in_stored_data():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c1', 'non-existant_column']
    return df


def _index_name_not_the_same_as_stored_index():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index.name = 'new_index_name'
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 10]
    return df


def _duplicate_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c2', 'c2']
    return df


@pytest.mark.parametrize(
    ("update_df", "exception"),
    [
        (_update_table_not_supported_type, TypeError),
        (_non_matching_index_dtype, TypeError),
        (_non_matching_column_dtypes, TypeError),
        (_index_not_in_table, ValueError),
        (_column_name_not_in_stored_data, IndexError),
        (_index_name_not_the_same_as_stored_index, ValueError),
        (_duplicate_index_values, IndexError),
        (_duplicate_column_names, IndexError),
    ],
    ids=[
        "_update_table_not_supported_type",
        "_non_matching_index_dtype",
        "_non_matching_column_dtypes",
        "_index_not_in_table",
        "_column_name_not_in_stored_data",
        "_index_name_not_the_same_as_stored_index",
        "_duplicate_index_values",
        "_duplicate_column_names",
    ],
)
def test_can_update_table(store, update_df, exception):
    # Arrange
    update_df = update_df()
    original_df = make_table(cols=5, astype='pandas')
    store.write_table(TABLE_NAME, original_df)
    table = store.select_table(TABLE_NAME)
    # Act and Assert
    with pytest.raises(exception):
        table.update(update_df)
