import pytest
from .fixtures import *


@pytest.mark.parametrize(
    ["index", "rows", "cols", "astype"],
    [
        (fake_default_index, [0, 5, 12], ['c0', 'c4', 'c2'], 'polars'),
        (fake_default_index, {'before': 12}, {"like": "c?"}, 'arrow'),
        (sorted_string_index, {"between": ['a', 'f']}, {"like": "c?"}, 'polars'),
        (continuous_datetime_index, ["2021-01-07", "2021-01-20"], None, 'arrow'),
    ]
)
def test_polars_arrow_filtering(store, index, rows, cols, astype):
    # Arrange
    original_df = make_table(index, astype=astype)
    index_name = get_index_name(original_df)
    _, expected = split_table(original_df, rows=rows, cols=cols, index_name=index_name, keep_index=True)
    if index == fake_default_index:
        original_df = original_df.drop([DEFAULT_ARROW_INDEX_NAME])
        index_name = None

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size,
                warnings='ignore')
    # Assert
    assert_table_equals(table, expected, rows=rows, cols=cols)


@pytest.mark.parametrize("astype", ['polars', 'arrow'])
def test_polars_and_arrow_to_pandas(store, astype):
    # Arrange
    original_df = make_table(astype=astype, cols=4)
    expected = convert_table(original_df, to='pandas')

    index_name = get_index_name(original_df)
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size)
    df = table.read_pandas()
    # Assert
    assert_df_equals(df, expected)
