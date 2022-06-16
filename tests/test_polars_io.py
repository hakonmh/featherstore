import pytest
from .fixtures import *


@pytest.mark.parametrize("index",
                         [default_index, sorted_datetime_index, sorted_string_index,
                          unsorted_int_index, unsorted_datetime_index,
                          unsorted_string_index])
def test_polars_io(store, index):
    # Arrange
    original_df = make_table(index, astype="polars")
    index_name = get_index_name(original_df)
    expected = _sort_polars_table(original_df, by=index_name)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size,
                warnings='ignore')
    df = table.read_polars()
    # Assert
    assert df.frame_equal(expected)


def _sort_polars_table(df, *, by):
    index_name = by
    if index_name:
        df = df.sort(by=index_name)
    return df


def test_polars_to_pandas(store):
    # Arrange
    original_df = make_table(astype="polars", cols=4)
    expected = _make_pd_table(original_df)

    index_name = get_index_name(original_df)
    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size)
    df = table.read_pandas()
    # Assert
    assert df.equals(expected)


def _make_pd_table(df):
    expected = df.to_pandas()
    expected = expected.astype({'c0': 'string'})
    return expected
