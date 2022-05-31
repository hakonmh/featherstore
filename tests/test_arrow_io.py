import pytest
from .fixtures import *


@pytest.mark.parametrize("index",
                         [default_index, sorted_datetime_index, sorted_string_index,
                          unsorted_int_index, unsorted_datetime_index,
                          unsorted_string_index])
@pytest.mark.parametrize("partition_size", [None, -1])
def test_arrow_io(index, partition_size, store):
    original_df = make_table(index, astype='arrow')
    expected = _sort_df(original_df)

    index_name = get_index_name(original_df)
    partition_size = _get_partition_size(original_df, partition_size)
    store.write_table(TABLE_NAME,
                      original_df,
                      partition_size=partition_size,
                      warnings="ignore",
                      index=index_name)
    # Act
    df = store.read_arrow(TABLE_NAME)
    # Assert
    assert df.equals(expected)


def _get_partition_size(df, partition_size):
    if partition_size is None:
        partition_size = get_partition_size(df)
    return partition_size


def _sort_df(df):
    index_name = get_index_name(df)
    if index_name:
        sorted_index = pa.compute.sort_indices(df[index_name])
        df = df.take(sorted_index)
    return df
