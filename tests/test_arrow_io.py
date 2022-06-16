import pytest
from .fixtures import *


@pytest.mark.parametrize("index",
                         [default_index, sorted_datetime_index, sorted_string_index,
                          unsorted_int_index, unsorted_datetime_index,
                          unsorted_string_index])
@pytest.mark.parametrize("partition_size", [None, -1])
def test_arrow_io(store, index, partition_size):
    original_df = make_table(index, astype='arrow')
    index_name = get_index_name(original_df)
    expected = _sort_arrow_table(original_df, by=index_name)

    partition_size = _get_partition_size(original_df, partition_size)
    table = store.select_table(TABLE_NAME)
    # Act
    table.write(original_df, index=index_name, partition_size=partition_size,
                warnings='ignore')
    df = table.read_arrow()
    # Assert
    assert df.equals(expected)


def _sort_arrow_table(df, *, by):
    index_name = by
    if index_name:
        sorted_index = pa.compute.sort_indices(df[index_name])
        df = df.take(sorted_index)
    return df


def _get_partition_size(df, partition_size):
    if partition_size is None:
        partition_size = get_partition_size(df)
    return partition_size
