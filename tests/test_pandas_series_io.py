import pytest
from .fixtures import *


@pytest.mark.parametrize(
    "original_df",
    [
        make_table(cols=1, astype="pandas"),
        make_table(sorted_datetime_index, cols=1, astype="pandas"),
        make_table(sorted_string_index, cols=1, astype="pandas"),
    ],
    ids=["int index", "datetime index", "string index"],
)
def test_pandas_series_io(store, original_df):
    # Arrange
    original_df = make_table(sorted_string_index, cols=1, astype="pandas")
    original_df = original_df.squeeze()
    partition_size = get_partition_size(original_df)
    store.write_table(TABLE_NAME,
                      original_df,
                      partition_size=partition_size)
    # Act
    df = store.read_pandas(TABLE_NAME)
    # Assert
    assert df.equals(original_df)
