
import pytest
from tests.fixtures import *


@pytest.mark.integration
def test_windows_permission_error(store):

    # Arrange
    original_df = make_table(rows=100, astype='arrow')
    update_df = make_table(rows=20, astype='pandas')

    partition_size = get_partition_size(original_df, num_partitions=100)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)
    # Act
    df = table.read_arrow()
    table.update(update_df)
    df1 = table.read_arrow()
    table.drop_table()
    # Assert
    assert df == original_df
    assert df != df1
