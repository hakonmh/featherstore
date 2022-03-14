import pytest
from .fixtures import *


@pytest.mark.parametrize(["num_partitions", "rows"], [(7, 30), (3, 125), (27, 36)])
def test_update_table(num_partitions, rows, store):
    # Arrange
    INDEX_NAME = 'index'
    original_df = make_table(rows=rows, cols=5, astype="pandas")
    original_df.index.name = INDEX_NAME

    ROW_INDICES = [10, 13, 14, 21]
    COL_INDICES = [3, 1]
    update_df = make_table(rows=rows, cols=5, astype='pandas')
    update_df = update_df.iloc[ROW_INDICES, COL_INDICES]
    expected = original_df.copy()
    expected.iloc[ROW_INDICES, COL_INDICES] = update_df

    partition_size = get_partition_size(original_df,
                                        num_partitions=num_partitions)
    store.write_table(TABLE_NAME,
                      original_df,
                      partition_size=partition_size)
    table = store.select_table(TABLE_NAME)
    partition_names = table._partition_data.keys()
    partition_data = table._partition_data.read()

    # Act
    table.update(update_df)

    # Assert
    df = store.read_pandas(TABLE_NAME)
    arrow_df = store.read_arrow(TABLE_NAME)
    arrow_index = arrow_df[INDEX_NAME]
    # Check that partitions keep the same tructure after update
    for partition, partition_name in zip(arrow_index.chunks, partition_names):
        metadata = partition_data[partition_name]
        index_start = partition[0].as_py()
        index_end = partition[-1].as_py()

        assert index_start == int(metadata['min'])
        assert index_end == int(metadata['max'])
        assert len(partition) == metadata['num_rows']

    assert df.equals(expected)
    assert not df.equals(original_df)


@pytest.mark.parametrize(["index", "rows"],
                         [(None, [10, 13, 14, 21]),
                          (hardcoded_string_index, ["row00010", "row00013",
                                                    "row00014", "row00021"]),
                          (hardcoded_datetime_index, ["2021-01-01", "2021-01-16",
                                                      "2021-01-07"])
                          ]
                         )
def test_update_table_with_pandas_series(index, rows, store):
    # Arrange
    original_df = make_table(index=index, cols=5, astype='pandas')
    store.write_table(TABLE_NAME, original_df)

    update_df = make_table(index=index, cols=1, astype='pandas')
    update_series = update_df.squeeze()
    update_series = update_series[rows]

    expected = original_df.copy()
    expected.loc[rows, 'c0'] = update_series

    table = store.select_table(TABLE_NAME)
    # Act
    table.update(update_series)
    # Assert
    df = store.read_pandas(TABLE_NAME)
    assert df.equals(expected)
    assert not df.equals(original_df)


def _wrong_index_dtype():
    df = make_table(sorted_datetime_index, astype="pandas")
    return df


def _wrong_index_values():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 459]
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 10]
    return df


def _wrong_column_dtype():
    df = make_table(sorted_string_index, cols=1, astype="pandas")
    df = df.reset_index()
    df.columns = ['c1', 'c2']
    df = df.head(5)
    return df


def _wrong_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c1', 'non-existant_column']
    return df


def _duplicate_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c2', 'c2']
    return df


@pytest.mark.parametrize(
    ("update_df", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_wrong_index_values(), ValueError),
        (_duplicate_index_values(), IndexError),
        (_wrong_column_dtype(), TypeError),
        (_wrong_column_names(), IndexError),
        (_duplicate_column_names(), IndexError),
    ],
    ids=[
        "_wrong_index_dtype",
        "_wrong_index_values",
        "_duplicate_index_values",
        "_wrong_column_dtype",
        "_wrong_column_names",
        "_duplicate_column_names",
    ],
)
def test_can_update_table(update_df, exception, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    store.write_table(TABLE_NAME, original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    with pytest.raises(exception) as e:
        table.update(update_df)
    # Assert
    assert isinstance(e.type(), exception)
