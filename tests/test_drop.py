import pytest
from .fixtures import *


def _wrong_index_dtype():
    return ['3', '19', '25']


def _wrong_index_values():
    return [2, 5, 7, 10, 459]


def _drop_all_values():
    return list(pd.RangeIndex(0, 30))


@pytest.mark.parametrize(
    ("rows", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_wrong_index_values(), ValueError),
        (_drop_all_values(), IndexError),
    ],
    ids=[
        "_wrong_index_dtype",
        "_wrong_index_values",
        "_drop_all_values"
    ],
)
def test_can_drop_rows_from_table(rows, exception, basic_data, database,
                                  connection, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(basic_data["table_name"])
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.drop(rows=rows)
    # Assert
    assert isinstance(e.type(), exception)


@pytest.mark.parametrize(
    ["rows", 'slice_'],
    [
        # (pd.Index([10, 24, 0, 13]), [10, 24, 0, 13]),
        ([10, 24, 0, 13], [10, 24, 0, 13]),
        (['after', 10], slice(10, 30)),
        (['before', 10], slice(0, 11)),
        (['between', 13, 16], slice(13, 17))
    ])
def test_drop_rows_from_int_indexed_table(rows, slice_, basic_data, database, connection, store):
    # Arrange
    original_df = make_table(rows=30, astype="pandas")
    mask = original_df.iloc[slice_, :].index
    expected = original_df.copy().drop(index=mask)

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.drop(rows=rows)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)


@pytest.mark.parametrize(
    ["rows", 'condition'],
    [
        (['row00010', 'row00024', 'row00000'], "['row00010', 'row00024', 'row00000']"),
        (['after', 'row00010'], "'row00010' <= original_df.index"),
        (['before', 'row00010'], "'row00010' >= original_df.index"),
    ])
def test_drop_rows_from_str_indexed_table(rows, condition, basic_data,
                                          database, connection, store):
    # Arrange
    original_df = make_table(hardcoded_string_index, rows=30, astype="pandas")
    mask = original_df.loc[eval(condition)].index
    expected = original_df.copy().drop(index=mask)

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.drop(rows=rows)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)


@pytest.mark.parametrize(
    ["rows", 'condition'],
    [
        (['2021-01-01', '2021-01-17'], "['2021-01-01', '2021-01-17']"),
        (['after', '2021-01-17'], "'2021-01-17' <= original_df.index"),
        (['before', '2021-01-17'], "'2021-01-17' >= original_df.index"),
    ])
def test_drop_rows_from_datetime_indexed_table(rows, condition, basic_data,
                                               database, connection, store):
    # Arrange
    original_df = make_table(sorted_datetime_index, rows=30, astype="pandas")
    mask = original_df.loc[eval(condition)].index
    expected = original_df.copy().drop(index=mask)

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.drop(rows=rows)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)
