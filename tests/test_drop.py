import pytest
from .fixtures import *


def _wrong_index_dtype():
    return ['3', '19', '25']


def _wrong_index_values():
    return [2, 5, 7, 10, 459]


def _drop_all_rows():
    return list(pd.RangeIndex(0, 30))


@pytest.mark.parametrize(
    ("rows", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_wrong_index_values(), ValueError),
        (_drop_all_rows(), IndexError),
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
        (pd.Index([10, 24, 0, 13]), [10, 24, 0, 13]),
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
    original_df = make_table(hardcoded_datetime_index, rows=30, astype="pandas")
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


def _wrong_cols_format():
    return ('c1', 'c2', 'c3')


def _wrong_col_elements_dtype():
    return ['c1', 2]


def _drop_index():
    return ['c1', 'index']


def _col_not_in_stored_data():
    return ['c1', 'Non-existant col']


def _drop_all_cols():
    return ['like', 'c%']


@pytest.mark.parametrize(
    ("cols", "exception"),
    [
        (_wrong_cols_format(), TypeError),
        (_wrong_col_elements_dtype(), TypeError),
        (_drop_index(), ValueError),
        (_col_not_in_stored_data(), IndexError),
        (_drop_all_cols(), IndexError),
    ],
    ids=[
        "_wrong_cols_format",
        "_wrong_col_elements_dtype",
        "_drop_index",
        "_col_not_in_stored_data",
        "_drop_all_cols"
    ],
)
def test_can_drop_cols_from_table(cols, exception, basic_data, database,
                                  connection, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    original_df.index.name = 'index'
    table = store.select_table(basic_data["table_name"])
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.drop(cols=cols)
    # Assert
    assert isinstance(e.type(), exception)


@pytest.mark.parametrize(
    "cols",
    [
        ['c1', 'c3'],
        ['c0'],
        ['c0', 'c2', 'c3', 'c4']
    ],
)
def test_drop_cols_from_table(cols, basic_data, database,
                              connection, store):
    # Arrange
    original_df = make_table(rows=30, astype="pandas")
    expected = original_df.copy().drop(columns=cols).squeeze()

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.drop(cols=cols)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)


def test_drop_cols_like_pattern_from_table(basic_data, database,
                                           connection, store):
    # Arrange
    original_df = make_table(rows=30, cols=20, astype="pandas")
    dropped_cols = [f"c{x}" for x in range(10)]
    expected = original_df.copy().drop(columns=dropped_cols)
    cols = ['like', 'c?']

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.drop(cols=cols)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)
