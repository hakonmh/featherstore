import pytest
from .fixtures import *


def _wrong_df_type():
    return make_table(cols=1, astype='arrow')


def _col_name_already_in_table():
    return make_table(cols=2, astype='pandas')


def _forbidden_col_name():
    df = make_table(cols=1, astype='pandas')
    df.columns = ['like']
    return df


def _wrong_index_dtype():
    df = make_table(index=sorted_string_index, cols=2, astype='pandas')
    df.columns = ['new_c1', 'new_c2']
    return df


def _num_rows_doesnt_match():
    df = make_table(rows=42, cols=1, astype='pandas')
    df.columns = ['new_c1']
    return df


def _wrong_index_values():
    df = make_table(cols=1, astype='pandas')
    df.index += 50
    df.columns = ['new_c1']
    return df


@pytest.mark.parametrize(
    ("df", "exception"),
    [
        (_wrong_df_type(), TypeError),
        (_col_name_already_in_table(), IndexError),
        (_forbidden_col_name(), IndexError),
        (_wrong_index_dtype(), TypeError),
        (_num_rows_doesnt_match(), IndexError),
        (_wrong_index_values(), ValueError),
    ],
    ids=[
        "_wrong_df_type",
        "_col_name_already_in_table",
        "_forbidden_col_name",
        "_wrong_index_dtype",
        "_num_rows_doesnt_match",
        "_wrong_index_values"
    ]
)
def test_can_add_cols(df, exception, basic_data, database,
                      connection, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(basic_data["table_name"])
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.add_columns(df)
    # Assert
    assert isinstance(e.type(), exception)


def test_add_cols(basic_data, database, connection, store):
    # Arrange
    original_df = make_table(rows=30, cols=2, astype="pandas")
    new_df = make_table(unsorted_int_index, rows=30, cols=2, astype='pandas')
    new_df.columns = ['n0', 'n1']
    cols = ['c0', 'n0', 'n1', 'c1']
    expected = original_df.join(new_df)
    expected = expected[cols]

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.add_columns(new_df, idx=1)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)


def test_append_col(basic_data, database, connection, store):
    # Arrange
    original_df = make_table(sorted_datetime_index, rows=30, cols=2, astype="pandas")
    new_df = make_table(sorted_datetime_index, rows=30, cols=1, astype='pandas')
    new_df.columns = ['n0']
    expected = original_df.join(new_df)

    partition_size = get_partition_size(
        original_df, num_partitions=basic_data['num_partitions'])
    store.write_table(basic_data["table_name"],
                      original_df,
                      partition_size=partition_size,
                      warnings='ignore')
    table = store.select_table(basic_data["table_name"])
    # Act
    table.add_columns(new_df, idx=-1)
    # Assert
    df = store.read_pandas(basic_data["table_name"])
    assert df.equals(expected)
