import pytest
from .fixtures import *

import pandas as pd


@pytest.mark.parametrize(["index", "row_indices"],
                         [[default_index, [4, 1, 7, 8, 3]],
                          [unsorted_int_index, [0, 1, 2, 3, 4]],
                          [continuous_string_index, ['ab', 'al']],
                          [continuous_datetime_index, ['2021-01-10', '2021-01-14']]]
                         )
@pytest.mark.parametrize(["num_rows", "num_cols", "num_partitions"],
                         [[75, 5, 5],
                          [75, 5, 30],
                          [30, 1, 1],
                          [30, 1, 5]]
                         )
def test_insert_rows(store, index, row_indices, num_rows, num_cols, num_partitions):
    # Arrange
    expected = make_table(index, rows=num_rows, cols=num_cols,
                          astype='pandas[series]')
    original_df, insert_df = split_table(expected, rows=row_indices)
    expected = sort_table(expected)

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(insert_df)
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize(["index", "col_names", "col_idx"],
                         [[unsorted_int_index, ['n0', 'n1'], 3],
                          [continuous_datetime_index, ['n0'], -1],
                          [unsorted_string_index, ['n0', 'n1'], -1],
                          [default_index, ['n0'], 0]
                          ]
                         )
def test_insert_columns(store, index, col_names, col_idx):
    # Arrange
    num_cols = 5 + len(col_names)
    df = make_table(index=index, cols=num_cols, astype="pandas")
    expected = _change_cols(df, col_names, col_idx)
    original_df, new_cols = split_table(expected, cols=col_names)
    expected = sort_table(expected)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(new_cols, idx=col_idx)
    # Assert
    assert_table_equals(table, expected)


def _change_cols(df, col_names, col_idx):
    num_cols = len(col_names)
    cols = df.columns.tolist()
    end = col_idx + num_cols
    if col_idx < 0:
        col_idx = -len(col_names)
        end = None
    cols[col_idx:end] = col_names
    df.columns = cols
    return df


@pytest.mark.parametrize(["index", "col_names", "col_indices"],
                         [[unsorted_int_index, ['n0', 'n1'], [1, 3]],
                          [default_index, ['n0', 'n1', 'n2'], [0, 2, 4]],
                          ]
                         )
def test_insert_columns_with_idx_sequence(store, index, col_names, col_indices):
    # Arrange
    num_cols = 5 + len(col_names)
    df = make_table(index=index, cols=num_cols, astype="pandas")
    expected = _build_expected_with_col_positions(df, col_names, col_indices)
    original_df, new_cols = split_table(expected, cols=col_names)
    expected = sort_table(expected)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(new_cols, idx=col_indices)
    # Assert
    assert_table_equals(table, expected)


def _build_expected_with_col_positions(df, col_names, col_indices):
    cols = df.columns.tolist()
    new_col_source = cols[-len(col_names):]
    base_cols = cols[:-len(col_names)]
    df = df.rename(columns=dict(zip(new_col_source, col_names)))
    expected = df[base_cols].copy()
    for col_name, col_idx in zip(col_names, col_indices):
        expected.insert(col_idx, col_name, df[col_name])
    return expected


@pytest.mark.parametrize("row_indices", ([-2, -1], [30, 33], [33, 30, 32, 31]))
def test_insert_ignores_idx_when_inserting_rows(store, row_indices):
    # Arrange
    original_df = make_table(default_index, rows=30, astype='pandas')
    insert_df = make_table(default_index, rows=len(row_indices), astype='pandas')
    insert_df.index = row_indices

    expected = _insert(original_df, insert_df)

    partition_size = get_partition_size(original_df, 5)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert(insert_df, idx=0)
    # Assert
    assert_table_equals(table, expected)


def _insert(df, other):
    new_df = pd.concat([df, other])
    new_df = sort_table(new_df)
    new_df = convert_table(new_df, to='arrow')
    new_df = format_arrow_table(new_df)
    return new_df


def _two_new_cols():
    df = make_table(cols=2, astype='pandas')
    df.columns = ['new_c0', 'new_c1']
    return df


@pytest.mark.parametrize(
    ("insert_cols_df", "idx", "exception"),
    [
        (_two_new_cols, [0], ValueError),
        (_two_new_cols, [0, 1, 2], ValueError),
    ],
    ids=[
        "_idx_too_short",
        "_idx_too_long",
    ]
)
def test_can_insert_with_invalid_idx(store, insert_cols_df, idx, exception):
    # Arrange
    insert_cols_df = insert_cols_df()
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.insert(insert_cols_df, idx=idx)
