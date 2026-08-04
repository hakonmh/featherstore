import pytest
from .fixtures import *

import warnings
import pandas as pd


DROPPED_ROWS_INDICES = [2, 5, 7, 10]


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
@pytest.mark.parametrize("astype", ["pandas", "pandas[series]", "polars", "arrow"])
def test_insert_table(store, index, row_indices, num_rows, num_cols, num_partitions,
                      astype):
    if astype == "pandas[series]" and num_cols != 1:
        pytest.skip("Series input requires a single column")

    expected_pd = make_table(index, rows=num_rows, cols=num_cols, astype='pandas')
    original_pd, insert_pd = split_table(expected_pd, rows=row_indices)
    expected_pd = sort_table(expected_pd)

    original_df, insert_df, expected = _rows_to_backend(
        original_pd, insert_pd, expected_pd, astype=astype
    )

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    write_index = None if astype.startswith('pandas') else (
        expected_pd.index.name or DEFAULT_ARROW_INDEX_NAME
    )
    table.write(original_df, partition_size=partition_size, warnings='ignore',
                index=write_index)
    table.insert_rows(insert_df, warnings='ignore')
    assert_table_equals(table, expected)


def _rows_to_backend(original_pd, insert_pd, expected_pd, *, astype):
    import pyarrow as pa

    backend = astype.split('[')[0]
    as_series = '[series]' in astype
    squeeze = as_series or expected_pd.shape[1] == 1

    if backend == 'pandas':
        original_df = original_pd.squeeze(axis=1) if squeeze else original_pd
        insert_df = insert_pd.squeeze(axis=1) if (
            as_series or insert_pd.shape[1] == 1
        ) else insert_pd
        expected = expected_pd.squeeze(axis=1) if squeeze else expected_pd
        return original_df, insert_df, expected

    index_name = expected_pd.index.name or DEFAULT_ARROW_INDEX_NAME

    def _with_index(pdf):
        arrow = convert_table(pdf, to='arrow')
        if index_name not in arrow.column_names:
            arrow = arrow.add_column(0, index_name, pa.array(pdf.index))
        return format_arrow_table(arrow)

    original_df = _with_index(original_pd)
    insert_df = _with_index(insert_pd)
    expected = _with_index(expected_pd)
    if backend == 'polars':
        original_df = convert_table(original_df, to='polars', as_series=False)
        insert_df = convert_table(insert_df, to='polars', as_series=False)
        expected = convert_table(expected, to='polars', as_series=False)
    return original_df, insert_df, expected


@pytest.mark.parametrize("row_indices", ([-2, -1], [30, 33], [33, 30, 32, 31]))
def test_default_index_behavior_when_inserting(store, row_indices):
    # Arrange
    original_df = make_table(default_index, rows=30, astype='pandas')
    insert_df = make_table(default_index, rows=len(row_indices), astype='pandas')
    insert_df.index = row_indices

    expected = _insert(original_df, insert_df)

    partition_size = get_partition_size(original_df, 5)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.insert_rows(insert_df, warnings='ignore')
    # Assert
    assert_table_equals(table, expected)


def _insert(df, other):
    new_df = pd.concat([df, other])
    new_df = sort_table(new_df)
    new_df = convert_table(new_df, to='arrow')
    new_df = format_arrow_table(new_df)
    return new_df


def _insert_table_not_supported_type():
    return make_table(astype="polars[series]")


def _non_matching_index_dtype():
    df = make_table(sorted_string_index, astype="pandas")
    return df


def _non_matching_column_dtypes():
    df = make_table(dtype='string', astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    return df


def _index_values_already_in_stored_data():
    df = make_table(astype="pandas")
    return df


def _column_name_not_in_stored_data():
    df = make_table(cols=2, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ['c1', 'non-existant_column']
    return df


def _index_name_not_the_same_as_stored_index():
    df = make_table(astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.index.name = 'new_index_name'
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df = pd.concat([df, df])
    return df


def _duplicate_column_names():
    df = make_table(cols=6, astype="pandas")
    df = df.iloc[DROPPED_ROWS_INDICES, :]
    df.columns = ['c0', 'c0', 'c1', 'c2', 'c3', 'c4']
    return df


@pytest.mark.parametrize(
    ("insert_df", "exception"),
    [
        (_insert_table_not_supported_type, TypeError),
        (_non_matching_index_dtype, TypeError),
        (_non_matching_column_dtypes, TypeError),
        (_index_values_already_in_stored_data, ValueError),
        (_column_name_not_in_stored_data, ValueError),
        (_index_name_not_the_same_as_stored_index, ValueError),
        (_duplicate_index_values, IndexError),
        (_duplicate_column_names, IndexError),
    ],
    ids=[
        "_insert_table_not_supported_type",
        "_non_matching_index_dtype",
        "_non_matching_column_dtypes",
        "_index_values_already_in_stored_data",
        "_column_name_not_in_stored_data",
        "_index_name_not_the_same_as_stored_index",
        "_duplicate_index_values",
        "_duplicate_column_names",
    ],
)
def test_can_insert_rows(store, insert_df, exception):
    # Arrange
    insert_df = insert_df()
    original_df = make_table(cols=5, astype='pandas')
    original_df = original_df.drop(index=DROPPED_ROWS_INDICES)
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.insert_rows(insert_df)


def test_insert_rows_warns_on_unsorted_index(store):
    expected = make_table(rows=30, cols=3, astype='pandas')
    original_df, insert_df = split_table(expected, rows=[4, 1, 7])
    insert_df = insert_df.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings='ignore')
    with pytest.warns(UserWarning, match="unsorted"):
        table.insert_rows(insert_df, warnings='warn')
