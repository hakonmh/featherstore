import warnings

import pandas as pd
import pyarrow as pa
import pytest

from .fixtures import *


@pytest.mark.parametrize(
    ["index", "rows", "cols", "num_cols"],
    [
        (default_index, [10, 13, 14, 21], ['c1', 'c3', 'c2'], 5),
        (default_index, None, ['c1', 'c3', 'c2'], 5),
        (default_index, None, ['c0'], 3),
        (continuous_string_index, ["al", "aj", "ba", "af"], ['c0'], 1),
        (continuous_datetime_index, ["2021-01-01", "2021-01-16", "2021-01-07"], ['c0'], 1)
    ]
)
@pytest.mark.parametrize("astype", ["pandas", "pandas[series]", "polars", "arrow"])
def test_update_table(store, index, rows, cols, num_cols, astype):
    if astype == "pandas[series]" and num_cols != 1:
        pytest.skip("Series tables require a single column")

    original_pd = make_table(index, cols=num_cols, astype="pandas")
    if astype == "pandas[series]":
        original_pd = original_pd.squeeze(axis=1)

    _, update_pd = split_table(original_pd, rows=rows, cols=cols)
    update_pd = update_values(update_pd)
    expected_pd = update_table(original_pd, update_pd)

    original_df, update_df, expected = _to_backend(
        original_pd, update_pd, expected_pd, astype=astype
    )

    table = store.select_table(TABLE_NAME)
    write_index = None if astype.startswith('pandas') else (
        (original_pd.index.name if not isinstance(original_pd, pd.Series)
         else original_pd.index.name) or DEFAULT_ARROW_INDEX_NAME
    )
    table.write(original_df, index=write_index)
    table.update(update_df)
    assert_table_equals(table, expected)


def _to_backend(original_pd, update_pd, expected_pd, *, astype):
    backend = astype.split('[')[0]
    as_series = '[series]' in astype
    squeeze = as_series or (
        isinstance(expected_pd, pd.Series) or getattr(expected_pd, 'shape', (0, 2))[1] == 1
    )

    if backend == 'pandas':
        if isinstance(update_pd, pd.DataFrame) and (
            isinstance(update_pd, pd.Series) or update_pd.shape[1] == 1
        ):
            update_df = update_pd.squeeze(axis=1)
        else:
            update_df = update_pd
        if isinstance(original_pd, pd.DataFrame) and squeeze:
            original_df = original_pd.squeeze(axis=1)
        else:
            original_df = original_pd
        if isinstance(expected_pd, pd.DataFrame) and squeeze:
            expected = expected_pd.squeeze(axis=1)
        else:
            expected = expected_pd
        return original_df, update_df, expected

    index_name = (
        expected_pd.index.name if hasattr(expected_pd, 'index') else None
    ) or DEFAULT_ARROW_INDEX_NAME

    def _with_index(pdf):
        arrow = convert_table(pdf, to='arrow')
        if index_name not in arrow.column_names:
            arrow = arrow.add_column(0, index_name, pa.array(pdf.index))
        return format_arrow_table(arrow)

    original = _with_index(original_pd if not isinstance(original_pd, pd.Series) else original_pd.to_frame())
    update = _with_index(update_pd if not isinstance(update_pd, pd.Series) else update_pd.to_frame())
    expected = _with_index(expected_pd if not isinstance(expected_pd, pd.Series) else expected_pd.to_frame())
    if df_has_default_index(expected):
        expected = drop_default_index_if_exists(expected)

    if backend == 'polars':
        original = convert_table(original, to='polars', as_series=False)
        update = convert_table(update, to='polars', as_series=False)
        expected = convert_table(expected, to='polars', as_series=False)
    return original, update, expected


def update_table(df, update_df):
    expected = df.copy()
    rows = update_df.index
    if isinstance(df, pd.Series):
        expected.loc[rows] = update_df
    else:
        cols = update_df.columns
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            expected.loc[rows, cols] = update_df
    return expected


@pytest.mark.parametrize(["num_partitions", "rows"], [(7, 30), (3, 125), (27, 36)])
def test_partition_structure_after_update_table(store, num_partitions, rows):
    # Arrange
    original_df = make_table(rows=rows, astype='pandas')
    original_df.index.name = 'index'
    _, update_df = split_table(original_df, rows=(10, 13, 14, 21), cols=['c2', 'c0'])
    update_df = update_values(update_df)
    expected = update_table(original_df, update_df)

    partition_size = get_partition_size(original_df, num_partitions)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size)

    partition_names = table._partition_data.keys()
    partition_data = table._partition_data.read()
    # Act
    table.update(update_df)
    # Assert
    assert_table_equals(table, expected)
    _assert_that_partitions_are_the_same(table, partition_names, partition_data)


def _assert_that_partitions_are_the_same(table, partition_names, partition_data):
    # Check that partitions keep the same structure after update
    df = table.read_arrow()
    index = df['index']
    for partition, partition_name in zip(index.chunks, partition_names):
        metadata = partition_data[partition_name]

        index_start = partition[0].as_py()
        index_end = partition[-1].as_py()
        num_rows = len(partition)

        assert index_start == metadata['min']
        assert index_end == metadata['max']
        assert num_rows == metadata['num_rows']


def _update_table_not_supported_type():
    return make_table(astype="polars[series]")


def _non_matching_index_dtype():
    df = make_table(sorted_string_index, astype="pandas")
    return df


def _non_matching_column_dtypes():
    df = make_table(sorted_string_index, cols=1, astype="pandas")
    df = df.reset_index()
    df.columns = ['c1', 'c2']
    df = df.head(5)
    return df


def _index_not_in_table():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 459]
    return df


def _column_name_not_in_stored_data():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c1', 'non-existant_column']
    return df


def _index_name_not_the_same_as_stored_index():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index.name = 'new_index_name'
    return df


def _duplicate_index_values():
    df = make_table(astype="pandas")
    df = df.head(5)
    df.index = [2, 5, 7, 10, 10]
    return df


def _duplicate_column_names():
    df = make_table(cols=2, astype="pandas")
    df = df.head(5)
    df.columns = ['c2', 'c2']
    return df


@pytest.mark.parametrize(
    ("update_df", "exception"),
    [
        (_update_table_not_supported_type, TypeError),
        (_non_matching_index_dtype, TypeError),
        (_non_matching_column_dtypes, TypeError),
        (_index_not_in_table, ValueError),
        (_column_name_not_in_stored_data, IndexError),
        (_index_name_not_the_same_as_stored_index, ValueError),
        (_duplicate_index_values, IndexError),
        (_duplicate_column_names, IndexError),
    ],
    ids=[
        "_update_table_not_supported_type",
        "_non_matching_index_dtype",
        "_non_matching_column_dtypes",
        "_index_not_in_table",
        "_column_name_not_in_stored_data",
        "_index_name_not_the_same_as_stored_index",
        "_duplicate_index_values",
        "_duplicate_column_names",
    ],
)
def test_can_update_table(store, update_df, exception):
    # Arrange
    update_df = update_df()
    original_df = make_table(cols=5, astype='pandas')
    store.write_table(TABLE_NAME, original_df)
    table = store.select_table(TABLE_NAME)
    # Act
    with pytest.raises(exception):
        table.update(update_df)
