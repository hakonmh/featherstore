import warnings

import pyarrow as pa
import pytest

from .fixtures import *


@pytest.mark.parametrize(["index", "col_names", "col_idx"],
                         [[unsorted_int_index, ['n0', 'n1'], 3],
                          [continuous_datetime_index, ['n0'], -1],
                          [unsorted_string_index, ['n0', 'n1'], -1],
                          [default_index, ['n0'], 0]
                          ]
                         )
@pytest.mark.parametrize("astype", ["pandas", "pandas[series]", "polars", "arrow"])
def test_insert_cols(store, index, col_names, col_idx, astype):
    if astype == "pandas[series]" and len(col_names) != 1:
        pytest.skip("Series input requires a single column")

    num_cols = 5 + len(col_names)
    df = make_table(index=index, cols=num_cols, astype="pandas")
    expected_pd = _change_cols(df, col_names, col_idx)
    expected_pd = sort_table(expected_pd)
    original_pd, new_cols_pd = split_table(expected_pd, cols=col_names)

    original_df, new_cols, expected = _to_backend(
        original_pd, new_cols_pd, expected_pd, astype=astype, index=index
    )

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    index_name = get_index_name(original_df) or (expected_pd.index.name or None)
    if not astype.startswith('pandas') and expected_pd.index.name:
        index_name = expected_pd.index.name
    table.write(original_df, partition_size=partition_size, warnings='ignore',
                index=index_name if not astype.startswith('pandas') else None)
    table.insert_columns(new_cols, idx=col_idx, warnings='ignore')
    assert_table_equals(table, expected)


def _to_backend(original_pd, new_cols_pd, expected_pd, *, astype, index):
    backend = astype.split('[')[0]
    as_series = '[series]' in astype

    if backend == 'pandas':
        squeeze = as_series or expected_pd.shape[1] == 1
        new_cols = new_cols_pd.squeeze(axis=1) if as_series else new_cols_pd
        original_df = original_pd.squeeze(axis=1) if squeeze else original_pd
        expected = expected_pd.squeeze(axis=1) if squeeze else expected_pd
        return original_df, new_cols, expected

    index_name = expected_pd.index.name or DEFAULT_ARROW_INDEX_NAME
    full = _arrow_with_index(expected_pd, index_name)
    original_df, new_cols = split_table(
        full, cols=list(new_cols_pd.columns), keep_index=True, index_name=index_name
    )
    expected = format_arrow_table(full)
    if df_has_default_index(expected):
        expected = drop_default_index_if_exists(expected)

    if backend == 'polars':
        original_df = convert_table(original_df, to='polars', as_series=False)
        new_cols = convert_table(new_cols, to='polars', as_series=False)
        expected = convert_table(expected, to='polars', as_series=False)
    return original_df, new_cols, expected


def _arrow_with_index(pdf, index_name):
    arrow = convert_table(pdf, to='arrow')
    if index_name not in arrow.column_names:
        arrow = arrow.add_column(0, index_name, pa.array(pdf.index))
    return format_arrow_table(arrow)


def _change_cols(df, col_names, col_idx):
    cols = df.columns.tolist()
    end = col_idx + len(col_names)
    if col_idx < 0:
        col_idx = -len(col_names)
        end = None
    cols[col_idx:end] = col_names
    df.columns = cols
    return df


def test_insert_cols_warns_on_unsorted_index(store):
    df = make_table(cols=6, astype="pandas")
    expected = _change_cols(df.copy(), ['n0'], -1)
    original_df, new_cols = split_table(expected, cols=['n0'])
    new_cols = new_cols.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings='ignore')
    with pytest.warns(UserWarning, match="unsorted"):
        table.insert_columns(new_cols, warnings='warn')


def test_insert_cols_can_ignore_unsorted_warning(store):
    df = make_table(cols=6, astype="pandas")
    expected = _change_cols(df.copy(), ['n0'], -1)
    original_df, new_cols = split_table(expected, cols=['n0'])
    new_cols = new_cols.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings='ignore')
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        table.insert_columns(new_cols, warnings='ignore')


def _wrong_table_type():
    return make_table(cols=1, astype='polars[series]').rename('new_c1')


def _col_name_already_in_table():
    return make_table(cols=2, astype='pandas')


def _add_col_named_same_as_index():
    df = make_table(cols=1, astype='pandas')
    df.columns = [DEFAULT_ARROW_INDEX_NAME]
    return df


def _new_cols_contain_duplicate_names():
    df = make_table(cols=2, astype='pandas')
    df.columns = ['new_c1', 'new_c1']
    return df


def _non_matching_index_dtype():
    df = make_table(index=sorted_string_index, cols=2, astype='pandas')
    df.columns = ['new_c1', 'new_c2']
    return df


def _num_rows_doesnt_match():
    df = make_table(rows=42, cols=1, astype='pandas')
    df.columns = ['new_c1']
    return df


def _non_matching_index_values():
    df = make_table(cols=1, astype='pandas')
    df.index += 50
    df.columns = ['new_c1']
    return df


@pytest.mark.parametrize(
    ("insert_cols_df", "exception"),
    [
        (_wrong_table_type, TypeError),
        (_col_name_already_in_table, IndexError),
        (_add_col_named_same_as_index, ValueError),
        (_new_cols_contain_duplicate_names, IndexError),
        (_non_matching_index_dtype, TypeError),
        (_num_rows_doesnt_match, IndexError),
        (_non_matching_index_values, ValueError),
    ],
    ids=[
        "_wrong_table_type",
        "_col_name_already_in_table",
        "_add_col_named_same_as_index",
        "_new_cols_contain_duplicate_names",
        "_non_matching_index_dtype",
        "_num_rows_doesnt_match",
        "_non_matching_index_values"
    ]
)
def test_can_insert_cols(store, insert_cols_df, exception):
    insert_cols_df = insert_cols_df()
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    with pytest.raises(exception):
        table.insert_columns(insert_cols_df)
