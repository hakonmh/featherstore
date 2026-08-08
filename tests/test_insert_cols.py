import warnings

import pytest

from featherstore.exceptions import (
    ColumnAlreadyExistsError,
    ColumnLengthMismatchError,
    DuplicateColumnNamesError,
    IndexMismatchError,
    IndexNameInColumnsError,
    IndexTypeMismatchError,
)

from .fixtures import (
    DEFAULT_ARROW_INDEX_NAME,
    TABLE_NAME,
    assert_table_equals,
    continuous_datetime_index,
    convert_expected,
    convert_table,
    default_index,
    get_index_name,
    get_partition_size,
    insert_column_names_at,
    make_table,
    sort_table,
    sorted_string_index,
    split_table,
    unsorted_int_index,
    unsorted_string_index,
)


@pytest.mark.parametrize(
    ["index", "col_names", "col_idx"],
    [
        [unsorted_int_index, ["n0", "n1"], 3],
        [continuous_datetime_index, ["n0"], -1],
        [unsorted_string_index, ["n0", "n1"], -1],
        [default_index, ["n0"], 0],
    ],
)
@pytest.mark.parametrize("astype", ["pandas", "polars", "arrow"])
def test_insert_cols(store, index, col_names, col_idx, astype):
    # Arrange
    expected_pd = make_table(index, cols=5 + len(col_names), astype="pandas")
    expected_pd = insert_column_names_at(expected_pd, col_names, col_idx)
    original_pd, new_cols_pd = split_table(expected_pd, cols=col_names)
    expected_pd = sort_table(expected_pd)

    original_df = convert_table(original_pd, to=astype)
    new_cols = convert_table(new_cols_pd, to=astype, keep_index=True)
    expected = convert_expected(expected_pd, to=astype, like=original_pd)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(
        original_df,
        partition_size=partition_size,
        warnings="ignore",
        index=get_index_name(original_df),
    )
    # Act
    table.insert_columns(new_cols, idx=col_idx, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize(
    ["index", "col_names", "col_idx"],
    [
        [unsorted_int_index, ["n0"], 3],
        [continuous_datetime_index, ["n0"], -1],
        [default_index, ["n0"], 0],
    ],
)
def test_insert_cols_series(store, index, col_names, col_idx):
    # Arrange
    expected = make_table(index, cols=5 + len(col_names), astype="pandas")
    expected = insert_column_names_at(expected, col_names, col_idx)
    original_df, new_cols = split_table(expected, cols=col_names)
    new_cols = convert_table(new_cols, to="pandas[series]")
    expected = sort_table(expected)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings="ignore")
    # Act
    table.insert_columns(new_cols, idx=col_idx, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize(
    ["index", "col_names", "col_indices"],
    [
        [unsorted_int_index, ["n0", "n1"], [1, 3]],
        [continuous_datetime_index, ["n0"], [2]],
        [unsorted_string_index, ["n0", "n1", "n2"], [0, 2, 4]],
        [default_index, ["n0"], [0]],
    ],
)
@pytest.mark.parametrize("astype", ["pandas", "polars", "arrow"])
def test_insert_cols_with_idx_sequence(store, index, col_names, col_indices, astype):
    # Arrange
    expected_pd = make_table(index, cols=5 + len(col_names), astype="pandas")
    expected_pd = _build_expected_with_col_positions(
        expected_pd, col_names, col_indices
    )
    original_pd, new_cols_pd = split_table(expected_pd, cols=col_names)
    expected_pd = sort_table(expected_pd)

    original_df = convert_table(original_pd, to=astype)
    new_cols = convert_table(new_cols_pd, to=astype, keep_index=True)
    expected = convert_expected(expected_pd, to=astype, like=original_pd)

    partition_size = get_partition_size(original_df)
    table = store.select_table(TABLE_NAME)
    table.write(
        original_df,
        partition_size=partition_size,
        warnings="ignore",
        index=get_index_name(original_df),
    )
    # Act
    table.insert_columns(new_cols, idx=col_indices, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


def _build_expected_with_col_positions(df, col_names, col_indices):
    cols = df.columns.tolist()
    new_col_source = cols[-len(col_names) :]
    base_cols = cols[: -len(col_names)]
    df = df.rename(columns=dict(zip(new_col_source, col_names)))
    expected = df[base_cols].copy()
    for col_name, col_idx in zip(col_names, col_indices):
        expected.insert(col_idx, col_name, df[col_name])
    return expected


def test_insert_cols_warns_on_unsorted_index(store):
    # Arrange
    df = make_table(cols=6, astype="pandas")
    expected = insert_column_names_at(df, ["n0"], -1)
    original_df, new_cols = split_table(expected, cols=["n0"])
    new_cols = new_cols.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings="ignore")
    # Act and Assert
    with pytest.warns(UserWarning, match="unsorted"):
        table.insert_columns(new_cols, warnings="warn")


def test_insert_cols_can_ignore_unsorted_index_warning(store):
    # Arrange
    df = make_table(cols=6, astype="pandas")
    expected = insert_column_names_at(df, ["n0"], -1)
    original_df, new_cols = split_table(expected, cols=["n0"])
    new_cols = new_cols.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings="ignore")
    # Act and Assert
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        table.insert_columns(new_cols, warnings="ignore")


def _wrong_table_type():
    return make_table(cols=1, astype="polars[series]").rename("new_c1")


def _col_name_already_in_table():
    return make_table(cols=2, astype="pandas")


def _add_col_named_same_as_index():
    df = make_table(cols=1, astype="pandas")
    df.columns = [DEFAULT_ARROW_INDEX_NAME]
    return df


def _new_cols_contain_duplicate_names():
    df = make_table(cols=2, astype="pandas")
    df.columns = ["new_c1", "new_c1"]
    return df


def _non_matching_index_dtype():
    df = make_table(index=sorted_string_index, cols=2, astype="pandas")
    df.columns = ["new_c1", "new_c2"]
    return df


def _num_rows_doesnt_match():
    df = make_table(rows=42, cols=1, astype="pandas")
    df.columns = ["new_c1"]
    return df


def _non_matching_index_values():
    df = make_table(cols=1, astype="pandas")
    df.index += 50
    df.columns = ["new_c1"]
    return df


def _two_new_cols():
    df = make_table(cols=2, astype="pandas")
    df.columns = ["new_c0", "new_c1"]
    return df


def _one_new_col():
    df = make_table(cols=1, astype="pandas")
    df.columns = ["new_c1"]
    return df


@pytest.mark.parametrize(
    ("insert_cols_df", "exception"),
    [
        (_wrong_table_type, TypeError),
        (_col_name_already_in_table, ColumnAlreadyExistsError),
        (_add_col_named_same_as_index, IndexNameInColumnsError),
        (_new_cols_contain_duplicate_names, DuplicateColumnNamesError),
        (_non_matching_index_dtype, IndexTypeMismatchError),
        (_num_rows_doesnt_match, ColumnLengthMismatchError),
        (_non_matching_index_values, IndexMismatchError),
    ],
    ids=[
        "_wrong_table_type",
        "_col_name_already_in_table",
        "_add_col_named_same_as_index",
        "_new_cols_contain_duplicate_names",
        "_non_matching_index_dtype",
        "_num_rows_doesnt_match",
        "_non_matching_index_values",
    ],
)
def test_can_insert_cols(store, insert_cols_df, exception):
    # Arrange
    insert_cols_df = insert_cols_df()
    original_df = make_table(cols=5, astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.insert_columns(insert_cols_df)


@pytest.mark.parametrize(
    ("insert_cols_df", "idx", "exception"),
    [
        (_two_new_cols, [0], ValueError),
        (_two_new_cols, [0, 1, 2], ValueError),
        (_one_new_col, ["a"], TypeError),
        (_one_new_col, [1.5], TypeError),
        (_one_new_col, 1.5, TypeError),
    ],
    ids=[
        "_idx_too_short",
        "_idx_too_long",
        "_idx_non_int_element",
        "_idx_non_int_in_sequence",
        "_idx_non_int_scalar",
    ],
)
def test_can_insert_cols_with_invalid_idx(store, insert_cols_df, idx, exception):
    # Arrange
    insert_cols_df = insert_cols_df()
    original_df = make_table(cols=5, astype="pandas")
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(exception):
        table.insert_columns(insert_cols_df, idx=idx)


def test_insert_cols_rejects_invalid_warnings(store):
    # Arrange
    original_df = make_table(cols=5, astype="pandas")
    new_cols = make_table(cols=1, astype="pandas")
    new_cols.columns = ["new_c1"]
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(ValueError):
        table.insert_columns(new_cols, warnings="abcd")


def test_insert_columns_rejects_positional_idx(store):
    # Arrange
    original_df = make_table(cols=2, astype="pandas")
    new_cols = make_table(cols=1, astype="pandas")
    new_cols.columns = ["new_c0"]
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(TypeError):
        table.insert_columns(new_cols, 0)
