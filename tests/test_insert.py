import warnings

import pytest

from .fixtures import (
    TABLE_NAME,
    assert_table_equals,
    convert_expected,
    convert_table,
    default_index,
    get_index_name,
    insert_column_names_at,
    make_table,
    sort_table,
    split_table,
)


@pytest.mark.parametrize("astype", ["pandas", "polars", "arrow"])
def test_insert_routes_matching_cols_to_rows(store, astype):
    # Arrange
    expected_pd = make_table(default_index, rows=30, cols=3, astype="pandas")
    original_pd, insert_pd = split_table(expected_pd, rows=[4, 1, 7])
    expected_pd = sort_table(expected_pd)

    original_df = convert_table(original_pd, to=astype, keep_index=True)
    insert_df = convert_table(insert_pd, to=astype, keep_index=True)
    expected = convert_expected(expected_pd, to=astype, like=original_pd)

    table = store.select_table(TABLE_NAME)
    table.write(
        original_df,
        warnings="ignore",
        index=get_index_name(original_df),
    )
    # Act
    table.insert(insert_df, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


@pytest.mark.parametrize("astype", ["pandas", "polars", "arrow"])
def test_insert_routes_differing_cols_to_columns(store, astype):
    # Arrange
    expected_pd = make_table(cols=3, astype="pandas")
    expected_pd = insert_column_names_at(expected_pd, ["n0"], -1)
    original_pd, new_cols_pd = split_table(expected_pd, cols=["n0"])
    expected_pd = sort_table(expected_pd)

    original_df = convert_table(original_pd, to=astype)
    new_cols = convert_table(new_cols_pd, to=astype, keep_index=True)
    expected = convert_expected(expected_pd, to=astype, like=original_pd)

    table = store.select_table(TABLE_NAME)
    table.write(original_df, index=get_index_name(original_df))
    # Act
    table.insert(new_cols, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


def test_insert_forwards_idx_when_inserting_columns(store):
    # Arrange
    expected = make_table(cols=4, astype="pandas")
    expected = insert_column_names_at(expected, ["n0"], 0)
    original_df, new_cols = split_table(expected, cols=["n0"])
    expected = sort_table(expected)

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings="ignore")
    # Act
    table.insert(new_cols, idx=0, warnings="ignore")
    # Assert
    assert_table_equals(table, expected)


def test_insert_raises_when_idx_passed_for_rows(store):
    # Arrange
    original_df = make_table(default_index, rows=30, astype="pandas")
    insert_df = make_table(default_index, rows=3, astype="pandas")
    insert_df.index = [30, 33, 31]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings="ignore")
    # Act and Assert
    with pytest.raises(TypeError, match="idx"):
        table.insert(insert_df, idx=0)


def test_insert_rejects_positional_idx(store):
    # Arrange
    original_df = make_table(cols=2, astype="pandas")
    new_cols = make_table(cols=1, astype="pandas")
    new_cols.columns = ["new_c0"]
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act and Assert
    with pytest.raises(TypeError):
        table.insert(new_cols, 0)


def test_insert_forwards_warnings_ignore(store):
    # Arrange
    expected = make_table(rows=30, cols=3, astype="pandas")
    original_df, insert_df = split_table(expected, rows=[4, 1, 7])
    insert_df = insert_df.iloc[::-1]

    table = store.select_table(TABLE_NAME)
    table.write(original_df, warnings="ignore")
    # Act and Assert
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        table.insert(insert_df, warnings="ignore")
