import pytest

from featherstore.exceptions import (
    AppendIndexError,
    CannotDropAllColumnsError,
    CannotDropAllRowsError,
    ColumnAlreadyExistsError,
    ColumnError,
    ColumnMismatchError,
    ColumnNotFoundError,
    DatabaseConnectionError,
    DuplicateColumnNamesError,
    DuplicateIndexValuesError,
    FeatherStoreException,
    IndexSchemaError,
    IndexTypeMismatchError,
    NotConnectedError,
    RowError,
    RowNotFoundError,
    TableAlreadyExistsError,
    TableError,
    TableNotFoundError,
)


@pytest.mark.parametrize(
    ("leaf", "category", "base"),
    [
        (ColumnNotFoundError, ColumnError, FeatherStoreException),
        (ColumnAlreadyExistsError, ColumnError, FeatherStoreException),
        (ColumnMismatchError, ColumnError, FeatherStoreException),
        (DuplicateColumnNamesError, ColumnError, FeatherStoreException),
        (RowNotFoundError, RowError, FeatherStoreException),
        (AppendIndexError, RowError, FeatherStoreException),
        (DuplicateIndexValuesError, IndexSchemaError, FeatherStoreException),
        (IndexTypeMismatchError, IndexSchemaError, FeatherStoreException),
        (TableNotFoundError, TableError, FeatherStoreException),
        (TableAlreadyExistsError, TableError, FeatherStoreException),
        (CannotDropAllRowsError, TableError, FeatherStoreException),
        (CannotDropAllColumnsError, TableError, FeatherStoreException),
        (NotConnectedError, DatabaseConnectionError, FeatherStoreException),
    ],
)
def test_exception_hierarchy(leaf, category, base):
    assert issubclass(leaf, category)
    assert issubclass(category, base)
    assert issubclass(leaf, base)


@pytest.mark.parametrize(
    ("category", "leaf"),
    [
        (ColumnError, ColumnNotFoundError),
        (RowError, RowNotFoundError),
        (TableError, TableNotFoundError),
        (FeatherStoreException, ColumnNotFoundError),
    ],
)
def test_can_catch_by_category(category, leaf):
    with pytest.raises(category):
        raise leaf("example")


def test_column_not_found_error_includes_missing_columns():
    missing = ["c7", "c8"]
    with pytest.raises(ColumnNotFoundError, match=r"\['c7', 'c8'\]"):
        raise ColumnNotFoundError(
            f"Trying to access columns not found in table ({missing})"
        )


def test_row_not_found_error_includes_missing_rows():
    missing = [3334]
    with pytest.raises(RowNotFoundError, match=r"\[3334\]"):
        raise RowNotFoundError(f"Trying to access rows not found in table ({missing})")


def test_column_not_found_error_via_read(store):
    from .fixtures import TABLE_NAME, make_table

    df = make_table()
    store.write_table(TABLE_NAME, df)
    with pytest.raises(ColumnNotFoundError, match=r"c3334"):
        store.read_pandas(TABLE_NAME, cols=["c0", "c1", "c3334"])
