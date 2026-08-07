from featherstore._table import _raise_if
from featherstore.exceptions import ColumnMismatchError


def can_insert(df, table_data, idx):
    inserting_rows = cols_matches_table_cols(df, table_data)
    if inserting_rows:
        _raise_if_idx_provided_when_inserting_rows(idx)


def cols_matches_table_cols(df, table_data):
    try:
        _raise_if.cols_does_not_match(df, table_data)
    except ColumnMismatchError:
        return False
    return True


def _raise_if_idx_provided_when_inserting_rows(idx):
    if idx is not -1:
        raise TypeError("'idx' is only valid when inserting columns")
