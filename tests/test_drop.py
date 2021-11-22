import pytest
from .fixtures import *


def _wrong_index_dtype():
    return ['2019-01-01', '2022-03-05']


def _wrong_index_values():
    return [2, 5, 7, 10, 459]


def _duplicate_index_values():
    return [2, 5, 7, 10, 10]


@pytest.mark.parametrize(
    ("rows", "exception"),
    [
        (_wrong_index_dtype(), TypeError),
        (_wrong_index_values(), ValueError),
        (_duplicate_index_values(), IndexError),
    ],
    ids=[
        "_wrong_index_dtype",
        "_wrong_index_values",
        "_duplicate_index_values",
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
