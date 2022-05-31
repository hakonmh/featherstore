import pytest
from .fixtures import *


@pytest.mark.parametrize(["index", "cols", "col_names", "col_idx"],
                         [[unsorted_int_index, 2, ['n0', 'n1'], 1],
                          [hardcoded_datetime_index, 1, ['n0'], -1],
                          [default_index, 1, ['n0'], 0]
                          ]
                         )
def test_add_cols(store, index, cols, col_names, col_idx):
    # Arrange
    fixtures = AddColsFixtures()
    original_df = fixtures.make_df(index)
    new_cols = fixtures.make_df(index, cols, col_names)
    expected = fixtures.add_cols(new_cols=new_cols, to=original_df, idx=col_idx)

    partition_size = get_partition_size(original_df, NUMBER_OF_PARTITIONS)
    table = store.select_table(TABLE_NAME)
    table.write(original_df, partition_size=partition_size, warnings='ignore')
    # Act
    table.add_columns(new_cols, idx=col_idx)
    # Assert
    df = table.read_pandas()
    assert df.equals(expected)


class AddColsFixtures:

    def __init__(self, rows=30):
        self.rows = rows

    def make_df(self, index=None, cols=5, col_names=None):
        df = make_table(index=index, rows=self.rows, cols=cols, astype="pandas")
        if col_names:
            df.columns = col_names
        return df

    def add_cols(self, to, new_cols, idx=-1):
        expected = to.join(new_cols)

        cols = to.columns.tolist()
        if idx != -1:
            for col in reversed(new_cols.columns):
                cols.insert(idx, col)
        else:
            cols = expected.columns

        expected = expected[cols]
        expected = expected.sort_index()
        return expected


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
        (_forbidden_col_name(), ValueError),
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
def test_can_add_cols(df, exception, store):
    # Arrange
    original_df = make_table(cols=5, astype='pandas')
    table = store.select_table(TABLE_NAME)
    table.write(original_df)
    # Act
    with pytest.raises(exception) as e:
        table.add_columns(df)
    # Assert
    assert isinstance(e.type(), exception)
