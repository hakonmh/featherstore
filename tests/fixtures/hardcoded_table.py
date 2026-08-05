import random
from dataclasses import dataclass

import pandas as pd
import pyarrow as pa

from .cast_table import change_dtype
from .convert_table import convert_table
from .expected_table import insert_columns_expected, merge_rows
from .make_table import make_table
from .split_table import split_table

E2E_SEED = 0
ROW_LABELS = [f"r{i:02d}" for i in range(10)]
INSERT_ROW_LABELS = ["r10", "r11"]
INSERT_COL_NAMES = ["n0", "n1"]
INSERT_COL_IDX = 2


def _after_row_ops_table():
    index = pd.Index(ROW_LABELS + INSERT_ROW_LABELS, name="row_id")
    df = make_table(rows=12, cols=5, astype="pandas", seed=E2E_SEED, dtype="float")
    df.index = index
    return df


def _after_insert_col_table():
    insert_cols = make_table(
        rows=12,
        cols=len(INSERT_COL_NAMES),
        astype="pandas",
        seed=E2E_SEED + 1,
        dtype="int",
    )
    insert_cols.columns = INSERT_COL_NAMES
    return insert_columns_expected(_after_row_ops_table(), insert_cols, INSERT_COL_IDX)


def make_hardcoded_table():
    """Return the initial table written at the start of the e2e workflow."""
    without_insert, _ = split_table(_after_row_ops_table(), rows=INSERT_ROW_LABELS)
    initial, _ = split_table(without_insert, rows=ROW_LABELS[6:10])
    return initial


def make_hardcoded_append_df():
    """Rows appended after the initial write."""
    without_insert, _ = split_table(_after_row_ops_table(), rows=INSERT_ROW_LABELS)
    _, append_df = split_table(without_insert, rows=ROW_LABELS[6:10])
    return append_df


def make_hardcoded_insert_rows_df():
    """Rows inserted by index during the e2e workflow."""
    _, insert_df = split_table(_after_row_ops_table(), rows=INSERT_ROW_LABELS)
    return insert_df


def make_hardcoded_insert_cols_df(num_rows):
    _, insert_cols = split_table(_after_insert_col_table(), cols=INSERT_COL_NAMES)
    return insert_cols.iloc[:num_rows]


@dataclass(frozen=True)
class E2EOperation:
    name: str
    group: int

    def apply_to_table(self, table):
        raise NotImplementedError

    def apply_to_expected(self, expected):
        raise NotImplementedError


@dataclass(frozen=True)
class AppendOperation(E2EOperation):
    append_df: pd.DataFrame

    def apply_to_table(self, table):
        table.append(self.append_df, warnings="ignore")

    def apply_to_expected(self, expected):
        return merge_rows(expected, self.append_df)


@dataclass(frozen=True)
class InsertRowsOperation(E2EOperation):
    insert_df: pd.DataFrame

    def apply_to_table(self, table):
        table.insert(self.insert_df)

    def apply_to_expected(self, expected):
        return merge_rows(expected, self.insert_df)


@dataclass(frozen=True)
class InsertColsOperation(E2EOperation):
    col_idx: int = INSERT_COL_IDX

    def apply_to_table(self, table):
        stored_df = table.read_pandas()
        insert_df = make_hardcoded_insert_cols_df(len(stored_df))
        insert_df.index = stored_df.index
        table.insert(insert_df, idx=self.col_idx)

    def apply_to_expected(self, expected):
        insert_df = make_hardcoded_insert_cols_df(len(expected))
        return insert_columns_expected(expected, insert_df, self.col_idx)


@dataclass(frozen=True)
class DropOperation(E2EOperation):
    rows: object = None
    cols: object = None

    def apply_to_table(self, table):
        table.drop(rows=self.rows, cols=self.cols)

    def apply_to_expected(self, expected):
        expected, _ = split_table(expected, rows=self.rows, cols=self.cols)
        return expected


@dataclass(frozen=True)
class RenameColumnsOperation(E2EOperation):
    columns: object
    to: object = None

    def apply_to_table(self, table):
        table.rename_columns(self.columns, to=self.to)

    def apply_to_expected(self, expected):
        if isinstance(self.columns, dict):
            return expected.rename(columns=self.columns)
        renamed = expected.copy()
        renamed.columns = self.to if self.to is not None else self.columns
        return renamed


@dataclass(frozen=True)
class ReorderColumnsOperation(E2EOperation):
    columns: list

    def apply_to_table(self, table):
        table.reorder_columns(self.columns)

    def apply_to_expected(self, expected):
        return expected[self.columns]


@dataclass(frozen=True)
class AstypeOperation(E2EOperation):
    columns: list
    dtype: object

    def apply_to_table(self, table):
        table.astype(self.columns, to=[self.dtype] * len(self.columns))

    def apply_to_expected(self, expected):
        index_name = expected.index.name
        arrow_df = convert_table(expected, to="arrow")
        arrow_df = change_dtype(arrow_df, self.dtype, cols=self.columns)
        return convert_table(arrow_df, to="pandas", index_name=index_name)


def build_e2e_operations():
    return [
        AppendOperation("append", group=1, append_df=make_hardcoded_append_df()),
        InsertRowsOperation(
            "insert_row", group=1, insert_df=make_hardcoded_insert_rows_df()
        ),
        InsertColsOperation("insert_col", group=2),
        DropOperation("drop_rows", group=3, rows=["r00", "r10"]),
        DropOperation("drop_cols", group=3, cols=["c1"]),
        RenameColumnsOperation(
            "rename_columns",
            group=4,
            columns={"c0": "a0", "c3": "a3"},
        ),
        ReorderColumnsOperation(
            "reorder_columns",
            group=4,
            columns=["a3", "n0", "a0", "n1", "c2", "c4"],
        ),
        AstypeOperation("astype", group=4, columns=["a0"], dtype=pa.float32()),
    ]


def shuffle_e2e_operations(operations, *, seed=0):
    def by_group(group):
        return [op for op in operations if op.group == group]

    group_3 = by_group(3)
    random.Random(seed).shuffle(group_3)

    return by_group(1) + by_group(2) + group_3 + by_group(4)


def apply_operations_to_expected(initial_df, operations):
    expected = initial_df.copy()
    for operation in operations:
        expected = operation.apply_to_expected(expected)
    return expected
