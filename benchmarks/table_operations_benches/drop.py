import bmark

from . import _fixtures as fx
from ._helpers import close_table, open_table, partition_size

drop_bench = bmark.Benchmark()


@drop_bench()
class Drop(bmark.Benched):
    def __init__(self, shape, rows=None, cols=None, name="values", num_partitions=0):
        self._shape = shape
        self._rows = rows
        self._cols = cols
        self._num_partitions = num_partitions
        self.name = f"FS drop {name}"

    def run(self):
        self._table.drop(rows=self._rows, cols=self._cols)

    def setup(self):
        self._df = fx.make_table(self._shape, astype="arrow")
        self._partition_size = partition_size(self._df, self._num_partitions)
        self._table = open_table()

    def teardown(self):
        close_table()

    def __enter__(self):
        self._table.write(self._df, index="index", partition_size=self._partition_size)
        return self

    def __exit__(self, exc, value, traceback):
        self._table.drop_table()
