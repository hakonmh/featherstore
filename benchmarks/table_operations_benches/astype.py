import bmark
import pyarrow as pa

from . import _fixtures as fx
from ._helpers import close_table, open_table, partition_size

SOURCE_DTYPE_MAP = {
    pa.float16(): "float",
    pa.float32(): "float",
    pa.float64(): "float",
    pa.int16(): "int",
    pa.int32(): "int",
    pa.int64(): "int",
    pa.uint32(): "uint",
    pa.bool_(): "bool",
    pa.date32(): "datetime",
    pa.date64(): "datetime",
    pa.time32("ms"): "datetime",
    pa.time64("us"): "datetime",
    pa.timestamp("us"): "datetime",
    pa.string(): "string",
    pa.large_string(): "string",
    pa.binary(): "string",
    pa.large_binary(): "string",
}

astype_bench = bmark.Benchmark()


@astype_bench()
class Astype(bmark.Benched):
    def __init__(self, shape, cols, dtype=None, to=None, num_partitions=0):
        self._shape = shape
        self._dtype = fx.to_pa_dtype(dtype)
        self._to = {col: to for col in cols}
        self._num_partitions = num_partitions
        to_dtype = fx.to_pa_dtype(to)
        self.name = f"FS astype {self._dtype} to {to_dtype}"

    def run(self):
        self._table.astype(self._to)

    def setup(self):
        source_dtype = SOURCE_DTYPE_MAP[self._dtype]
        df = fx.make_table(self._shape, astype="arrow", dtype=source_dtype)
        self._df = fx.change_dtype(df, to=self._dtype)
        self._partition_size = partition_size(self._df, self._num_partitions)
        self._table = open_table()

    def teardown(self):
        close_table()

    def __enter__(self):
        self._table.write(self._df, index="index", partition_size=self._partition_size)
        return self

    def __exit__(self, exc, value, traceback):
        self._table.drop_table()
