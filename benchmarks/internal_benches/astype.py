import bmark
import dev.fixtures as fx
import featherstore as fs
import pyarrow as pa

TYPE_MAP = {
    pa.float16(): 'float',
    pa.float32(): 'float',
    pa.float64(): 'float',
    pa.int16(): 'int',
    pa.int32(): 'int',
    pa.int64(): 'int',
    pa.uint32(): 'uint',
    pa.bool_(): 'bool',
    pa.date32(): 'datetime',
    pa.date64(): 'datetime',
    pa.time32('ms'): 'datetime',
    pa.time64('us'): 'datetime',
    pa.timestamp('us'): 'datetime',
    pa.string(): 'string',
    pa.large_string(): 'string',
    pa.binary(): 'string',
    pa.large_binary(): 'string',
}

astype_bench = bmark.Benchmark()


@astype_bench()
class astype(bmark.Benched):

    def __init__(self, shape, cols, dtype=None, to=None, num_partitions=0):
        self._shape = shape
        self._dtype = _convert_to_pa_dtype(dtype)
        self._to = {col: to for col in cols}
        self._num_partitions = num_partitions
        to = _convert_to_pa_dtype(to)
        self.name = f"FS astype {self._dtype} to {to}"

    def run(self):
        self._table.astype(self._to)

    def setup(self):
        dtype = TYPE_MAP[self._dtype]
        df = fx.make_table(self._shape, astype='arrow', dtype=dtype)
        self._df = fx.change_dtype(df, to=self._dtype)
        self._partition_size = fx.get_partition_size(self._df, self._num_partitions)

        fs.create_database('db')
        store = fs.create_store('store_name')
        self._table = store.select_table('table_name')

    def teardown(self):
        fs.drop_store('store_name')
        fx.io.delete_db()

    def __enter__(self):
        self._table.write(self._df, index='index', partition_size=self._partition_size)
        return self

    def __exit__(self, exc, value, traceback):
        self._table.drop_table()


def _convert_to_pa_dtype(dtype):
    if __is_valid_dtype(dtype):
        dtype = pa.from_numpy_dtype(dtype)
    return dtype


def __is_valid_dtype(item):
    try:
        pa.from_numpy_dtype(item)
        return True
    except Exception:
        return False
