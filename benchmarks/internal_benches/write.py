import bmark
import featherstore as fs
from . import _fixtures as fx

write_bench = bmark.Benchmark()


class WriteFS(bmark.Benched):

    def __init__(self, shape, astype, num_partitions, sorted):
        self._shape = shape
        self._astype = astype
        self._num_partitions = num_partitions
        self.sorted = sorted
        super().__init__()

    def run(self):
        self._store.write_table('table_name', self._df, index='index',
                                partition_size=self._partition_size,
                                warnings='ignore')

    def setup(self):
        self._df = fx.make_table(self._shape, sorted=self.sorted, astype=self._astype)
        self._partition_size = fx.get_partition_size(self._df, self._num_partitions)
        fs.create_database('db')

    def teardown(self):
        fx.delete_db()

    def __enter__(self):
        self._store = fs.create_store('store_name')
        return self

    def __exit__(self, exc, value, traceback):
        self._store.drop_table('table_name')
        fs.drop_store('store_name')


@write_bench()
class write_arrow(WriteFS):

    def __init__(self, shape, num_partitions=0, sorted=True):
        self.name = "FS write Arrow"
        super().__init__(shape, astype='arrow', num_partitions=num_partitions,
                         sorted=sorted)


@write_bench()
class write_pandas(WriteFS):

    def __init__(self, shape, num_partitions=0, sorted=True):
        self.name = "FS write Pandas"
        super().__init__(shape, astype='pandas', num_partitions=num_partitions,
                         sorted=sorted)


@write_bench()
class write_polars(WriteFS):

    def __init__(self, shape, num_partitions=0, sorted=True):
        self.name = "FS write Polars"
        super().__init__(shape, astype='polars', num_partitions=num_partitions,
                         sorted=sorted)
