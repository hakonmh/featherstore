import bmark
import featherstore as fs
import sys
sys.path.insert(0, '')
from internal_benches import _fixtures as fx  # noqa: E402


class fs_write_pd(bmark.Benched):

    def __init__(self, shape, num_partitions=0):
        self.name = "Pandas write FeatherStore"
        self._shape = shape
        self._num_partitions = num_partitions
        super().__init__()

    def run(self):
        self._store.write_table('table_name', self._df, index='index',
                                partition_size=self._partition_size)

    def setup(self):
        self._df = fx.make_table(self._shape, astype='pandas')
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


class fs_read_pd(bmark.Benched):

    def __init__(self, shape, rows=None, cols=None, name='', num_partitions=0):
        self.name = "Pandas read FeatherStore"
        self._shape = shape
        self._rows = rows
        self._cols = cols
        self._num_partitions = num_partitions

    def run(self):
        self._store.read_pandas('table_name', cols=self._cols, rows=self._rows)

    def setup(self):
        df = fx.make_table(self._shape)
        partition_size = fx.get_partition_size(df, self._num_partitions)

        fs.create_database('db')
        self._store = fs.create_store('store_name')
        self._store.write_table('table_name', df, partition_size=partition_size)

    def teardown(self):
        self._store.drop_table('table_name')
        fs.drop_store('store_name')
        fx.delete_db()
