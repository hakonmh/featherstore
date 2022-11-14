import bmark
from . import _fixtures as fx
import featherstore as fs

read_bench = bmark.Benchmark()


@read_bench('setup')
def _setup(shape, num_partitions=0):
    df = fx.make_table(shape)
    partition_size = fx.get_partition_size(df, num_partitions)

    fs.create_database('db')
    store = fs.create_store('store_name')
    store.write_table('table_name', df, partition_size=partition_size)


@read_bench('teardown')
def _teardown():
    store = fs.Store('store_name')
    store.drop_table('table_name')
    fx.delete_db()


class ReadFS(bmark.Benched):

    def __init__(self, rows, cols, name):
        self._rows = rows
        self._cols = cols
        if name:
            self.name = self.name + ' ' + name

    def setup(self):
        self._store = fs.Store('store_name')

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        pass


@read_bench()
class read_arrow(ReadFS):

    def __init__(self, *, rows=None, cols=None, name=''):
        self.name = "FS read Arrow"
        super().__init__(rows, cols, name)

    def run(self):
        self._store.read_arrow('table_name', cols=self._cols, rows=self._rows)


@read_bench()
class read_pandas(ReadFS):

    def __init__(self, *, rows=None, cols=None, name=''):
        self.name = "FS read Pandas"
        super().__init__(rows, cols, name)

    def run(self):
        self._store.read_pandas('table_name', cols=self._cols, rows=self._rows)


@read_bench()
class read_polars(ReadFS):

    def __init__(self, *, rows=None, cols=None, name=''):
        self.name = "FS read Polars"
        super().__init__(rows, cols, name)

    def run(self):
        self._store.read_polars('table_name', cols=self._cols, rows=self._rows)
