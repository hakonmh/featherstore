import bmark
from . import _fixtures as fx
import featherstore as fs

append_bench = bmark.Benchmark()


class AppendFS(bmark.Benched):

    def __init__(self, shape, rows, astype, num_partitions):
        self._shape = shape
        self._rows = {'after': self._shape[0] - rows}
        self._astype = astype
        self._num_partitions = num_partitions

    def run(self):
        self._table.append(self._append_df)

    def setup(self):
        df = fx.make_table(self._shape, astype=self._astype)
        df, self._append_df = fx.split_table(df, rows=self._rows)
        self._partition_size = fx.get_partition_size(df, self._num_partitions)

        fs.create_database('db')
        store = fs.create_store('store_name')
        self._table = store.select_table('table_name')
        self._table.write(df, index='index', partition_size=self._partition_size)

    def teardown(self):
        self._table.drop_table()
        fs.drop_store('store_name')
        fx.delete_db()

    def __exit__(self, exc, value, traceback):
        self._table.drop(rows=self._rows)


@append_bench()
class append_pandas(AppendFS):

    def __init__(self, shape, rows=None, num_partitions=0):
        self.name = f"FS append Pandas (list of {rows:,d} rows)"
        super().__init__(shape, rows, astype='pandas', num_partitions=num_partitions)


@append_bench()
class append_arrow(AppendFS):

    def __init__(self, shape, rows=None, num_partitions=0):
        self.name = f"FS append Arrow (list of {rows:,d} rows)"
        super().__init__(shape, rows, astype='arrow', num_partitions=num_partitions)


@append_bench()
class append_polars(AppendFS):

    def __init__(self, shape, rows=None, num_partitions=0):
        self.name = f"FS append Polars (list of {rows:,d} rows)"
        super().__init__(shape, rows, astype='polars', num_partitions=num_partitions)
