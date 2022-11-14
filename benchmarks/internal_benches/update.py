import bmark
from . import _fixtures as fx
import featherstore as fs

update_bench = bmark.Benchmark()


@update_bench()
class update(bmark.Benched):

    def __init__(self, shape, rows=None, cols=None, name='values', num_partitions=0):
        self._shape = shape
        self._rows = rows
        self._cols = cols
        self._num_partitions = num_partitions
        self.name = f"FS update {name}"

    def run(self):
        self._table.update(self._update_df)

    def setup(self):
        self._df = fx.make_table(self._shape, astype='pandas')
        _, update_df = fx.split_table(self._df, rows=self._rows, cols=self._cols)
        self._update_df = fx.update_values(update_df)
        self._partition_size = fx.get_partition_size(self._df, self._num_partitions)

        fs.create_database('db')
        store = fs.create_store('store_name')
        self._table = store.select_table('table_name')

    def teardown(self):
        fs.drop_store('store_name')
        fx.delete_db()

    def __enter__(self):
        self._table.write(self._df, partition_size=self._partition_size)
        return super().__enter__()

    def __exit__(self, exc, value, traceback):
        self._table.drop_table()
