import bmark
from . import _fixtures as fx
import featherstore as fs

insert_bench = bmark.Benchmark()


@insert_bench()
class insert(bmark.Benched):

    def __init__(self, shape, rows=None, cols=None, name='values', num_partitions=0):
        self._shape = shape
        self._rows = rows
        self._cols = cols
        self._num_partitions = num_partitions
        self.name = f"FS insert {name}"

    def run(self):
        self._insert(self._insert_df)

    def setup(self):
        df = fx.make_table(self._shape, astype='pandas')
        self._df, self._insert_df = fx.split_table(df, rows=self._rows, cols=self._cols)
        self._partition_size = fx.get_partition_size(self._df, self._num_partitions)

        fs.create_database('db')
        store = fs.create_store('store_name')
        self._table = store.select_table('table_name')

        if self._rows is not None:
            self._insert = self._table.insert
        elif self._cols is not None:
            self._insert = self._table.add_columns

    def teardown(self):
        fs.drop_store('store_name')
        fx.delete_db()

    def __enter__(self):
        self._table.write(self._df, index='index', partition_size=self._partition_size)
        return super().__enter__()

    def __exit__(self, exc, value, traceback):
        self._table.drop_table()
