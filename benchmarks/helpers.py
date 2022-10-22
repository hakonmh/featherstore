import os
import timeit
import pandas as pd
from numpy.random import uniform
import featherstore as fs


def time_it(func, number, *args, **kwargs):
    MS = 1000
    runtime = timeit.timeit('func(*args, **kwargs)',
                            globals={**globals(), **locals()},
                            number=number)
    runtime = runtime * MS / number
    return runtime


def generate_df(rows, cols):
    index = list(range(rows))
    data = {f'c{c}': uniform(-10000, 10000, size=rows) for c in range(cols)}
    return pd.DataFrame(data=data, index=index)


class TimeSuiteWrite:

    def setup(self):
        self.db_path = 'benchmark_db'
        fs.create_database('benchmark_db', errors='ignore')
        fs.connect('benchmark_db')
        fs.create_store('test_store', errors='ignore')
        self.store = fs.Store('test_store')

    def teardown(self):
        tables = self.store.list_tables()
        for table_name in tables:
            self.store.drop_table(table_name)
        self.store.drop()
        _delete_db(self.db_path)

    def write_table(self, table):
        self.store.write_table('df_benchmark', table, errors='ignore')


class TimeSuiteRead:

    def setup(self, table):
        self.db_path = 'benchmark_db'
        fs.create_database('benchmark_db', errors='ignore')
        fs.connect('benchmark_db')
        fs.create_store('test_store')
        self.store = fs.Store('test_store')

        self.store.write_table('test_df', table, errors='ignore')

    def teardown(self):
        tables = self.store.list_tables()
        for table_name in tables:
            self.store.drop_table(table_name)
        self.store.drop()
        _delete_db(self.db_path)

    def read_pandas(self):
        self.store.read_pandas('test_df')

    def read_arrow(self):
        self.store.read_arrow('test_df')

    def read_polars(self):
        self.store.read_polars('test_df')


def _delete_db(db_path):
    DB_MARKER = '.featherstore'
    items = os.listdir(db_path)
    if items == [DB_MARKER]:
        db_marker_path = os.path.join(db_path, DB_MARKER)
        os.remove(db_marker_path)
        os.rmdir(db_path)
    else:
        raise PermissionError("Can't delete a database that contains items")
