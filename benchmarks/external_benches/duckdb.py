from ._fixtures import OtherIO
import duckdb
import os


class duckdb_write_pd(OtherIO):

    def __init__(self, shape):
        super().__init__(shape, astype='pandas')
        self.name = "Pandas write DuckDB"
        self._path += '.duckdb'

    def run(self):
        df = self._df  # noqa: F841
        self._con.execute("CREATE TABLE table_name AS SELECT * FROM df")
        self._con.execute("INSERT INTO table_name SELECT * FROM df")

    def setup(self):
        super().setup()

    def __enter__(self):
        self._con = duckdb.connect(self._path)
        return super().__enter__()

    def __exit__(self, exception_type, exception_value, traceback):
        self._con.close()
        super().__exit__(exception_type, exception_value, traceback)


class duckdb_read_pd(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas read DuckDB"
        super().__init__(shape, astype='pandas')
        self._path += '.feather'

    def run(self):
        self._con.execute("SELECT * from table_name").fetchdf()

    def setup(self):
        super().setup()
        df = self._df  # noqa: F841
        con = duckdb.connect(self._path)
        con.execute("CREATE TABLE table_name AS SELECT * FROM df")
        con.execute("INSERT INTO table_name SELECT * FROM df")
        con.close()
        del self._df, df

    def __enter__(self):
        self._con = duckdb.connect(self._path, read_only=True)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._con.close()

    def teardown(self):
        if os.path.exists(self._path):
            os.remove(self._path)
        return super().teardown()
