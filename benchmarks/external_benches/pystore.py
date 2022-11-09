import os
import pystore
from ._fixtures import OtherIO


class pystore_write_pd(OtherIO):

    def __init__(self, shape):
        super().__init__(shape, astype='pandas')
        self.name = "Pandas write Pystore"

    def run(self):
        self._collection.write('table_name', self._df)

    def teardown(self):
        os.rmdir(self._path)
        return super().teardown()

    def __enter__(self):
        path = os.path.abspath(self._path)
        pystore.set_path(path)
        self._store = pystore.store('store_name')
        self._collection = self._store.collection('collection')
        return super().__enter__()

    def __exit__(self, exception_type, exception_value, traceback):
        self._collection.delete_item('table_name')
        self._store.delete_collection('collection')
        pystore.delete_store('store_name')


class pystore_read_pd(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas read Pystore"
        super().__init__(shape, astype='pandas')

    def run(self):
        self._collection.item('table_name').to_pandas()

    def setup(self):
        super().setup()

        path = os.path.abspath(self._path)
        pystore.set_path(path)

        self._store = pystore.store('store_name')
        self._collection = self._store.collection('collection')
        return self

    def teardown(self):
        self._store.delete_collection('collection')
        pystore.delete_store('store_name')
        os.rmdir(self._path)
        return super().teardown()

    def __enter__(self):
        self._collection.write('table_name', self._df)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._collection.delete_item('table_name')
