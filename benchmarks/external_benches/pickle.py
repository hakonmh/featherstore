from ._fixtures import OtherIO
import pandas as pd
import os


class pd_write_pickle(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas write Pickle"
        super().__init__(shape, astype='pandas')
        self._path += '.pickle'

    def run(self):
        self._df.to_pickle(self._path)


class pd_read_pickle(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas read Pickle"
        super().__init__(shape, astype='pandas')
        self._path += '.pickle'

    def run(self):
        pd.read_pickle(self._path)

    def setup(self):
        super().setup()
        self._df.to_pickle(self._path)
        del self._df

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        pass

    def teardown(self):
        if os.path.exists(self._path):
            os.remove(self._path)
        return super().teardown()
