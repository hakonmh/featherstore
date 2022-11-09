from ._fixtures import OtherIO
import pandas as pd


class pd_write_pickle(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas write Pickle"
        super().__init__(shape, astype='pandas')
        self._path += '.pickle'

    def run(self):
        self._df.to_pickle(self._path)


class pd_read_pickle(OtherIO):

    def __init__(self, shape):
        super().__init__(shape, astype='pandas')
        self.name = "Pandas read Pickle"
        self._path += '.pickle'

    def run(self):
        pd.read_pickle(self._path)

    def __enter__(self):
        self._df.to_pickle(self._path)
        return self
