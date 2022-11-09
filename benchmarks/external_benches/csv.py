from ._fixtures import OtherIO
import pandas as pd


class pd_write_csv(OtherIO):

    def __init__(self, shape):
        super().__init__(shape, astype='pandas')
        self.name = "Pandas write CSV"
        self._path += '.csv'

    def run(self):
        self._df.to_csv(self._path)


class pd_read_csv(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas read CSV"
        super().__init__(shape, astype='pandas')
        self._path += '.csv'

    def run(self):
        pd.read_csv(self._path)

    def __enter__(self):
        self._df.to_csv(self._path)
        return self
