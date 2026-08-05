from ._fixtures import OtherIO, ReadFileIO
import pandas as pd


class PandasWriteCsv(OtherIO):

    def __init__(self, shape):
        super().__init__(shape, astype='pandas')
        self.name = "Pandas write CSV"
        self._path += '.csv'

    def run(self):
        self._df.to_csv(self._path)


class PandasReadCsv(ReadFileIO):

    def __init__(self, shape):
        super().__init__(shape, extension='.csv', name="Pandas read CSV")

    def run(self):
        pd.read_csv(self._path)

    def _write_file(self):
        self._df.to_csv(self._path)
