import pandas as pd

from ._fixtures import OtherIO, ReadFileIO


class PandasWritePickle(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas write Pickle"
        super().__init__(shape, astype='pandas')
        self._path += '.pickle'

    def run(self):
        self._df.to_pickle(self._path)


class PandasReadPickle(ReadFileIO):

    def __init__(self, shape):
        super().__init__(shape, extension='.pickle', name="Pandas read Pickle")

    def run(self):
        pd.read_pickle(self._path)

    def _write_file(self):
        self._df.to_pickle(self._path)
