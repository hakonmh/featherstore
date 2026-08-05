from pyarrow import feather

from ._fixtures import OtherIO, ReadFileIO

FEATHER_CHUNKSIZE = 128 * 1024**2


class PandasWriteFeather(OtherIO):
    def __init__(self, shape):
        super().__init__(shape, astype="pandas")
        self.name = "Pandas write Feather"
        self._path += ".feather"

    def run(self):
        feather.write_feather(
            self._df,
            self._path,
            compression="uncompressed",
            chunksize=FEATHER_CHUNKSIZE,
        )


class PandasReadFeather(ReadFileIO):
    def __init__(self, shape):
        super().__init__(shape, extension=".feather", name="Pandas read Feather")

    def run(self):
        feather.read_feather(self._path, memory_map=True)

    def _write_file(self):
        feather.write_feather(
            self._df,
            self._path,
            compression="uncompressed",
            chunksize=FEATHER_CHUNKSIZE,
        )
