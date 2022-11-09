from ._fixtures import OtherIO
from pyarrow import feather


class pd_write_feather(OtherIO):

    def __init__(self, shape):
        super().__init__(shape, astype='pandas')
        self.name = "Pandas write Feather"
        self._path += '.feather'

    def run(self):
        CHUNKSIZE = 128 * 1024**2  # bytes
        feather.write_feather(self._df, self._path,
                              compression="uncompressed",
                              chunksize=CHUNKSIZE)


class pd_read_feather(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas read Feather"
        super().__init__(shape, astype='pandas')
        self._path += '.feather'

    def run(self):
        feather.read_feather(self._path, memory_map=True)

    def setup(self):
        super().setup()
        return self

    def __enter__(self):
        CHUNKSIZE = 128 * 1024**2  # bytes
        feather.write_feather(self._df, self._path,
                              compression="uncompressed",
                              chunksize=CHUNKSIZE)
        return self
