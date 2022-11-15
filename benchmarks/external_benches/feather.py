from ._fixtures import OtherIO
from pyarrow import feather
import os


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
        CHUNKSIZE = 128 * 1024**2  # bytes
        feather.write_feather(self._df, self._path,
                              compression="uncompressed",
                              chunksize=CHUNKSIZE)
        del self._df

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        pass

    def teardown(self):
        if os.path.exists(self._path):
            os.remove(self._path)
        return super().teardown()
