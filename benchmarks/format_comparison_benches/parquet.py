import pandas as pd

from ._fixtures import OtherIO, ReadFileIO

PARQUET_KWARGS = {
    "engine": "pyarrow",
    "compression": None,
    "data_page_size": 1024,
    "use_dictionary": False,
    "row_group_size": 512 * 1024**2,
}


class PandasWriteParquet(OtherIO):
    def __init__(self, shape):
        super().__init__(shape, astype="pandas")
        self.name = "Pandas write Parquet"
        self._path += ".parquet"

    def run(self):
        self._df.to_parquet(self._path, **PARQUET_KWARGS)


class PandasReadParquet(ReadFileIO):
    def __init__(self, shape):
        super().__init__(shape, extension=".parquet", name="Pandas read Parquet")

    def run(self):
        pd.read_parquet(self._path, engine="pyarrow", memory_map=True)

    def _write_file(self):
        self._df.to_parquet(self._path, **PARQUET_KWARGS)
