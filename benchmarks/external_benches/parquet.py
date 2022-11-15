from ._fixtures import OtherIO
import pandas as pd
import os


class pd_write_parquet(OtherIO):

    def __init__(self, shape):
        super().__init__(shape, astype='pandas')
        self.name = "Pandas write Parquet"
        self._path += '.parquet'

    def run(self):
        self._df.to_parquet(self._path, engine='pyarrow', compression=None,
                            data_page_size=1024, use_dictionary=False,
                            row_group_size=512 * 1024**2)


class pd_read_parquet(OtherIO):

    def __init__(self, shape):
        self.name = "Pandas read Parquet"
        super().__init__(shape, astype='pandas')
        self._path += '.parquet'

    def run(self):
        pd.read_parquet(self._path, engine='pyarrow', memory_map=True)

    def setup(self):
        super().setup()
        self._df.to_parquet(self._path, engine='pyarrow', compression=None,
                            data_page_size=1024, use_dictionary=False,
                            row_group_size=512 * 1024**2)
        del self._df

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        pass

    def teardown(self):
        if os.path.exists(self._path):
            os.remove(self._path)
        return super().teardown()
