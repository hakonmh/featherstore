import bmark
import fixtures as fx
import os
import shutil

BENCH_DIR = 'db'
BENCH_MARKER = '.bmark'


class OtherIO(bmark.Benched):

    def __init__(self, shape, astype):
        self._shape = shape
        self._astype = astype
        self._path = f'{BENCH_DIR}/table_name'
        super().__init__()

    def setup(self):
        self._df = fx.make_table(self._shape, astype=self._astype)
        _setup()

    def teardown(self):
        _teardown()

    def __exit__(self, exception_type, exception_value, traceback):
        if os.path.exists(self._path):
            os.remove(self._path)


def _setup():
    if os.path.exists(BENCH_DIR):
        raise FileExistsError('Database already exists')
    os.makedirs(BENCH_DIR)
    with open(os.path.join(BENCH_DIR, BENCH_MARKER), 'a'):
        pass


def _teardown():
    if BENCH_MARKER not in os.listdir(BENCH_DIR):
        raise PermissionError("Can't delete files outside the benchmark directory")
    shutil.rmtree(BENCH_DIR)
