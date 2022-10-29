from logging import exception
import os
import pickle
import weakref
from featherstore import _utils
import bisect
import operator


METADATA_FOLDER_NAME = ".metadata"


class Metadata:

    def __init__(self, path, file_name=None):
        _can_init_metadata(path, file_name)
        self._metadata_folder = os.path.join(path, METADATA_FOLDER_NAME)
        self.db_path = os.path.join(self._metadata_folder, file_name)
        self.index = KeyIndex(self.db_path)
        self.db_path += '.db'
        self._counter = self.count()

    def count(self):
        if os.path.exists(self.db_path):
            with open(self.db_path, 'rb') as fp:  # TODO: Make a counter
                count = len(fp.readlines())
        else:
            count = 0
        return count

    def exists(self):
        return os.path.exists(self._metadata_folder)

    def create(self):
        if not self.exists():
            os.makedirs(self._metadata_folder)
            _utils.mark_as_hidden(self._metadata_folder)
            _touch(self.db_path)

    def read(self):
        items = dict()
        with open(self.db_path, "rb") as f:
            for key in self.keys():
                byte_offset = self.index[key]
                f.seek(byte_offset)
                line = f.peek()
                value = pickle.loads(line)
                items[key] = value
        return items

    def keys(self):
        return self.index.keys()

    def write(self, new_data: dict):
        _can_write_metadata(new_data)
        byte_offsets = []
        with open(self.db_path, "ab") as f:
            for key, value in new_data.items():
                byte_offset = self._write_item(f, value)
                byte_offsets.append((key, byte_offset))
                self._counter += 1
        self.index.write(byte_offsets)
        self.compact()

    def _write_item(self, f, value):
        byte_offset = f.tell()
        f.write(pickle.dumps(value))
        return byte_offset

    def __getitem__(self, key: str):
        with open(self.db_path, "rb") as f:
            byte_offset = self.index[key]
            f.seek(byte_offset)
            line = f.peek()
            value = pickle.loads(line)
            return value

    def __setitem__(self, key: str, value):
        with open(self.db_path, "ab") as f:
            byte_offsets = self._write_item(f, value)
        self._counter += 1
        self.index[key] = byte_offsets
        self.compact()

    def __delitem__(self, key: str):
        del self.index[key]

    def __repr__(self):
        return self.db_path

    def __len__(self):
        return len(self.index)

    def compact(self):
        if len(self) * 2 < self._counter:
            items = self.read()
            os.remove(self.db_path)
            self._counter = 0
            self.write(items)


class KeyIndex:
    dbs = weakref.WeakValueDictionary()
    data = weakref.WeakKeyDictionary()

    def __new__(cls, file_path):
        file_path += '.index'
        # if db_files exists: return existing instance, else create new
        if file_path in cls.dbs:
            instance = cls.dbs[file_path]
        else:
            instance = super(KeyIndex, cls).__new__(cls)
        return instance

    def __init__(self, path, /):
        self._path = path + '.index'
        self.__class__.dbs[self._path] = self
        if self.exists():
            self._data = self._read_data()
        else:
            self._data = {}

    def _read_data(self):
        with open(self._path, 'rb') as f:
            data = pickle.load(f)
        return data

    @property
    def _data(self):
        return self.__class__.data[self]

    @_data.setter
    def _data(self, data):
        self.__class__.data[self] = data

    def exists(self):
        return os.path.exists(self._path)

    def write(self, items):
        self._data.update(dict(items))
        self._write_file()

    def keys(self):
        return sorted(self._data.keys())

    def __len__(self):
        return len(self._data)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value
        self._write_file()

    def __delitem__(self, key):
        del self._data[key]
        self._write_file()

    def _write_file(self):
        with open(self._path, 'wb') as f:
            pickle.dump(self._data, f)


def _touch(path):
    with open(path, 'ab'):
        pass


def _can_init_metadata(base_path, db_name):
    if not isinstance(base_path, str):
        raise TypeError("Metadata 'base_path' must be of type 'str'")

    if not isinstance(db_name, (str, type(None))):
        raise TypeError("Metadata 'db_name' must be of type 'str' or 'None'")


def _can_write_metadata(data):
    if not isinstance(data, dict):
        raise TypeError("Metadata must be of type 'dict'")
