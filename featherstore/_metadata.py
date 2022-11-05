import os
import pickle
from featherstore import _utils

METADATA_FOLDER_NAME = ".metadata"


class Metadata():

    def __init__(self, path, file_name):
        _can_init_metadata(path, file_name)
        self._metadata_folder = os.path.join(path, METADATA_FOLDER_NAME)
        self._db_path = os.path.join(self._metadata_folder, file_name) + '.db'
        self.index = KeyIndex(self._metadata_folder, file_name)

    def create(self):
        if not os.path.exists(self._metadata_folder):
            os.makedirs(self._metadata_folder)
            _utils.mark_as_hidden(self._metadata_folder)
            _utils.touch(self._db_path, flag='ab')

    def write(self, new_data: dict):
        _can_write_metadata(new_data)
        byte_offsets = []
        with open(self._db_path, "ab") as f:
            for key, value in new_data.items():
                byte_offset = self._write_item(f, value)
                byte_offsets.append((key, byte_offset))
        self.index.write(byte_offsets)
        self._compact()

    def keys(self):
        return self.index.keys()

    def read(self):
        items = dict()
        with open(self._db_path, "rb") as f:
            for key in self.keys():
                byte_offset = self.index[key]
                f.seek(byte_offset)
                value = pickle.load(f)
                items[key] = value
        return items

    def __getitem__(self, key: str):
        with open(self._db_path, "rb") as f:
            byte_offset = self.index[key]
            f.seek(byte_offset)
            value = pickle.load(f)
            return value

    def __setitem__(self, key: str, value):
        with open(self._db_path, "ab") as f:
            byte_offsets = self._write_item(f, value)
        self.index[key] = byte_offsets
        self._compact()

    def __delitem__(self, key: str):
        del self.index[key]
        self._compact()

    def __len__(self):
        return self.index._db_size

    def _write_item(self, f, value):
        byte_offset = f.tell()
        pickle.dump(value, f)
        return byte_offset

    def _compact(self):
        if len(self) > len(self.index) * 2:
            items = self.read()
            os.remove(self._db_path)
            self.index._db_size = 0
            self.write(items)


class KeyIndex:

    def __init__(self, metadata_folder, file_name):
        self._path = os.path.join(metadata_folder, file_name) + '.index'
        if os.path.exists(self._path):
            self._data, self._db_size = self._read_data()
        else:
            self._data = {}
            self._db_size = 0

    def write(self, items):
        self._data.update(dict(items))
        self._db_size += len(items)
        self._write_data()

    def keys(self):
        return sorted(self._data.keys())

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value
        self._db_size += 1
        self._write_data()

    def __delitem__(self, key):
        del self._data[key]
        self._write_data()

    def __len__(self):
        return len(self._data)

    def _write_data(self):
        with open(self._path, 'wb') as f:
            pickle.dump(self._data, f)
            pickle.dump(self._db_size, f)

    def _read_data(self):
        with open(self._path, 'rb') as f:
            data = pickle.load(f)
            size = pickle.load(f)
        return data, size


def _can_init_metadata(base_path, db_name):
    if not isinstance(base_path, str):
        raise TypeError("Metadata 'base_path' must be of type 'str'")

    if not isinstance(db_name, (str, type(None))):
        raise TypeError("Metadata 'db_name' must be of type 'str' or 'None'")


def _can_write_metadata(data):
    if not isinstance(data, dict):
        raise TypeError("Metadata must be of type 'dict'")
