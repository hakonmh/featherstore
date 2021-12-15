import os
import pickle

from featherstore import _utils

METADATA_FOLDER_NAME = ".metadata"


class Metadata:
    def __init__(self, path, file_name=None):
        _can_init_metadata(path, file_name)
        self._metadata_folder = os.path.join(path, METADATA_FOLDER_NAME)
        self.db_path = os.path.join(self._metadata_folder, file_name)

    def create(self):
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)
            _utils.mark_as_hidden(self._metadata_folder)

    def read(self):
        values = {key: self[key] for key in self.keys()}
        return values

    def keys(self):
        keys = sorted(os.listdir(self.db_path))
        return keys

    def write(self, new_data: dict):
        _can_write_metadata(new_data)
        for key, value in new_data.items():
            self[key] = value

    def __getitem__(self, key: str):
        path = os.path.join(self.db_path, key)
        with open(path, "rb") as f:
            value = pickle.loads(f.read())
        return value

    def __setitem__(self, key: str, value):
        path = os.path.join(self.db_path, key)
        with open(path, "wb") as f:
            f.write(pickle.dumps(value))

    def __delitem__(self, key: str):
        path = os.path.join(self.db_path, key)
        os.remove(path)

    def __repr__(self):
        return self.db_path


def _can_init_metadata(base_path, db_name):
    if not isinstance(base_path, str):
        raise TypeError("Metadata 'base_path' must be of type 'str'")

    if not isinstance(db_name, (str, type(None))):
        raise TypeError("Metadata 'db_name' must be of type 'str' or 'None'")


def _can_write_metadata(data):
    if not isinstance(data, dict):
        raise TypeError("Metadata must be of type 'dict'")
