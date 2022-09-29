import os
from featherstore import _utils
from featherstore._utils import mark_as_hidden, expand_home_dir_modifier

DB_MARKER_NAME = ".featherstore"


def connect(connection_string):
    """Connects to a database.

    Parameters
    ----------
    connection_string : str
        Path to the database directory
    """
    Connection(connection_string)


def disconnect():
    """Disconnects from the current database."""
    Connection.disconnect()


def create_database(path, *, errors="raise", connect=True):
    """Creates a new database.

    Parameters
    ----------
    path : str
        Where to create the database.
    errors : str, optional
        Whether or not to raise an error if the database directory already exist.
        Can be either `raise` or `ignore`, `ignore` tries to create a database
        in existing directory, by default `raise`
    connect : bool
        Whether or not to connect to the created database, by default True
    """
    _can_create_database(path, errors)
    path = expand_home_dir_modifier(path)
    if not os.path.exists(path):
        os.mkdir(path)
    _make_database_marker(path)
    if connect:
        Connection(path)


def _make_database_marker(db_path):
    """A database marker tells FeatherStore that `db_path` is a database directory"""
    db_marker_path = os.path.join(db_path, DB_MARKER_NAME)
    open(db_marker_path, "a").close()
    mark_as_hidden(db_marker_path)


def current_db():
    """Fetches the active database.

    Returns
    -------
    str
        The current database directory
    """
    return Connection.location()


def is_connected():
    """Checks if FeatherStore is connected to a database."""
    return Connection.is_connected()


def database_exists(path):
    db_marker_path = os.path.join(path, DB_MARKER_NAME)
    if os.path.exists(db_marker_path):
        return True
    else:
        return False


class Connection:
    def __new__(cls, *args, **kwargs):
        cls.instance = super(Connection, cls).__new__(cls)
        return cls.instance

    def __init__(self, connection_string):
        _can_connect(connection_string)
        path = expand_home_dir_modifier(connection_string)
        self._location = os.path.abspath(path)

    @classmethod
    def disconnect(cls):
        cls._raise_if_not_connected()
        delattr(cls, "instance")

    @classmethod
    def location(cls):
        cls._raise_if_not_connected()
        return cls.instance._location

    @classmethod
    def is_connected(cls):
        if hasattr(cls, "instance"):
            location = cls.instance._location
            if database_exists(location):
                return True
        return False

    @classmethod
    def _raise_if_not_connected(cls):
        if not cls.is_connected():
            raise ConnectionError("Not connected to a database")


def _can_create_database(db_path, errors):
    _utils.raise_if_errors_argument_is_not_valid(errors)
    _raise_if_db_path_is_not_string(db_path)
    if errors == "raise":
        _raise_if_directory_is_empty(db_path)


def _raise_if_db_path_is_not_string(db_path):
    if not isinstance(db_path, str):
        raise TypeError(f"'db_path' must be str, is {type(db_path)}")


def _raise_if_directory_is_empty(db_path):
    db_path = expand_home_dir_modifier(db_path)
    directory_exists = os.path.exists(db_path)
    if directory_exists:
        directory_is_not_empty = len(os.listdir(db_path)) > 0
        if directory_is_not_empty:
            raise OSError("Can not create database in a populated directory")


def _can_connect(connection_string):
    _raise_if_connection_str_is_not_string(connection_string)
    _raise_if_directory_is_not_database(connection_string)


def _raise_if_connection_str_is_not_string(connection_string):
    if not isinstance(connection_string, str):
        raise TypeError(
            f"'connection_string' must be of type str, is {type(connection_string)}"
        )


def _raise_if_directory_is_not_database(connection_string):
    path = expand_home_dir_modifier(connection_string)
    path = os.path.abspath(path)
    db_marker_path = os.path.join(path, DB_MARKER_NAME)
    is_database = os.path.exists(db_marker_path)
    if not is_database:
        raise ConnectionRefusedError(f"{connection_string} is not a database")
