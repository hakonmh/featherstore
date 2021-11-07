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


def create_database(path, errors="raise"):
    """Creates a new database.

    Parameters
    ----------
    path : str
        Where to create the database.
    errors : str, optional
        Whether or not to raise an error if the database directory already exist.
        Can be either 'raise' or 'ignore', 'ignore' tries to create a database
        in existing directory, by default 'raise'
    """
    _can_create_database(path, errors)
    path = expand_home_dir_modifier(path)
    if not os.path.exists(path):
        os.mkdir(path)
    _make_database_marker(path)


def _make_database_marker(db_path):
    """A database marker is used to tell FeatherStore that db_path is a database directory"""
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


class Connection:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "instance"):
            cls.instance = super(Connection, cls).__new__(cls)
        return cls.instance

    def __init__(self, connection_string):
        _can_connect(connection_string)
        path = expand_home_dir_modifier(connection_string)
        self._location = os.path.abspath(path)

    @classmethod
    def disconnect(cls):
        _can_disconnect(cls)
        delattr(cls, "instance")

    @classmethod
    def location(cls):
        _can_fetch_current_db(cls)
        return cls.instance._location


def _can_create_database(db_path, errors):
    _utils.check_if_arg_errors_is_valid(errors)

    if not isinstance(db_path, str):
        raise TypeError(f"Database path must be str, is {type(db_path)}")

    db_path = expand_home_dir_modifier(db_path)
    directory_exists = os.path.exists(db_path)
    if directory_exists:
        directory_is_not_empty = len(os.listdir(db_path)) > 0
        if directory_is_not_empty and errors == "raise":
            raise OSError("Can not create database in a populated directory")


def _can_connect(connection_string):
    if not isinstance(connection_string, str):
        raise TypeError(
            f"connection_string must be of type str, is {type(connection_string)}"
        )

    path = expand_home_dir_modifier(connection_string)
    path = os.path.abspath(path)
    db_marker_path = os.path.join(path, DB_MARKER_NAME)
    is_database = os.path.exists(db_marker_path)
    if not is_database:
        raise ConnectionRefusedError(f"{connection_string} is not a database")


def _can_disconnect(connection_cls):
    if not hasattr(connection_cls, "instance"):
        raise ConnectionError("No connection to disconnect from")


def _can_fetch_current_db(connection_cls):
    if not hasattr(connection_cls, "instance"):
        raise ConnectionError("Not connected to a database")
