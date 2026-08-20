import os
import warnings as _warnings

from featherstore import _utils
from featherstore._db_version import assert_database_compatible, write_database_marker
from featherstore._utils import DB_MARKER_NAME, expand_home_dir_modifier
from featherstore.exceptions import (
    DatabaseNotEmptyError,
    NotADatabaseError,
    NotConnectedError,
    PopulatedDirectoryError,
)


def connect(connection_string):
    """Connects to a database.

    If a connection is already open, it is replaced.

    Parameters
    ----------
    connection_string : str
        Path to the database directory. ``~`` is expanded to the user home
        directory.

    Raises
    ------
    :exc:`~featherstore.exceptions.NotADatabaseError`
        If ``connection_string`` is not a FeatherStore database.
    :exc:`~featherstore.exceptions.IncompatibleDatabaseVersionError`
        If the database format versions are incompatible with this install.
    :exc:`TypeError`
        If ``connection_string`` is not a str.
    """
    Connection(connection_string)


def disconnect():
    """Disconnects from the current database.

    Raises
    ------
    :exc:`~featherstore.exceptions.NotConnectedError`
        If FeatherStore is not connected to a database.
    """
    Connection.disconnect()


def create_database(path, *, errors="raise", connect=True):
    """Creates a new database.

    Parameters
    ----------
    path : str
        Where to create the database. ``~`` is expanded to the user home
        directory.
    errors : str, optional
        Whether to raise an error if the database directory already exists.
        Can be ``'raise'`` or ``'ignore'``; ``'ignore'`` tries to create a
        database in an existing directory. Default is ``'raise'``.
    connect : bool, optional
        Whether to connect to the created database. Default is ``True``.
        A new connection replaces any existing connection.

    Raises
    ------
    :exc:`~featherstore.exceptions.PopulatedDirectoryError`
        If ``errors='raise'`` and the directory is not empty.
    :exc:`TypeError`
        If ``path`` is not a str.
    :exc:`ValueError`
        If ``errors`` is not ``'raise'`` or ``'ignore'``.
    """
    _can_create_database(path, errors)
    path = expand_home_dir_modifier(path)
    if not os.path.exists(path):
        os.mkdir(path)
    if not (errors == "ignore" and database_exists(path)):
        write_database_marker(path)
    if connect:
        Connection(path)


def drop_database(path, *, warnings="warn"):
    """Deletes a database.

    *Warning*: You cannot delete a database containing stores. All stores must
    be deleted first.

    On success, FeatherStore disconnects from the deleted database.

    Parameters
    ----------
    path : str
        Path to the database directory. Must be the currently connected
        database. ``~`` is expanded to the user home directory.
    warnings : str, optional
        Whether to warn if the database does not exist. Can be ``'warn'`` or
        ``'ignore'``. Default is ``'warn'``.

    Raises
    ------
    :exc:`~featherstore.exceptions.NotConnectedError`
        If FeatherStore is not connected to a database.
    :exc:`~featherstore.exceptions.DatabaseNotEmptyError`
        If the database still contains stores.
    :exc:`TypeError`
        If ``path`` is not a str.
    :exc:`ValueError`
        If ``warnings`` is not ``'warn'`` or ``'ignore'``, or if ``path`` is
        not the currently connected database.
    """
    _can_drop_database(path, warnings)
    path = os.path.abspath(expand_home_dir_modifier(path))
    if os.path.exists(path):
        Connection.disconnect()
        _utils.delete_folder_tree(path, path)


def current_db():
    """Returns the path of the active database.

    Returns
    -------
    str
        The current database directory.

    Raises
    ------
    :exc:`~featherstore.exceptions.NotConnectedError`
        If FeatherStore is not connected to a database.
    """
    return Connection.location()


def is_connected():
    """Returns whether FeatherStore is connected to a database.

    Returns
    -------
    bool
    """
    return Connection.is_connected()


def database_exists(path):
    """Returns whether a directory is a FeatherStore database.

    Parameters
    ----------
    path : str
        Path to the directory to check. ``~`` is expanded to the user home
        directory.

    Returns
    -------
    bool
        ``True`` if the directory contains a FeatherStore database marker.
    """
    path = expand_home_dir_modifier(path)
    db_marker_path = os.path.join(path, DB_MARKER_NAME)
    return os.path.exists(db_marker_path)


class Connection:
    def __init__(self, connection_string):
        _can_connect(connection_string)
        path = expand_home_dir_modifier(connection_string)
        self._location = os.path.abspath(path)
        type(self).instance = self

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
            raise NotConnectedError("Not connected to a database")


def _can_create_database(db_path, errors):
    _utils.raise_if_errors_argument_is_not_valid(errors)
    _raise_if_db_path_is_not_string(db_path)
    if errors == "raise":
        _raise_if_directory_is_empty(db_path)


def _can_drop_database(db_path, warnings):
    Connection._raise_if_not_connected()
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _raise_if_db_path_is_not_string(db_path)
    _raise_if_path_is_not_current_database(db_path)
    _raise_if_database_contains_stores()
    if not database_exists(db_path) and warnings == "warn":
        _warnings.warn(f"Database doesn't exist: '{db_path}'")


def _raise_if_path_is_not_current_database(db_path):
    path = os.path.abspath(expand_home_dir_modifier(db_path))
    if path != current_db():
        raise ValueError("'path' must be the currently connected database")


def _raise_if_database_contains_stores():
    if _utils.list_stores(current_db):
        raise DatabaseNotEmptyError("Can't delete a database that contains stores")


def _raise_if_db_path_is_not_string(db_path):
    if not isinstance(db_path, str):
        raise TypeError(f"'db_path' must be str, is {type(db_path)}")


def _raise_if_directory_is_empty(db_path):
    db_path = expand_home_dir_modifier(db_path)
    directory_exists = os.path.exists(db_path)
    if directory_exists:
        directory_is_not_empty = len(os.listdir(db_path)) > 0
        if directory_is_not_empty:
            raise PopulatedDirectoryError(
                "Can not create database in a populated directory"
            )


def _can_connect(connection_string):
    _raise_if_connection_str_is_not_string(connection_string)
    _raise_if_directory_is_not_database(connection_string)
    _raise_if_database_not_compatible(connection_string)


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
        raise NotADatabaseError(f"{connection_string} is not a database")


def _raise_if_database_not_compatible(connection_string):
    path = expand_home_dir_modifier(connection_string)
    path = os.path.abspath(path)
    assert_database_compatible(path)
