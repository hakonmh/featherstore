import ctypes
import os
import platform
import re
import time
from pathlib import Path

from featherstore._windows_posix import _remove_path_posix
from featherstore.exceptions import ForbiddenStoreNameError, UnsafeDeletePathError

DB_MARKER_NAME = ".featherstore"
METADATA_SCHEMA_VERSION = 1
PARTITION_LAYOUT_VERSION = 1

DEFAULT_ARROW_INDEX_NAME = "__index_level_0__"

_WINDOWS_DELETE_RETRIES = 10
_WINDOWS_DELETE_BACKOFF_S = 0.001


def touch(path, flag="ab"):
    with open(path, flag):
        pass


def mark_as_hidden(path):
    FILE_ATTRIBUTE_HIDDEN = 0x02
    is_windows = platform.system() == "Windows"
    if is_windows:
        success = ctypes.windll.kernel32.SetFileAttributesW(path, FILE_ATTRIBUTE_HIDDEN)
        if not success:
            raise ctypes.WinError()


def delete_folder_tree(path, db_path):
    if _is_in_database(path, db_path):
        __delete_folder_tree(path)
    else:
        raise UnsafeDeletePathError(f"Can't delete files outside the database ({path})")


def _is_in_database(path, db_path):
    path = Path(path).resolve()
    db_path = Path(db_path).resolve()
    return path == db_path or db_path in path.parents


def raise_if_store_name_is_invalid(store_name):
    if not isinstance(store_name, str):
        raise TypeError(f"'store_name' must be a str, is type {type(store_name)}")
    if store_name == DB_MARKER_NAME or _is_unsafe_path_name(store_name):
        raise ForbiddenStoreNameError(f"Store name {store_name!r} is forbidden")


def _is_unsafe_path_name(name):
    return name in {"", ".", ".."} or "/" in name or "\\" in name


def __delete_folder_tree(path):
    try:
        rmtree(path)
    except FileNotFoundError:
        pass


def _remove_path(path):
    for attempt in range(_WINDOWS_DELETE_RETRIES):
        try:
            _remove_path_posix(path)
            return
        except FileNotFoundError:
            return
        except PermissionError:
            is_last_attempt = attempt == _WINDOWS_DELETE_RETRIES - 1
            if platform.system() != "Windows" or is_last_attempt:
                raise
            time.sleep(_WINDOWS_DELETE_BACKOFF_S * (attempt + 1))


def rmtree(path):
    if __isfile(path):
        _remove_path(path)
    else:
        for sub_path in os.listdir(path):
            sub_path = f"{path}/{sub_path}"
            rmtree(sub_path)
        os.rmdir(path)


def __isfile(path):
    return os.stat(path).st_mode & 0o170000 == 0o100000


def expand_home_dir_modifier(path):
    return os.path.expanduser(path)


def list_stores(current_db, like=None):
    db_content = os.listdir(current_db())
    if like:
        db_content = filter_items_like_pattern(db_content, like=like)
    stores = []
    for item in db_content:
        path = os.path.join(current_db(), item)
        if os.path.isdir(path):
            stores.append(item)
    stores.sort()
    return stores


def filter_items_like_pattern(items, *, like):
    pattern = _sql_str_pattern_to_regexp(like)
    results = _filter(items, like=pattern)
    return results


def _sql_str_pattern_to_regexp(pattern):
    parts = []
    for char in pattern.lower():
        if char == "%":
            parts.append(".*")
        elif char == "?":
            parts.append(".")
        else:
            parts.append(re.escape(char))
    regex = "".join(parts)
    if not pattern.startswith("%"):
        regex = f"^{regex}"
    if not pattern.endswith("%"):
        regex = f"{regex}$"
    return re.compile(regex)


def _filter(items, *, like):
    str_lower_list = [item.lower() for item in items]
    filtered_list = set(filter(like.search, str_lower_list))
    results = [item for item in items if item.lower() in filtered_list]
    return results


def raise_if_errors_argument_is_not_valid(errors):
    if errors not in {"raise", "ignore"}:
        raise ValueError("'errors' must be either 'raise' or 'ignore'")


def raise_if_warnings_argument_is_not_valid(warnings):
    if warnings not in {"warn", "ignore"}:
        raise ValueError("'warnings' must be either 'warn' or 'ignore'")
