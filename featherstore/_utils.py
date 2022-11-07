import os
from pathlib import Path
import shutil
import platform
import subprocess
import ctypes
import re

DB_MARKER_NAME = ".featherstore"
DEFAULT_ARROW_INDEX_NAME = "__index_level_0__"


def touch(path, flag='ab'):
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
        raise PermissionError(f"Can't delete files outside the database ({path})")


def _is_in_database(path, db_path):
    path = Path(path)
    db_path = Path(db_path)
    return db_path in path.parents or path == db_path


def __delete_folder_tree(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass
    except PermissionError as e:
        # Force delete stubborn open file on Windows
        cmd = ["del", "/f", "/a", f"{e.filename}"]
        output = subprocess.run(cmd, shell=True, check=True, capture_output=True).stderr.decode()
        if output.startswith('The process cannot access the file'):
            raise e
        else:
            # Try to delete folder with stubborn file deleted
            __delete_folder_tree(path)


def expand_home_dir_modifier(path):
    return os.path.expanduser(path)


def filter_items_like_pattern(items, *, like):
    pattern = _sql_str_pattern_to_regexp(like)
    results = _filter(items, like=pattern)
    return results


def _sql_str_pattern_to_regexp(pattern):
    if pattern[0] != "%":
        pattern = "^" + pattern
    if pattern[-1] != "%":
        pattern = pattern + "$"
    pattern = pattern.replace("?", ".")
    pattern = pattern.replace("%", ".*")

    pattern = pattern.lower()
    return re.compile(pattern)


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
