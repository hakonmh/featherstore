import os
import platform
import shutil
import re

DEFAULT_ARROW_INDEX_NAME = "__index_level_0__"


def mark_as_hidden(path):
    is_windows = platform.system() == "Windows"
    if is_windows:
        mark_as_hidden_command = f"attrib +h {path}"
        os.system(mark_as_hidden_command)


def delete_folder_tree(path):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def like_pattern_matching(like, str_list):
    like = _sql_str_pattern_to_regexp(like)
    regexp = re.compile(like)
    str_lower_list = [item.lower() for item in str_list]
    filtered_list = set(filter(regexp.search, str_lower_list))
    results = [item for item in str_list if item.lower() in filtered_list]
    return results


def _sql_str_pattern_to_regexp(pattern):
    if pattern[0] != "%":
        pattern = "^" + pattern
    if pattern[-1] != "%":
        pattern = pattern + "$"
    pattern = pattern.replace("_", ".")
    pattern = pattern.replace("%", ".*")
    return pattern.lower()


def check_if_arg_errors_is_valid(errors):
    if errors not in {"raise", "ignore"}:
        raise ValueError("'errors' must be either 'raise' or 'ignore'")


def check_if_arg_warnings_is_valid(warnings):
    if warnings not in {"warn", "ignore"}:
        raise ValueError("'warnings' must be either 'warn' or 'ignore'")
