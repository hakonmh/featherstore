import json
import os

import featherstore as fs


def _marker_path(db_path):
    for name in os.listdir(db_path):
        path = os.path.join(db_path, name)
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(f"No database marker found in {db_path}")


def _read_marker(db_path):
    with open(_marker_path(db_path), encoding="utf-8") as f:
        return json.load(f)


def _replace_marker(db_path, content):
    marker_file = _marker_path(db_path)
    os.remove(marker_file)
    with open(marker_file, "w", encoding="utf-8") as f:
        if isinstance(content, dict):
            json.dump(content, f)
        else:
            f.write(content)


def _create_database(db_path):
    if os.path.exists(db_path):
        raise FileExistsError(db_path)
    fs.create_database(db_path, connect=False)


def setup_incompatible_database(db_path, scenario):
    _create_database(db_path)
    if scenario == "empty_legacy_marker":
        _replace_marker(db_path, "")
        return

    marker = _read_marker(db_path)
    metadata_schema_version = marker["metadata_schema_version"]
    partition_layout_version = marker["partition_layout_version"]

    if scenario == "invalid_json":
        _replace_marker(db_path, "{not json")
    elif scenario == "missing_metadata_schema_version":
        del marker["metadata_schema_version"]
        _replace_marker(db_path, marker)
    elif scenario == "non_integer_metadata_schema_version":
        marker["metadata_schema_version"] = str(metadata_schema_version)
        _replace_marker(db_path, marker)
    elif scenario == "outdated_metadata_schema":
        marker["metadata_schema_version"] = metadata_schema_version - 1
        _replace_marker(db_path, marker)
    elif scenario == "outdated_partition_layout":
        marker["partition_layout_version"] = partition_layout_version - 1
        _replace_marker(db_path, marker)
    elif scenario == "newer_metadata_schema":
        marker["metadata_schema_version"] = metadata_schema_version + 1
        _replace_marker(db_path, marker)
    elif scenario == "newer_partition_layout":
        marker["partition_layout_version"] = partition_layout_version + 1
        _replace_marker(db_path, marker)
    elif scenario == "outdated_format":
        marker["metadata_schema_version"] = metadata_schema_version - 1
        marker["partition_layout_version"] = partition_layout_version - 1
        _replace_marker(db_path, marker)
    else:
        raise ValueError(f"Unknown incompatible database scenario: {scenario}")
