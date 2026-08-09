import json
import os

from featherstore._utils import (
    DB_MARKER_NAME,
    METADATA_SCHEMA_VERSION,
    PARTITION_LAYOUT_VERSION,
    mark_as_hidden,
)
from featherstore.exceptions import IncompatibleDatabaseVersionError

_METADATA_SCHEMA_VERSION_KEY = "metadata_schema_version"
_PARTITION_LAYOUT_VERSION_KEY = "partition_layout_version"


def write_database_marker(db_path):
    db_marker_path = os.path.join(db_path, DB_MARKER_NAME)
    payload = {
        _METADATA_SCHEMA_VERSION_KEY: METADATA_SCHEMA_VERSION,
        _PARTITION_LAYOUT_VERSION_KEY: PARTITION_LAYOUT_VERSION,
    }
    with open(db_marker_path, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    mark_as_hidden(db_marker_path)


def assert_database_compatible(db_path):
    metadata_schema_version, partition_layout_version = _read_database_versions(db_path)
    db_version_not_compatible = (
        metadata_schema_version != METADATA_SCHEMA_VERSION
        or partition_layout_version != PARTITION_LAYOUT_VERSION
    )
    if db_version_not_compatible:
        raise IncompatibleDatabaseVersionError(
            _incompatible_version_message(
                metadata_schema_version, partition_layout_version
            )
        )


def _read_database_versions(db_path):
    db_marker_path = os.path.join(db_path, DB_MARKER_NAME)
    try:
        with open(db_marker_path, encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError):
        raise IncompatibleDatabaseVersionError(
            _incompatible_version_message(None, None)
        ) from None
    return __parse_database_versions(payload)


def __parse_database_versions(payload):
    if not isinstance(payload, dict):
        raise IncompatibleDatabaseVersionError(
            _incompatible_version_message(None, None)
        )
    metadata_schema_version = payload.get(_METADATA_SCHEMA_VERSION_KEY)
    partition_layout_version = payload.get(_PARTITION_LAYOUT_VERSION_KEY)
    if not isinstance(metadata_schema_version, int) or not isinstance(
        partition_layout_version, int
    ):
        raise IncompatibleDatabaseVersionError(
            _incompatible_version_message(
                metadata_schema_version, partition_layout_version
            )
        )
    return metadata_schema_version, partition_layout_version


def _incompatible_version_message(metadata_schema_version, partition_layout_version):
    return (
        "Database format versions are incompatible with this FeatherStore install "
        f"(metadata_schema_version {metadata_schema_version!r}, expected "
        f"{METADATA_SCHEMA_VERSION}; partition_layout_version "
        f"{partition_layout_version!r}, expected {PARTITION_LAYOUT_VERSION})"
    )
