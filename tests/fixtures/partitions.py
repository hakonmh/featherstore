"""Inspect how a stored table is split into partitions.

Put helpers that read partition bounds, resolve which partitions a read opens,
or compare partition metadata against the partition files on disk here.
"""

import itertools
import os
from collections import namedtuple

import pyarrow as pa
from pyarrow import ipc

from featherstore._table.common import format_rows_arg
from featherstore._table.read import get_partition_names

Partition = namedtuple("Partition", ["name", "min", "max", "num_rows"])


def partition_layout(table):
    """The bounds of every partition of `table`, ordered by partition id."""
    partition_data = table._partition_data.read()
    return [
        Partition(name, bounds["min"], bounds["max"], bounds["num_rows"])
        for name, bounds in partition_data.items()
    ]


def partition_names(partitions):
    """The partition ids of `partitions`."""
    return [partition.name for partition in partitions]


def pruned_partitions(table, rows):
    """The partitions a read of `rows` narrows down to."""
    rows = format_rows_arg(rows, to_dtype=table._table_data["index_dtype"])
    return get_partition_names(table, rows)


def assert_partition_metadata_matches_files(table):
    """The partition metadata describes exactly the partition files on disk."""
    partitions = partition_layout(table)

    assert partition_names(partitions) == _stored_partition_names(table)
    assert table._table_data["num_partitions"] == len(partitions)
    assert table._table_data["num_rows"] == sum(p.num_rows for p in partitions)

    for partition in partitions:
        index = _read_partition_index(table, partition.name)
        assert partition.min == index[0]
        assert partition.max == index[-1]
        assert partition.num_rows == len(index)


def assert_partition_bounds_are_ordered(table):
    """Every partition starts strictly after the preceding one ends."""
    partitions = partition_layout(table)
    for partition, next_partition in itertools.pairwise(partitions):
        assert partition.max < next_partition.min


def _stored_partition_names(table):
    file_names = os.listdir(table._table_path)
    return sorted(
        os.path.splitext(name)[0] for name in file_names if name.endswith(".feather")
    )


def _read_partition_index(table, partition_name):
    path = os.path.join(table._table_path, f"{partition_name}.feather")
    index_name = table._table_data["index_name"]
    with pa.OSFile(path, "r") as source:
        partition = ipc.open_file(source).read_all()
    return partition[index_name].to_pylist()
