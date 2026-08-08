import pyarrow as pa

from featherstore._table._table_utils import get_next_item
from featherstore.exceptions import PartitionCountMismatchError

PARTITION_NAME_LENGTH = 14
INSERTION_BUFFER_LENGTH = 10**6


def make_partitions(df, rows_per_partition):
    df = df.combine_chunks()
    if rows_per_partition == -1:
        partitions = _make_single_partition(df)
    else:
        partitions = df.to_batches(rows_per_partition)
        partitions = _combine_small_partitions(partitions, rows_per_partition)
        if len(partitions) == 0:
            partitions = [pa.RecordBatch.from_pylist(df.to_pylist(), df.schema)]
    return partitions


def _make_single_partition(df):
    return df.to_batches()


def _combine_small_partitions(partitions, partition_size):
    has_multiple_partitions = len(partitions) > 1
    try:
        size_of_last_partition = partitions[-1].num_rows
    except IndexError:
        size_of_last_partition = 0
    min_partition_size = partition_size * 0.5

    if has_multiple_partitions and size_of_last_partition < min_partition_size:
        new_last_partition = _combine_last_two_partitions(partitions)
        partitions = _replace_last_two_partitions(new_last_partition, partitions)
    return partitions


def _combine_last_two_partitions(partitions):
    last_partition = pa.Table.from_batches(partitions[-2:])
    last_partition = last_partition.combine_chunks()
    return last_partition.to_batches()


def _replace_last_two_partitions(new_last_partition, partitions):
    partitions = partitions[:-2]
    partitions.extend(new_last_partition)
    return partitions


def convert_int_to_partition_id(partition_id):
    partition_id = int(partition_id * INSERTION_BUFFER_LENGTH)
    format_string = f"0{PARTITION_NAME_LENGTH}d"
    partition_id = format(partition_id, format_string)
    return partition_id


def convert_partition_id_to_float(partition_id):
    return int(partition_id) / INSERTION_BUFFER_LENGTH


def convert_partition_id_to_int(partition_id):
    return int(partition_id) // INSERTION_BUFFER_LENGTH


def add_new_partition_ids(partitions, partition_ids):
    partition_ids = partition_ids.copy()
    num_new_partition_ids = len(partitions) - len(partition_ids) + 1
    partition_ids = append_new_partition_ids(num_new_partition_ids, partition_ids)
    return sorted(partition_ids)


def append_new_partition_ids(num_partitions, partition_ids):
    last_partition_id = partition_ids[-1]

    range_start = convert_partition_id_to_int(last_partition_id) + 1
    range_end = range_start + num_partitions - 1

    for partition_num in range(range_start, range_end):
        partition_id = convert_int_to_partition_id(partition_num)
        partition_ids.append(partition_id)
    return partition_ids


def assign_ids_to_partitions(df, ids):
    if len(df) != len(ids):
        raise PartitionCountMismatchError(
            f"Num partitions doesn't match num partition names "
            f"({len(df)} != {len(ids)})"
        )
    id_mapping = {}
    for identifier, partition in zip(ids, df):
        id_mapping[identifier] = partition
    return id_mapping


def create_partitions(
    df,
    rows_per_partition,
    partition_names=None,
    *,
    strategy="reuse",
    all_partition_names=None,
):
    partitions = make_partitions(df, rows_per_partition)
    names = resolve_partition_names(
        partitions,
        partition_names,
        strategy=strategy,
        all_partition_names=all_partition_names,
    )
    return assign_ids_to_partitions(partitions, names)


def resolve_partition_names(
    partitions, partition_names, *, strategy, all_partition_names=None
):
    if strategy == "new":
        return _make_new_partition_ids(partitions)
    if strategy == "reuse":
        return partition_names
    if strategy == "grow":
        return _grow_partition_names(partitions, partition_names)
    if strategy == "shrink":
        return partition_names[: len(partitions)]
    if strategy == "append":
        last_partition_name = _as_last_partition_name(partition_names)
        return append_new_partition_ids(len(partitions), [last_partition_name])
    if strategy == "insert":
        return _insert_partition_ids(partitions, partition_names, all_partition_names)
    raise ValueError(f"Unknown partition naming strategy: {strategy!r}")


def _make_new_partition_ids(partitions):
    partition_ids = []
    for partition_num in range(1, len(partitions) + 1):
        partition_ids.append(convert_int_to_partition_id(partition_num))
    return partition_ids


def _grow_partition_names(partitions, partition_ids):
    if len(partitions) < len(partition_ids):
        return partition_ids[: len(partitions)]
    return add_new_partition_ids(partitions, partition_ids)


def _as_last_partition_name(partition_names):
    if isinstance(partition_names, str):
        return partition_names
    return partition_names[-1]


def _insert_partition_ids(partitions, partition_names, all_partition_names):
    num_names_to_make = len(partitions) - len(partition_names)
    subsequent_partition = get_next_item(
        item=partition_names[-1], sequence=all_partition_names
    )
    return _make_insert_partition_names(
        num_names_to_make, partition_names, subsequent_partition
    )


def _make_insert_partition_names(num_names, partition_names, subsequent_partition):
    last_id = convert_partition_id_to_float(partition_names[-1])
    if subsequent_partition is not None:
        subsequent_id = convert_partition_id_to_float(subsequent_partition)
        increment = (subsequent_id - last_id) / (num_names + 1)
    else:
        increment = 1

    new_partition_names = partition_names.copy()
    for partition_num in range(1, num_names + 1):
        new_partition_id = last_id + increment * partition_num
        new_partition_names.append(convert_int_to_partition_id(new_partition_id))
    return sorted(new_partition_names)


def get_first_stored_index_value(partition_metadata):
    first_partition = partition_metadata.keys()[0]
    first_stored_value = partition_metadata[first_partition]["min"]
    return first_stored_value


def get_last_stored_index_value(partition_metadata):
    last_partition = partition_metadata.keys()[-1]
    last_stored_value = partition_metadata[last_partition]["max"]
    return last_stored_value
