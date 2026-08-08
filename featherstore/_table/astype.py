import pyarrow as pa

from featherstore._table import _partitions, _raise_if


def can_change_type(table, cols, astype):
    _raise_if.not_connected_or_table_not_exists(table)

    cols = _raise_if.cols_and_to_arguments_are_not_valid(cols, astype)

    _raise_if.cols_argument_items_is_not_str_or_none(cols.keys())
    _raise_if_astype_items_are_not_pa_or_np_types(cols.values())

    _raise_if.col_names_contains_duplicates(cols.keys())
    _raise_if.cols_not_in_table(cols.keys(), table._table_data)
    _raise_if_new_index_type_is_not_valid(cols, table._table_data)


def _raise_if_astype_items_are_not_pa_or_np_types(astype):
    col_elements_are_arrow_types = all(
        isinstance(item, (pa.DataType)) for item in astype
    )
    col_elements_are_np_types = all(__is_valid_dtype(item) for item in astype)

    if not (col_elements_are_arrow_types or col_elements_are_np_types):
        raise TypeError("Elements in 'to' must be Arrow or Numpy types")


def __is_valid_dtype(item):
    try:
        pa.from_numpy_dtype(item)
        return True
    except (TypeError, ValueError, pa.ArrowInvalid):
        return False


def _raise_if_new_index_type_is_not_valid(cols, table_data):
    index_name = table_data["index_name"]
    col_keys = cols.keys()
    if col_keys is not None and index_name in col_keys:
        _raise_if.index_type_not_supported(cols[index_name])


def change_type(df, cols):
    df = df.combine_chunks()

    schema = df.schema
    for col, dtype in cols.items():
        dtype = _convert_to_pa_dtype(dtype)
        idx = schema.get_field_index(col)
        field = schema.field(idx)
        field = field.with_type(dtype)
        schema = schema.set(idx, field)
    df = df.cast(schema)
    return df


def _convert_to_pa_dtype(dtype):
    if __is_valid_dtype(dtype):
        dtype = pa.from_numpy_dtype(dtype)
    return dtype


def create_partitions(df, rows_per_partition, partition_names=None):
    return _partitions.create_partitions(
        df, rows_per_partition, partition_names, strategy="grow"
    )


def get_partitions_to_drop(partitions, stored_names):
    partition_names = partitions.keys()
    partitions_to_drop = set(stored_names) - set(partition_names)
    return sorted(partitions_to_drop)
