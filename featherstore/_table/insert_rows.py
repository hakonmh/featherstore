import itertools

from featherstore import _utils
from featherstore._table import _partitions, _raise_if, _table_utils


def can_insert_rows(table, df, warnings):
    _raise_if.not_connected_or_table_not_exists(table)
    _utils.raise_if_warnings_argument_is_not_valid(warnings)
    _raise_if.df_is_not_table_type(df, _table_utils.EDIT_TABLE_TYPES)

    table_data = table._table_data
    index_name = table_data["index_name"]
    cols = _table_utils.get_col_names(df, index_name=index_name)
    index = _table_utils.get_index_if_exists(df, index_name)
    _raise_if.incoming_index_schema_incompatible_with_stored(
        df, table_data, cols, index=index
    )
    _raise_if.cols_does_not_match(df, table_data)


def insert_data(df, *, to):
    index_name = _table_utils.get_index_name(df)
    _raise_if.index_values_in_stored_data(to, df, index_name, all_must_be_in=False)

    df = _table_utils.concat_arrow_tables(to, df)
    df = _table_utils.sort_arrow_table(df, by=index_name)
    return df


def create_partitions(df, rows_per_partition, partition_names, all_partition_names):
    return _partitions.create_partitions(
        df,
        rows_per_partition,
        partition_names,
        strategy="insert",
        all_partition_names=all_partition_names,
    )


def has_still_default_index(table, df):
    has_default_index = table._table_data["has_default_index"]
    if not has_default_index:
        return False

    index_name = table._table_data["index_name"]
    rows = df[index_name]
    if len(rows) == 0:
        return True

    last_stored_value = _partitions.get_last_stored_index_value(table._partition_data)
    first_row_value = rows[0].as_py()
    rows_are_continuous = all(
        a.as_py() + 1 == b.as_py() for a, b in itertools.pairwise(rows)
    )
    starts_immediately_after = first_row_value == last_stored_value + 1
    return starts_immediately_after and rows_are_continuous
