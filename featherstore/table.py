import os
import polars as pl

from .connection import current_db
from . import _metadata
from ._metadata import Metadata
from . import _utils

from ._table.read import (
    can_read_table,
    format_rows,
    get_partition_names,
    format_cols,
    read_partitions,
    filter_table_rows,
    drop_default_index,
    can_be_converted_to_rangeindex,
    convert_to_rangeindex,
)
from ._table.write import (
    can_write_table,
    calculate_rows_per_partition,
    make_partitions,
    assign_id_to_partitions,
    write_partitions,
)
from ._table.append import (
    can_append_table,
    format_default_index,
    sort_columns,
    delete_last_partition,
)
from ._table.common import (
    can_init_table,
    can_rename_table,
    combine_partitions,
    format_table,
)


class Table:
    def __init__(self, table_name, store):
        """A class for saving and loading DataFrames as partitioned Feather files.

        Tables supports several operations that can be done without loading in the
        full data:

        - Predicate Filtering when reading
        - Appends
        - Fetching Columns
        - Fetching Index

        It will also support the following operations down the line:

        - Inserts
        - Updates
        - Remove columns and rows
        - Changing types
        - Changing index

        Parameters
        ----------
        table_name : str
            The name of the table
        store : str
            The name of the store
        """
        can_init_table(table_name, store)

        self.table_name = table_name
        self.store = store
        self._table_path = f"{current_db()}/{store}/{table_name}"
        self._table_exists = os.path.exists(self._table_path)
        self._table_data = Metadata(self._table_path, "table")
        self._partition_data = Metadata(self._table_path, "partition")

    def read(self, *, cols=None, rows=None):
        """Reads the data as a PyArrow Table

        Parameters
        ----------
        cols : list, optional
            list of column names or, filter-predicates in the form of
            [like, pattern], by default None
        rows : list, optional
            list of index values or, filter-predicates in the form of
            [keyword, value], where keyword can be either 'before', 'after',
            or 'between', by default None
        """
        can_read_table(cols, rows, self._table_exists)

        index_col_name = self._table_data["index_name"]
        has_default_index = self._table_data["has_default_index"]
        index_type = self._table_data["index_dtype"]

        rows = format_rows(rows, index_type)
        cols = format_cols(cols, self._table_data)

        partition_names = get_partition_names(self, rows)
        partitions = read_partitions(partition_names, self._table_path, cols)
        df = combine_partitions(partitions)
        df = filter_table_rows(df, rows, index_col_name)
        if has_default_index and rows is None:
            df = drop_default_index(df, index_col_name)

        return df

    def read_pandas(self, *, cols, rows):
        """Reads the data as a Pandas DataFrame

        Parameters
        ----------
        cols : list, optional
            list of column names or, filter-predicates in the form of
            [like, pattern], by default None
        rows : list, optional
            list of index values or, filter-predicates in the form of
            [keyword, value], where keyword can be either 'before', 'after',
            or 'between', by default None
        """
        df = self.read(cols=cols, rows=rows)
        df = df.to_pandas()
        if can_be_converted_to_rangeindex(df):
            df = convert_to_rangeindex(df)
        return df

    def read_polars(self, *, cols, rows):
        """Reads the data as a Polars DataFrame

        Parameters
        ----------
        cols : list, optional
            list of column names or, filter-predicates in the form of
            [like, pattern], by default None
        rows : list, optional
            list of index values or, filter-predicates in the form of
            [keyword, value], where keyword can be either 'before', 'after',
            or 'between', by default None
        """
        df = self.read(cols=cols, rows=rows)
        df = pl.from_arrow(df)
        return df

    def write(
        self,
        df,
        /,
        index=None,
        *,
        partition_size=None,
        errors="raise",
        warnings="warn",
    ):
        """Writes a DataFrame to the current table.

        The DataFrame index column, if provided, must be either of type int, str,
        or datetime. FeatherStore sorts the DataFrame by the index before storage.

        Parameters
        ----------
        df : Pandas DataFrame or Series, Pyarrow Table, or Polars DataFrame
            The DataFrame to be stored
        index : str, optional
            The name of the column to be used as index. Uses current index for
            Pandas or a standard integer index for Arrow and Polars if 'index' not
            provided, by default None
        partition_size : int, optional
            The size of each partition in bytes, by default 128 MB
        errors : str, optional
            Whether or not to raise an error if the table already exist. Can be either
            'raise' or 'ignore', 'ignore' overwrites existing table, by default 'raise'
        warnings : str, optional
            Whether or not to warn if a unsorted index is about to get sorted.
            Can be either 'warn' or 'ignore', by default 'warn'
        """
        can_write_table(
            df,
            index,
            errors,
            warnings,
            partition_size,
            self._table_exists,
            self.table_name,
        )

        self.drop_table()
        formatted_df = format_table(df, index, warnings)
        rows_per_partition = calculate_rows_per_partition(formatted_df, partition_size)
        partitioned_df = make_partitions(formatted_df, rows_per_partition)
        partitioned_df = assign_id_to_partitions(partitioned_df)

        collected_metadata = (partition_size, rows_per_partition)
        table_metadata = _metadata.make_table_metadata(
            partitioned_df, collected_metadata
        )
        partition_metadata = _metadata.make_partition_metadata(partitioned_df)

        self._create_table()
        self._table_data.write(table_metadata)
        self._partition_data.write(partition_metadata)
        write_partitions(partitioned_df, self._table_path)

    def append(self, df, *, warnings="warn"):
        """Appends data to the current table

        Parameters
        ----------
        df : Pandas DataFrame or Series, Pyarrow Table, or Polars DataFrame
            The data to be appended
        warnings : str, optional
            Whether or not to warn if a unsorted index is about to get sorted.
            Can be either 'warn' or 'ignore', by default 'warn'
        """
        index_col_name = self._table_data["index_name"]
        df = format_table(df, index_col_name, warnings)

        can_append_table(
            df,
            warnings,
            self._table_path,
            self._table_exists,
        )

        has_default_index = self._table_data["has_default_index"]
        if has_default_index:
            df = format_default_index(df, self._table_path)
        df = sort_columns(df, self._table_data)

        last_partition_min = self._partition_data["min"][-1]
        last_partition = self.read(rows=["after", last_partition_min])
        df = combine_partitions([last_partition, df])

        rows_per_partition = self._table_data["rows_per_partition"]
        partitioned_df = make_partitions(df, rows_per_partition)
        partitioned_df = assign_id_to_partitions(partitioned_df)

        last_partition_name = self._partition_data["name"][-1]
        _metadata.append_metadata(partitioned_df, self._table_path)
        delete_last_partition(self._table_path, last_partition_name)
        write_partitions(partitioned_df, self._table_path)

    def _update(self, rows, values, edit_index=False):
        raise NotImplementedError

    def _insert(self, rows, values):
        raise NotImplementedError

    def _drop(self, cols=None, rows=None):
        raise NotImplementedError

    def _add_columns(self, cols):
        raise NotImplementedError

    @property
    def columns(self):
        """Returns the table columns"""
        return self._table_data["columns"]

    @columns.setter
    def columns(self, new_cols):
        raise NotImplementedError

    def _rename_columns(self, cols, new_col_names=None):
        raise NotImplementedError

    def _astype(self, cols, dtypes):
        raise NotImplementedError

    @property
    def index(self):
        """Returns the table index"""
        index = self.read(cols=[])
        return index.to_pandas().index

    def _set_index(self, new_index, drop_old=False):
        raise NotImplementedError

    def _reset_index(self, drop_old=False):
        raise NotImplementedError

    def rename_table(self, new_table_name):
        """Renames the current table

        Parameters
        ----------
        new_table_name : str
            The new name of the table.
        """
        new_path = f"{current_db()}/{self.store}/{new_table_name}"
        can_rename_table(new_table_name, self._table_path, new_path)

        os.rename(self._table_path, new_path)
        self.table_name = new_table_name
        self._table_path = new_path

    def drop_table(self, skip_bin=False):
        """Deletes the current table"""
        _utils.delete_folder_tree(self._table_path)

    def read_table_metadata(self, item=None):
        if item:
            metadata = self._table_data[item]
        else:
            metadata = self._table_data.read()
        return metadata

    def read_partition_metadata(self, item=None):
        if item:
            metadata = self._partition_data[item]
        else:
            metadata = self._partition_data.read()
        return metadata

    def _create_table(self):
        os.makedirs(self._table_path)
        self._table_data.create()
        self._partition_data.create()
