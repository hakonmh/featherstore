import os

from featherstore.connection import current_db
from featherstore._metadata import Metadata
from featherstore import _utils

from featherstore._table import read
from featherstore._table import write
from featherstore._table import append
from featherstore._table import update
from featherstore._table import insert
from featherstore._table import drop
from featherstore._table import add_cols
from featherstore._table import rename_cols
from featherstore._table import misc
from featherstore._table import common

DEFAULT_PARTITION_SIZE = 128 * 1024**2


class Table:
    def __init__(self, table_name, store_name):
        """A class for saving and loading DataFrames as partitioned Feather files.

        Tables supports several operations that can be done without loading in the
        full data:

        - Predicate Filtering when reading
        - Appends
        - Fetching Column names
        - Fetching Index
        - Updates
        - Inserts
        - Drop columns/Drop rows

        It will also support the following operations down the line:

        - Changing types
        - Changing index

        Parameters
        ----------
        table_name : str
            The name of the table
        store_name : str
            The name of the store
        """
        misc.can_init_table(table_name, store_name)

        self._table_path = os.path.join(current_db(), store_name, table_name)
        self._table_data = Metadata(self._table_path, "table")
        self._partition_data = Metadata(self._table_path, "partition")

    def read_arrow(self, *, cols=None, rows=None):
        """Reads the data as a PyArrow Table

        Parameters
        ----------
        cols : list, optional
            list of column names or, filter-predicates in the form of
            `[like, pattern]`, by default `None`
        rows : list, optional
            list of index values or filter-predicates in the form of
            `[keyword, value]`, where keyword can be either `before`, `after`,
            or `between`, by default `None`
        """
        read.can_read_table(cols, rows, self._table_path)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        has_default_index = self._table_data["has_default_index"]
        stored_cols = self._table_data["columns"]

        rows = common.format_rows_arg_if_provided(rows, index_type)
        cols = common.filter_cols_if_like_provided(cols, stored_cols)

        partition_names = read.get_partition_names(rows, self._table_path)
        df = read.read_table(partition_names, self._table_path, cols, rows)
        if has_default_index and rows is None:
            df = read.drop_default_index(df, index_name)

        return df

    def read_pandas(self, *, cols=None, rows=None):
        """Reads the data as a Pandas DataFrame

        Parameters
        ----------
        cols : list, optional
            list of column names or filter-predicates in the form of
            `[like, pattern]`, by default `None`
        rows : list, optional
            list of index values or, filter-predicates in the form of
            `[keyword, value]`, where keyword can be either `before`, `after`,
            or `between`, by default `None`
        """
        df = self.read_arrow(cols=cols, rows=rows)
        df = df.to_pandas()

        if read.can_be_converted_to_series(df):
            df = df.squeeze()
        if read.can_be_converted_to_rangeindex(df):
            df.index = read.make_rangeindex(df)

        return df

    def read_polars(self, *, cols=None, rows=None):
        """Reads the data as a Polars DataFrame

        Parameters
        ----------
        cols : list, optional
            list of column names or filter-predicates in the form of
            `[like, pattern]`, by default `None`
        rows : list, optional
            list of index values or, filter-predicates in the form of
            `[keyword, value]`, where keyword can be either `before`, `after`,
            or `between`, by default `None`
        """
        df = self.read_arrow(cols=cols, rows=rows)
        df = read.convert_table_to_polars(df)
        return df

    def write(self, df, /, index=None, *,
              partition_size=DEFAULT_PARTITION_SIZE,
              errors="raise", warnings="warn"):
        """Writes a DataFrame to the current table.

        The DataFrame index column, if provided, must be either of type int, str,
        or datetime. FeatherStore sorts the DataFrame by the index before storage.

        Parameters
        ----------
        df : Pandas DataFrame or Series, Pyarrow Table, or Polars DataFrame
            The DataFrame to be stored
        index : str, optional
            The name of the column to be used as index. Uses current index for
            Pandas or a standard integer index for Arrow and Polars if `index` not
            provided, by default `None`
        partition_size : int, optional
            The size of each partition in bytes, by default 128 MB
        errors : str, optional
            Whether or not to raise an error if the table already exist. Can be either
            `raise` or `ignore`, `ignore` overwrites existing table, by default `raise`
        warnings : str, optional
            Whether or not to warn if a unsorted index is about to get sorted.
            Can be either `warn` or `ignore`, by default `warn`
        """
        write.can_write_table(df, self._table_path, index,
                              partition_size, errors, warnings)

        self.drop_table()

        df = common.format_table(df, index, warnings)
        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        partitions = write.create_partitions(df, rows_per_partition)

        metadata = write.generate_metadata(partitions, partition_size,
                                           rows_per_partition)

        self._create_table()
        write.write_metadata(metadata, self._table_path)
        write.write_partitions(partitions, self._table_path)

    def append(self, df, *, warnings="warn"):
        """Appends data to the current table

        Parameters
        ----------
        df : Pandas DataFrame or Series, Pyarrow Table, or Polars DataFrame
            The data to be appended
        warnings : str, optional
            Whether or not to warn if a unsorted index is about to get sorted.
            Can be either `warn` or `ignore`, by default `warn`
        """
        append.can_append_table(df, warnings, self._table_path)

        index_name = self._table_data["index_name"]
        has_default_index = self._table_data["has_default_index"]
        rows_per_partition = self._table_data["rows_per_partition"]
        name_last_partition = self._partition_data.keys()[-1]

        df = common.format_table(df, index_name, warnings)
        if has_default_index:
            df = append.format_default_index(df, self._table_path)
        last_partition = read.read_table([name_last_partition],
                                         self._table_path,
                                         edit_mode=True)

        df = append.append_data(df, to=last_partition)
        partitions = append.create_partitions(df, rows_per_partition,
                                              name_last_partition)

        metadata = common.update_metadata(partitions, self._table_path,
                                          [name_last_partition])

        write.write_metadata(metadata, self._table_path)
        write.write_partitions(partitions, self._table_path)

    def update(self, df):
        """Updates data in the current table.

        *Note*: You can't use this method to update index values. Updating index
        values can be accomplished by deleting the old records and inserting new
        ones with the updated index values.

        Parameters
        ----------
        df : Pandas DataFrame or Pandas Series
            The updated data. The index of `df` is the rows to be updated, while
            the columns of `df` are the new values.
        """
        update.can_update_table(df, self._table_path)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]

        rows = common.format_rows_arg_if_provided(df.index, index_type)

        partition_names = read.get_partition_names(rows, self._table_path)
        stored_df = read.read_table(partition_names, self._table_path, edit_mode=True)

        df = update.update_data(stored_df, to=df)
        df = common.format_table(df, index_name=index_name, warnings=False)
        partitions = write.create_partitions(df, rows_per_partition, partition_names)

        write.write_partitions(partitions, self._table_path)

    def insert(self, df):
        """Insert one or more rows into the current table.

        Parameters
        ----------
        df : Pandas DataFrame or Pandas Series
            The data to be inserted. `df` must have the same index and column
            types as the stored data.
        """
        insert.can_insert_table(df, self._table_path)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]

        rows = common.format_rows_arg_if_provided(df.index, index_type)

        partition_names = read.get_partition_names(rows, self._table_path)
        stored_df = read.read_table(partition_names, self._table_path, edit_mode=True)

        df = insert.insert_data(stored_df, to=df)
        df = common.format_table(df, index_name=index_name, warnings='ignore')
        partitions = insert.create_partitions(df, rows_per_partition, partition_names)

        metadata = common.update_metadata(partitions, self._table_path, partition_names)

        write.write_metadata(metadata, self._table_path)
        write.write_partitions(partitions, self._table_path)

    def drop(self, *, cols=None, rows=None):
        """Drop specified labels from rows or columns.

        cols : list, optional
            list of column names or filter-predicates in the form of
            `[like, pattern]`, by default `None`
        rows : list, optional
            list of index values or, filter-predicates in the form of
            `[keyword, value]`, where keyword can be either `before`, `after`,
            or `between`, by default `None`

        Raises
        ------
        AttributeError
            Raised if both or neither of `rows` and `cols` are provided.
        """
        both_rows_and_cols_is_provided = cols is not None and rows is not None
        if both_rows_and_cols_is_provided:
            raise AttributeError("Can't drop both rows and columns at the same time")
        elif rows is not None:
            self.drop_rows(rows)
        elif cols is not None:
            self.drop_columns(cols)
        else:
            raise AttributeError("Neither 'rows' or 'cols' is provided")

    def drop_rows(self, rows):
        """Drops specified rows from table

        Same as `Table.drop(rows=val)`
        """
        drop.can_drop_rows_from_table(rows, self._table_path)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]

        rows = common.format_rows_arg_if_provided(rows, index_type)
        partition_names = drop.get_partition_names(rows, self._table_path)

        stored_df = read.read_table(partition_names, self._table_path, edit_mode=True)

        df = drop.drop_rows_from_data(stored_df, rows, index_name)
        df = common.format_table(df, index_name=index_name, warnings=False)
        partitions = drop.create_partitions(df, rows_per_partition, partition_names)

        metadata = common.update_metadata(partitions, self._table_path, partition_names,
                                          has_default_index=False)

        partitions_to_drop = drop.get_partitions_to_drop(partitions, partition_names)
        drop.drop_partitions(self._table_path, partitions_to_drop)
        write.write_metadata(metadata, self._table_path)
        write.write_partitions(partitions, self._table_path)

    def drop_columns(self, cols):
        """Drops specified rows from table

        Same as `Table.drop(cols=val)`
        """
        drop.can_drop_cols_from_table(cols, self._table_path)

        index_name = self._table_data["index_name"]
        partition_size = self._table_data["partition_size"]
        stored_cols = self._table_data["columns"]

        cols = common.filter_cols_if_like_provided(cols, stored_cols)
        partition_names = drop.get_partition_names(None, self._table_path)

        stored_df = read.read_table(partition_names, self._table_path, edit_mode=True)

        df = drop.drop_cols_from_data(stored_df, cols)
        df = common.format_table(df, index_name=index_name, warnings=False)

        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        columns = df.column_names
        partitions = drop.create_partitions(df, rows_per_partition, partition_names)

        metadata = common.update_metadata(partitions, self._table_path, partition_names,
                                          rows_per_partition=rows_per_partition,
                                          columns=columns)

        partitions_to_drop = drop.get_partitions_to_drop(partitions, partition_names)
        drop.drop_partitions(self._table_path, partitions_to_drop)
        write.write_metadata(metadata, self._table_path)
        write.write_partitions(partitions, self._table_path)

    def add_columns(self, df, idx=-1):
        """Add one or more columns to the table.

        Parameters
        ----------
        df : Pandas DataFrame or Pandas Series
            The data to be inserted. `df` must have the same index as the stored data.
        idx : int
            The position to insert the new column(s). Default is to add columns to
            the end.
        """
        add_cols.can_add_columns(df, self._table_path)

        index_name = self._table_data["index_name"]
        partition_size = self._table_data["partition_size"]

        partition_names = read.get_partition_names(None, self._table_path)
        stored_df = read.read_table(partition_names, self._table_path, edit_mode=True)

        df = add_cols.add_columns(stored_df, df, index=idx)
        df = common.format_table(df, index_name=index_name, warnings=False)

        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        columns = df.column_names
        partitions = add_cols.create_partitions(df, rows_per_partition, partition_names)

        metadata = common.update_metadata(partitions, self._table_path, partition_names,
                                          rows_per_partition=rows_per_partition,
                                          columns=columns)

        write.write_metadata(metadata, self._table_path)
        write.write_partitions(partitions, self._table_path)

    def rename_columns(self, cols, *, to=None):
        """Rename one or more columns.

        `rename_columns` supports two different call-syntaxes:

        - `rename_columns({'c1': 'new_c1', 'c2': 'new_c2'})`
        - `rename_columns(['c1', 'c2'], to=['new_c1', 'new_c2'])`

        Parameters
        ----------
        cols : list or dict
            Either a list of columns to be renamed, or a dict mapping columns
            to be renamed to new column names
        to : list, optional
            New column names, by default `None`
        """
        rename_cols.can_rename_columns(cols, to, self._table_path)

        index_name = self._table_data["index_name"]
        rows_per_partition = self._table_data["rows_per_partition"]

        partition_names = read.get_partition_names(None, self._table_path)
        df = read.read_table(partition_names, self._table_path, edit_mode=True)

        df = rename_cols.rename_columns(df, cols, to)
        df = common.format_table(df, index_name=index_name, warnings=False)
        partitions = write.create_partitions(df, rows_per_partition, partition_names)

        rename_cols.write_metadata(partitions, self._table_path)
        write.write_partitions(partitions, self._table_path)

    @property
    def columns(self):
        """Fetches the names of the table columns

        Returns
        -------
        columns : list
        """
        return self._table_data["columns"]

    @columns.setter
    def columns(self, cols):
        """Reorder the current columns

        *Note*: You can't use this setter method to rename columns, use
        `rename_columns` instead.

        Parameters
        ----------
        cols : list[str]
            The new column ordering. The column names provided must be the
            same as the column names used in the table.
        """
        misc.can_reorganize_columns(cols, self._table_path)
        self._table_data["columns"] = cols

    @property
    def index(self):
        """Fetches the table index

        Returns
        -------
        index : Pandas Index
        """
        index = self.read_arrow(cols=[])
        index = index.to_pandas().index
        return index

    def _set_index(self, new_index, drop_old=False):
        raise NotImplementedError

    def _reset_index(self, drop_old=False):
        raise NotImplementedError

    def _astype(self, cols, dtypes):
        raise NotImplementedError

    def rename_table(self, *, to):
        """Renames the current table

        Parameters
        ----------
        to : str
            The new name of the table.
        """
        new_table_name = to
        store_path = os.path.split(self._table_path)[0]
        new_path = os.path.join(store_path, new_table_name)
        misc.can_rename_table(new_table_name, new_path)

        os.rename(self._table_path, new_path)
        self._table_path = new_path

    def drop_table(self):
        """Deletes the current table"""
        _utils.delete_folder_tree(self._table_path)

    @property
    def shape(self):
        """Fetches the shape of the stored table as `(rows, cols)`

        Returns
        -------
        shape : tuple(int, int)
        """
        cols = self._table_data["num_cols"]
        rows = self._table_data["num_rows"]
        return (rows, cols)

    @property
    def exists(self):
        return os.path.exists(self._table_path)

    def _create_table(self):
        os.makedirs(self._table_path)
        self._table_data.create()
        self._partition_data.create()
