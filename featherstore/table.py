import os

from featherstore.connection import current_db
from featherstore._metadata import Metadata
from featherstore import _utils
from featherstore.snapshot import _create_snapshot

from featherstore._table import read
from featherstore._table import write
from featherstore._table import append
from featherstore._table import update
from featherstore._table import insert
from featherstore._table import drop
from featherstore._table import add_cols
from featherstore._table import rename_cols
from featherstore._table import astype
from featherstore._table import misc
from featherstore._table import common

DEFAULT_PARTITION_SIZE = 128 * 1024**2


class Table:
    def __init__(self, table_name, store_name):
        """A class for saving and loading DataFrames as partitioned Feather files.

        Tables supports several operations that can be done without loading in the
        full data:

        - Partial reading of data
        - Append data
        - Insert data
        - Update data
        - Drop data
        - Read metadata (column names, index, table shape, etc)
        - Changing column types
        - Changing types

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
        cols : Collection, optional
            List of column names or, filter-predicates in the form of
            `{'like': pattern}`, by default `None`
        rows : Collection, optional
            List of index values or filter-predicates in the form of
            `{keyword: value}`, where keyword can be either `before`, `after`,
            or `between`, by default `None`

        Returns
        -------
        pyarrow.Table
        """
        read.can_read_table(self, cols, rows)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        has_default_index = self._table_data["has_default_index"]
        stored_cols = self._table_data["columns"]

        cols = common.format_cols_arg(cols, like=stored_cols)
        rows = common.format_rows_arg(rows, to_dtype=index_type)

        partition_names = read.get_partition_names(self, rows)
        df = read.read_table(self, partition_names, cols, rows)
        if has_default_index and rows.values() is None:
            df = read.drop_default_index(df, index_name)

        return df

    def read_pandas(self, *, cols=None, rows=None):
        """Reads the data as a Pandas DataFrame

        Parameters
        ----------
        cols : Collection, optional
            List of column names or filter-predicates in the form of
            `{'like': pattern}`, by default `None`
        rows : Collection, optional
            List of index values or, filter-predicates in the form of
            `{keyword: value}`, where keyword can be either `before`, `after`,
            or `between`, by default `None`

        Returns
        -------
        pandas.DataFrame or pandas.Series
        """
        df = self.read_arrow(cols=cols, rows=rows)
        df = df.to_pandas(date_as_object=False)

        if read.can_be_converted_to_series(df):
            df = df.squeeze()
        if read.can_be_converted_to_rangeindex(df):
            df.index = read.make_rangeindex(df)

        return df

    def read_polars(self, *, cols=None, rows=None):
        """Reads the data as a Polars DataFrame

        Parameters
        ----------
        cols : Collection, optional
            List of column names or filter-predicates in the form of
            `{'like': pattern}`, by default `None`
        rows : Collection, optional
            List of index values or, filter-predicates in the form of
            `{keyword: value}`, where keyword can be either `before`, `after`,
            or `between`, by default `None`

        Returns
        -------
        polars.DataFrame
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
            The size of each partition in bytes. A `partition_size` value of `-1`
            disables partitioning, by default 128 MB
        errors : str, optional
            Whether or not to raise an error if the table already exist. Can be either
            `raise` or `ignore`, `ignore` overwrites existing table, by default `raise`
        warnings : str, optional
            Whether or not to warn if a unsorted index is about to get sorted.
            Can be either `warn` or `ignore`, by default `warn`
        """
        write.can_write_table(self, df, index, partition_size, errors, warnings)

        df = common.format_table(df, index, warnings)
        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        partitions = write.create_partitions(df, rows_per_partition)

        metadata = write.generate_metadata(partitions, partition_size,
                                           rows_per_partition)

        self.drop_table()
        self._create_table()
        write.write_metadata(self, metadata)
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
        append.can_append_table(self, df, warnings)

        index_name = self._table_data["index_name"]
        has_default_index = self._table_data["has_default_index"]
        rows_per_partition = self._table_data["rows_per_partition"]
        last_partition_name = self._partition_data.keys()[-1]

        df = common.format_table(df, index_name, warnings)
        if has_default_index:
            df = append.format_default_index(self, df)
        last_partition = read.read_table(self, [last_partition_name], edit_mode=True)

        df = append.append_data(df, to=last_partition)
        partitions = append.create_partitions(df, rows_per_partition,
                                              last_partition_name)

        metadata = common.update_metadata(self, partitions, [last_partition_name])

        write.write_metadata(self, metadata)
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
        update.can_update_table(self, df)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]

        rows = common.format_rows_arg(df.index, to_dtype=index_type)

        partition_names = read.get_partition_names(self, rows)
        stored_df = read.read_table(self, partition_names, edit_mode=True)

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
        insert.can_insert_table(self, df)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]
        all_partition_names = self._partition_data.keys()

        rows = common.format_rows_arg(df.index, to_dtype=index_type)

        df = common.format_table(df, index_name=index_name, warnings='ignore')
        has_default_index = insert.has_still_default_index(self, df)

        partition_names = read.get_partition_names(self, rows)
        stored_df = read.read_table(self, partition_names, edit_mode=True)

        df = insert.insert_data(df, to=stored_df)
        partitions = insert.create_partitions(df, rows_per_partition, partition_names,
                                              all_partition_names)

        metadata = common.update_metadata(self, partitions, partition_names,
                                          has_default_index=has_default_index)

        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def add_columns(self, df, idx=-1):
        """Insert one or more columns into the current table.

        Parameters
        ----------
        df : Pandas DataFrame or Pandas Series
            The data to be inserted. `df` must have the same index as the stored data.
        idx : int
            The position to insert the new column(s). Default is to add columns to
            the end.
        """
        add_cols.can_add_columns(self, df)

        index_name = self._table_data["index_name"]
        partition_size = self._table_data["partition_size"]

        partition_names = read.get_partition_names(self, None)
        stored_df = read.read_table(self, partition_names, edit_mode=True)

        df = add_cols.add_columns(stored_df, df, index=idx)
        df = common.format_table(df, index_name=index_name, warnings=False)

        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        columns = df.column_names
        partitions = add_cols.create_partitions(df, rows_per_partition, partition_names)

        metadata = common.update_metadata(self, partitions, partition_names,
                                          rows_per_partition=rows_per_partition,
                                          columns=columns)

        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def drop(self, *, cols=None, rows=None):
        """Drop specified labels from rows or columns.

        cols : Collection, optional
            list of column names or filter-predicates in the form of
            `{'like': pattern}`, by default `None`
        rows : Collection, optional
            list of index values or, filter-predicates in the form of
            `{keyword: value}`, where keyword can be either `before`, `after`,
            or `between`, by default `None`

        Raises
        ------
        AttributeError
            Raised if neither of `rows` and `cols` are provided.
        """
        neither_of_rows_and_cols_are_provided = cols is None and rows is None
        if neither_of_rows_and_cols_are_provided:
            raise AttributeError("Neither 'rows' or 'cols' is provided")
        if rows is not None:
            self.drop_rows(rows)
        if cols is not None:
            self.drop_columns(cols)

    def drop_rows(self, rows):
        """Drops specified rows from table

        Same as `Table.drop(rows=value)`
        """
        drop.can_drop_rows_from_table(self, rows)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]

        rows = common.format_rows_arg(rows, to_dtype=index_type)

        partition_names = drop.get_partition_names(self, rows)
        stored_df = read.read_table(self, partition_names, edit_mode=True)

        df = drop.drop_rows_from_data(stored_df, rows, index_name)
        df = common.format_table(df, index_name=index_name, warnings=False)
        partitions = drop.create_partitions(df, rows_per_partition, partition_names)

        has_default_index = drop.has_still_default_index(self, rows)
        metadata = common.update_metadata(self, partitions, partition_names,
                                          has_default_index=has_default_index)

        partitions_to_drop = drop.get_partitions_to_drop(partitions, partition_names)
        drop.drop_partitions(self, partitions_to_drop)
        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def drop_columns(self, cols):
        """Drops specified rows from table

        Same as `Table.drop(cols=value)`
        """
        drop.can_drop_cols_from_table(self, cols)

        index_name = self._table_data["index_name"]
        partition_size = self._table_data["partition_size"]
        stored_cols = self._table_data["columns"]

        cols = common.format_cols_arg(cols, like=stored_cols)

        partition_names = drop.get_partition_names(self, None)
        stored_df = read.read_table(self, partition_names, edit_mode=True)

        df = drop.drop_cols_from_data(stored_df, cols)
        df = common.format_table(df, index_name=index_name, warnings=False)

        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        columns = df.column_names
        partitions = drop.create_partitions(df, rows_per_partition, partition_names)

        metadata = common.update_metadata(self, partitions, partition_names,
                                          rows_per_partition=rows_per_partition,
                                          columns=columns)

        partitions_to_drop = drop.get_partitions_to_drop(partitions, partition_names)
        drop.drop_partitions(self, partitions_to_drop)
        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def rename_columns(self, cols, *, to=None):
        """Rename one or more columns.

        `rename_columns` supports two different call-syntaxes:

        - `rename_columns({'c1': 'new_c1', 'c2': 'new_c2'})`
        - `rename_columns(['c1', 'c2'], to=['new_c1', 'new_c2'])`

        Parameters
        ----------
        cols : Collection
            Either a list of columns to be renamed, or a dict mapping columns
            to be renamed to new column names
        to : Collection[str], optional
            New column names, by default `None`
        """
        rename_cols.can_rename_columns(self, cols, to)

        index_name = self._table_data["index_name"]
        rows_per_partition = self._table_data["rows_per_partition"]

        cols_mapping = common.format_cols_and_to_args(cols, to)

        partition_names = read.get_partition_names(self, None)
        df = read.read_table(self, partition_names, edit_mode=True)

        df = rename_cols.rename_columns(df, cols_mapping)
        df = common.format_table(df, index_name=index_name, warnings=False)
        partitions = write.create_partitions(df, rows_per_partition, partition_names)

        rename_cols.write_metadata(self, partitions)
        write.write_partitions(partitions, self._table_path)

    @property
    def columns(self):
        """Fetches the names of the table columns

        Returns
        -------
        list
            The table columns
        """
        return self._table_data["columns"]

    @columns.setter
    def columns(self, cols):
        """Same as `Table.reorder_columns(values)`

        *Note*: You can not use this method to rename columns, use `rename_columns`
        instead.

        Parameters
        ----------
        cols : Sequence[str]
            The new column ordering. The column names provided must be the
            same as the column names used in the table.
        """
        misc.can_reorder_columns(self, cols)
        self._table_data["columns"] = cols

    def reorder_columns(self, cols):
        """Reorder the current columns

        Parameters
        ----------
        cols : Sequence[str]
            The new column ordering. The column names provided must be the
            same as the column names used in the table.
        """
        self.columns = cols

    @property
    def index(self):
        """Fetches the table index

        Returns
        -------
        pandas.Index
        """
        index = self.read_arrow(cols=[])
        index = index.to_pandas().index
        return index

    def astype(self, cols, *, to=None):
        """Change data type of one or more columns.

        `astype` supports two different call-syntaxes:

        - `astype({'c1': pa.int64(), 'c2': pa.int16()})`
        - `astype(['c1', 'c2'], to=[pa.int64(), pa.int16()])`

        Parameters
        ----------
        cols : Sequence[str] or dict
            Either a sequence of columns to have its data types changed, or a
            dict mapping columns to new column data types.
        to : Sequence[Pyarrow DataType], optional
            New column data types, by default `None`
        """
        astype.can_change_type(self, cols, to)
        index_name = self._table_data["index_name"]
        partition_size = self._table_data["partition_size"]

        astype_mapping = common.format_cols_and_to_args(cols, to)

        partition_names = read.get_partition_names(self, None)
        df = read.read_table(self, partition_names, edit_mode=True)

        df = astype.change_type(df, astype_mapping)
        df = common.format_table(df, index_name=index_name, warnings=False)

        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        partitions = astype.create_partitions(df, rows_per_partition, partition_names)

        metadata = common.update_metadata(self, partitions, partition_names,
                                          rows_per_partition=rows_per_partition)

        partitions_to_drop = astype.get_partitions_to_drop(partitions, partition_names)
        drop.drop_partitions(self, partitions_to_drop)

        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

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
        self._table_data = Metadata(self._table_path, "table")
        self._partition_data = Metadata(self._table_path, "partition")

    def create_snapshot(self, path):
        """Creates a compressed backup of the table.

        The table can later be restored by using `snapshot.restore_table()`.

        Parameters
        ----------
        path : str
            The path to the snapshot archive.
        """
        _create_snapshot(path, self._table_path, 'table')

    def repartition(self, new_partition_size):
        """Repartitions a table so that each partition is `new_partition_size` big.

        Parameters
        ----------
        new_partition_size : int
            The size of each partition in bytes. A `new_partition_size` value of `-1`
            disables partitioning
        """
        df = self.read_arrow()
        has_default_index = self._table_data["has_default_index"]
        if has_default_index:
            index_name = None
        else:
            index_name = self._table_data["index_name"]
        self.write(df, index=index_name, partition_size=new_partition_size,
                   errors='ignore')

    @property
    def shape(self):
        """Fetches the shape of the stored table as `(rows, columns)`.

        Returns
        -------
        tuple(int, int)
            The shape of the table
        """
        rows = self._table_data["num_rows"]
        cols = self._table_data["num_columns"]
        return (rows, cols)

    @property
    def partition_size(self):
        """Fetches the table partition size in bytes.

        Returns
        -------
        int
            The partition size in bytes.
        """
        return self._table_data["partition_size"]

    def exists(self):
        return os.path.exists(self._table_path)

    @property
    def name(self):
        table_name = os.path.split(self._table_path)[-1]
        return table_name

    def _create_table(self):
        os.makedirs(self._table_path)
        self._table_data.create()
        self._partition_data.create()
