import os

from featherstore import _utils
from featherstore._metadata import Metadata
from featherstore._table import (
    append,
    astype,
    common,
    drop,
    insert,
    insert_cols,
    insert_rows,
    misc,
    read,
    rename_cols,
    update,
    write,
)
from featherstore.connection import current_db
from featherstore.snapshot import _create_snapshot

DEFAULT_PARTITION_SIZE = 128 * 1024**2


class Table:
    """Saves and loads DataFrames as partitioned Feather files.

    Tables support several operations that can be done without loading the
    full dataset:

    - Partial reading of data
    - Append data
    - Insert rows and columns
    - Update data
    - Drop data
    - Read metadata (column names, index, table shape, etc)
    - Changing column types

    The table does not need to exist when this object is created. Call
    :meth:`write` to create it. The store must already exist.
    """

    def __init__(self, table_name, store_name):
        """Selects a table in a store.

        Parameters
        ----------
        table_name : str
            The name of the table.
        store_name : str
            The name of the store.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.StoreNotFoundError`
            If the store does not exist.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``table_name`` is reserved or not a valid path name.
        :exc:`TypeError`
            If ``table_name`` or ``store_name`` is not a str.
        """
        misc.can_init_table(table_name, store_name)

        self._table_path = os.path.join(current_db(), store_name, table_name)
        self._table_data = Metadata(self._table_path, "table")
        self._partition_data = Metadata(self._table_path, "partition")

    def read_arrow(self, *, cols=None, rows=None, mmap=None):
        """Reads the data as a PyArrow Table.

        A named index is returned as a column. A default integer index is
        omitted from the result.

        Parameters
        ----------
        cols : Collection, optional
            List of column names or a filter predicate ``{'like': pattern}``.
            ``pattern`` uses SQL wildcards (``?`` matches one character, ``%``
            matches any number of characters) and is case-insensitive. If not
            provided, all columns are read.
        rows : Collection, optional
            List of index values or a filter predicate ``{keyword: value}``,
            where keyword can be ``before``, ``after``, or ``between``. Range
            bounds are inclusive. For ``between``, ``value`` is a two-element
            sequence ``[start, end]``. If not provided, all rows are read.
        mmap : bool, optional
            Use memory mapping when opening the table on disk. Default is
            ``False`` on Windows and ``True`` on other systems.

        Returns
        -------
        pyarrow.Table

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`~featherstore.exceptions.ColumnNotFoundError`
            If any requested column is not in the table.
        :exc:`~featherstore.exceptions.RowNotFoundError`
            If any requested row is not in the table.
        :exc:`~featherstore.exceptions.IndexTypeMismatchError`
            If row values do not match the table index dtype.
        :exc:`TypeError`
            If ``cols`` or ``rows`` has an invalid type.
        :exc:`ValueError`
            If ``mmap`` is not a bool or ``None``.
        """
        read.can_read_table(self, cols, rows, mmap)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        has_default_index = self._table_data["has_default_index"]
        stored_cols = self._table_data["columns"]

        cols = common.format_cols_arg(cols, like=stored_cols)
        rows = common.format_rows_arg(rows, to_dtype=index_type)

        partition_names = read.get_partition_names(self, rows)
        df = read.read_table(self, partition_names, cols, rows, mmap=mmap)

        if has_default_index and (
            rows.values() is None or common.index_is_default(df[index_name])
        ):
            df = read.drop_default_index(df, index_name)

        return df

    def read_pandas(self, *, cols=None, rows=None, mmap=None):
        """Reads the data as a pandas DataFrame or Series.

        The stored index is restored as a pandas ``Index``. A Series is
        returned when the result has a single data column.

        Parameters
        ----------
        cols : Collection, optional
            See :meth:`read_arrow`.
        rows : Collection, optional
            See :meth:`read_arrow`.
        mmap : bool, optional
            See :meth:`read_arrow`.

        Returns
        -------
        pandas.DataFrame or pandas.Series

        See Also
        --------
        read_arrow : Parameters and exceptions.
        """
        df = self.read_arrow(cols=cols, rows=rows, mmap=mmap)
        df = read.convert_table_to_pandas(df)
        return df

    def read_polars(self, *, cols=None, rows=None, mmap=None):
        """Reads the data as a polars DataFrame or Series.

        A named index is returned as a column. A default integer index is
        omitted. A Series is returned when the result has a single data column.

        Parameters
        ----------
        cols : Collection, optional
            See :meth:`read_arrow`.
        rows : Collection, optional
            See :meth:`read_arrow`.
        mmap : bool, optional
            See :meth:`read_arrow`.

        Returns
        -------
        polars.DataFrame or polars.Series

        See Also
        --------
        read_arrow : Parameters and exceptions.
        """
        df = self.read_arrow(cols=cols, rows=rows, mmap=mmap)
        df = read.convert_table_to_polars(df)
        return df

    def write(
        self,
        df,
        /,
        index=None,
        *,
        partition_size=DEFAULT_PARTITION_SIZE,
        errors="raise",
        warnings="warn",
    ):
        """Writes a DataFrame to the current table.

        The DataFrame index, if provided, must be a supported type: integer,
        unsigned integer, float, decimal, string, binary, duration, or temporal
        (date, time, or timestamp). FeatherStore sorts the DataFrame by the
        index before storage.

        Parameters
        ----------
        df : pandas.DataFrame or pandas.Series, polars.DataFrame or polars.Series, or pyarrow.Table
            The DataFrame to be stored.
        index : str, optional
            The name of the column to be used as index. Uses the current index
            for pandas, or a standard integer index for Arrow and Polars, if
            ``index`` is not provided. Default is ``None``.
        partition_size : int, optional
            The size of each partition in bytes. ``-1`` disables partitioning.
            Default is 128 MB.
        errors : str, optional
            Whether to raise an error if the table already exists. Can be
            ``'raise'`` or ``'ignore'``; ``'ignore'`` overwrites the existing
            table. Default is ``'raise'``.
        warnings : str, optional
            Whether to warn if an unsorted index is about to get sorted. Can be
            ``'warn'`` or ``'ignore'``. Default is ``'warn'``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableAlreadyExistsError`
            If ``errors='raise'`` and the table already exists.
        :exc:`~featherstore.exceptions.DuplicateColumnNamesError`
            If column names are not unique.
        :exc:`~featherstore.exceptions.DuplicateIndexValuesError`
            If index values are not unique.
        :exc:`~featherstore.exceptions.IndexNotInColumnsError`
            If ``index`` is not among the table columns.
        :exc:`~featherstore.exceptions.UnsupportedIndexTypeError`
            If the index type is not supported.
        :exc:`~featherstore.exceptions.MultiTypeColumnError`
            If a column contains multiple dtypes.
        :exc:`TypeError`
            If arguments have invalid types.
        :exc:`ValueError`
            If ``errors`` is not ``'raise'`` or ``'ignore'``, or ``warnings``
            is not ``'warn'`` or ``'ignore'``.
        """
        write.can_write_table(self, df, index, partition_size, errors, warnings)

        df = common.format_table(df, index, warnings)
        rows_per_partition = common.compute_rows_per_partition(df, partition_size)

        partitions = write.create_partitions(df, rows_per_partition)
        metadata = write.generate_metadata(
            partitions, partition_size, rows_per_partition
        )
        self.drop_table(warnings="ignore")
        self._create_table()
        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def append(self, df, *, warnings="warn"):
        """Appends data to the current table.

        Parameters
        ----------
        df : pandas.DataFrame or pandas.Series, polars.DataFrame or polars.Series, or pyarrow.Table
            The data to be appended.
        warnings : str, optional
            Whether to warn if an unsorted index is about to get sorted. Can be
            ``'warn'`` or ``'ignore'``. Default is ``'warn'``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`~featherstore.exceptions.AppendIndexError`
            If append index is not strictly after stored data.
        :exc:`~featherstore.exceptions.ColumnDtypeMismatchError`
            If column dtypes are incompatible.
        :exc:`~featherstore.exceptions.ColumnMismatchError`
            If column names do not match the stored table.
        :exc:`~featherstore.exceptions.DuplicateColumnNamesError`
            If column names are not unique.
        :exc:`~featherstore.exceptions.DuplicateIndexValuesError`
            If index values are not unique.
        :exc:`~featherstore.exceptions.IndexNameMismatchError`
            If the index name does not match the stored table.
        :exc:`~featherstore.exceptions.IndexTypeMismatchError`
            If the index type does not match the stored table.
        :exc:`~featherstore.exceptions.MissingIndexError`
            If an index is required but not provided.
        :exc:`TypeError`
            If ``df`` has an invalid type.
        :exc:`ValueError`
            If ``warnings`` is not ``'warn'`` or ``'ignore'``.
        """
        append.can_append_table(self, df, warnings)

        index_name = self._table_data["index_name"]
        has_default_index = self._table_data["has_default_index"]
        rows_per_partition = self._table_data["rows_per_partition"]
        last_partition_name = self._partition_data.keys()[-1]

        df = common.format_table(df, index_name, warnings)
        if has_default_index:
            if common.index_is_default(df[index_name]):
                df = append.format_default_index(self, df)
            else:
                has_default_index = insert_rows.has_still_default_index(self, df)
        last_partition = read.read_table(self, [last_partition_name])

        df = append.append_data(df, to=last_partition)
        partitions = append.create_partitions(
            df, rows_per_partition, last_partition_name
        )

        metadata = common.update_metadata(
            self, partitions, [last_partition_name], has_default_index=has_default_index
        )

        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def update(self, df):
        """Updates data in the current table.

        *Note*: You cannot use this method to update index values. Updating index
        values can be accomplished by deleting the old records and inserting new
        ones with the updated index values.

        Parameters
        ----------
        df : pandas.DataFrame or pandas.Series, polars.DataFrame, or pyarrow.Table
            The updated data. The index of ``df`` is the rows to be updated, while
            the columns of ``df`` are the new values.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`~featherstore.exceptions.ColumnDtypeMismatchError`
            If column dtypes are incompatible.
        :exc:`~featherstore.exceptions.ColumnNotFoundError`
            If any column is not in the stored table.
        :exc:`~featherstore.exceptions.DuplicateColumnNamesError`
            If column names are not unique.
        :exc:`~featherstore.exceptions.DuplicateIndexValuesError`
            If index values are not unique.
        :exc:`~featherstore.exceptions.IndexNameMismatchError`
            If the index name does not match the stored table.
        :exc:`~featherstore.exceptions.IndexTypeMismatchError`
            If the index type does not match the stored table.
        :exc:`~featherstore.exceptions.RowNotFoundError`
            If any row is not in the stored table.
        :exc:`TypeError`
            If ``df`` is not a supported table type.
        """
        update.can_update_table(self, df)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]

        df = common.format_table(df, index_name=index_name, warnings=False)
        rows = common.format_rows_arg(df[index_name].to_pylist(), to_dtype=index_type)

        partition_names = read.get_partition_names(self, rows)
        stored_df = read.read_table(self, partition_names)

        df = update.update_data(stored_df, to=df)
        partitions = write.create_partitions(df, rows_per_partition, partition_names)

        write.write_partitions(partitions, self._table_path)

    def insert(self, df, *, idx=-1, warnings="warn"):
        """Inserts one or more rows or columns into the current table.

        If ``df`` column names match the stored table, rows are inserted.
        Otherwise, columns are inserted at position ``idx``.

        Also raises the same exceptions as :meth:`insert_rows` and
        :meth:`insert_columns`.

        Parameters
        ----------
        df : pandas.DataFrame or pandas.Series, polars.DataFrame, or pyarrow.Table
            The data to be inserted.
        idx : int or Sequence[int], optional
            The position(s) to insert new column(s), 0-based among data
            columns (the index is not counted). ``0`` inserts as the first
            data column. Only valid when inserting columns. If a sequence is
            provided, it must have one position per new column. Default is to
            add columns to the end.
        warnings : str, optional
            Whether to warn if an unsorted index is about to get sorted. Can be
            ``'warn'`` or ``'ignore'``. Default is ``'warn'``.

        Raises
        ------
        :exc:`TypeError`
            If ``idx`` is passed when inserting rows.
        """
        insert.can_insert(df, self._table_data, idx)
        if insert.cols_matches_table_cols(df, self._table_data):
            self.insert_rows(df, warnings=warnings)
        else:
            self.insert_columns(df, idx=idx, warnings=warnings)

    def insert_rows(self, df, *, warnings="warn"):
        """Inserts one or more rows into the current table.

        Parameters
        ----------
        df : pandas.DataFrame or pandas.Series, polars.DataFrame, or pyarrow.Table
            The data to be inserted. ``df`` must have the same index and column
            types as the stored data.
        warnings : str, optional
            Whether to warn if an unsorted index is about to get sorted. Can be
            ``'warn'`` or ``'ignore'``. Default is ``'warn'``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`~featherstore.exceptions.ColumnDtypeMismatchError`
            If column dtypes are incompatible.
        :exc:`~featherstore.exceptions.ColumnMismatchError`
            If column names do not match the stored table.
        :exc:`~featherstore.exceptions.DuplicateColumnNamesError`
            If column names are not unique.
        :exc:`~featherstore.exceptions.DuplicateIndexValuesError`
            If index values are not unique.
        :exc:`~featherstore.exceptions.IndexNameMismatchError`
            If the index name does not match the stored table.
        :exc:`~featherstore.exceptions.IndexTypeMismatchError`
            If the index type does not match the stored table.
        :exc:`~featherstore.exceptions.RowAlreadyExistsError`
            If any row already exists in the stored table.
        :exc:`TypeError`
            If ``df`` is not a supported table type.
        :exc:`ValueError`
            If ``warnings`` is not ``'warn'`` or ``'ignore'``.
        """
        insert_rows.can_insert_rows(self, df, warnings)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]
        all_partition_names = self._partition_data.keys()

        df = common.format_table(df, index_name=index_name, warnings=warnings)
        has_default_index = insert_rows.has_still_default_index(self, df)

        rows = common.format_rows_arg(df[index_name].to_pylist(), to_dtype=index_type)
        partition_names = read.get_partition_names(self, rows)
        stored_df = read.read_table(self, partition_names)

        df = insert_rows.insert_data(df, to=stored_df)
        partitions = insert_rows.create_partitions(
            df, rows_per_partition, partition_names, all_partition_names
        )

        metadata = common.update_metadata(
            self, partitions, partition_names, has_default_index=has_default_index
        )

        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def insert_columns(self, df, *, idx=-1, warnings="warn"):
        """Inserts one or more columns into the current table.

        Parameters
        ----------
        df : pandas.DataFrame or pandas.Series, polars.DataFrame, or pyarrow.Table
            The data to be inserted. ``df`` must have the same index as the
            stored data.
        idx : int or Sequence[int], optional
            The position(s) to insert the new column(s), 0-based among data
            columns (the index is not counted). ``0`` inserts as the first
            data column. If a sequence is provided, it must have one position
            per new column. Default is to add columns to the end.
        warnings : str, optional
            Whether to warn if an unsorted index is about to get sorted. Can be
            ``'warn'`` or ``'ignore'``. Default is ``'warn'``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`~featherstore.exceptions.ColumnAlreadyExistsError`
            If a new column name already exists.
        :exc:`~featherstore.exceptions.ColumnLengthMismatchError`
            If new column length does not match stored row count.
        :exc:`~featherstore.exceptions.DuplicateColumnNamesError`
            If column names are not unique.
        :exc:`~featherstore.exceptions.IndexMismatchError`
            If indices do not match the stored table.
        :exc:`~featherstore.exceptions.IndexNameInColumnsError`
            If a new column uses the index name.
        :exc:`~featherstore.exceptions.IndexTypeMismatchError`
            If the index type does not match the stored table.
        :exc:`TypeError`
            If ``df`` or ``idx`` has an invalid type.
        :exc:`ValueError`
            If ``idx`` length does not match the number of new columns, or
            ``warnings`` is not ``'warn'`` or ``'ignore'``.
        """
        insert_cols.can_insert_columns(self, df, idx, warnings)

        index_name = self._table_data["index_name"]
        partition_size = self._table_data["partition_size"]

        df = common.format_table(df, index_name=index_name, warnings=warnings)

        partition_names = read.get_partition_names(self, None)
        stored_df = read.read_table(self, partition_names)

        df = insert_cols.insert_columns(stored_df, df, index=idx)

        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        columns = df.column_names
        partitions = insert_cols.create_partitions(
            df, rows_per_partition, partition_names
        )

        metadata = common.update_metadata(
            self,
            partitions,
            partition_names,
            rows_per_partition=rows_per_partition,
            columns=columns,
        )

        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def drop(self, *, cols=None, rows=None):
        """Drops specified labels from rows or columns.

        Both ``rows`` and ``cols`` may be provided in the same call.

        Parameters
        ----------
        cols : Collection, optional
            List of column names or a filter predicate ``{'like': pattern}``.
            ``pattern`` uses SQL wildcards (``?`` matches one character, ``%``
            matches any number of characters) and is case-insensitive.
        rows : Collection, optional
            List of index values or a filter predicate ``{keyword: value}``,
            where keyword can be ``before``, ``after``, or ``between``. Range
            bounds are inclusive. For ``between``, ``value`` is a two-element
            sequence ``[start, end]``.

        Raises
        ------
        :exc:`AttributeError`
            If neither ``rows`` nor ``cols`` is provided.
        :exc:`~featherstore.exceptions.CannotDropAllColumnsError`
            If dropping all columns.
        :exc:`~featherstore.exceptions.CannotDropAllRowsError`
            If dropping all rows.
        :exc:`~featherstore.exceptions.ColumnNotFoundError`
            If any column to drop is not in the table.
        :exc:`~featherstore.exceptions.IndexNameInColumnsError`
            If attempting to drop the index column.
        :exc:`~featherstore.exceptions.IndexTypeMismatchError`
            If row values do not match the table index dtype.
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.RowNotFoundError`
            If any row to drop is not in the table.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`TypeError`
            If ``cols`` or ``rows`` has an invalid type.
        """
        neither_of_rows_and_cols_are_provided = cols is None and rows is None
        if neither_of_rows_and_cols_are_provided:
            raise AttributeError("Neither 'rows' or 'cols' is provided")
        if rows is not None:
            self.drop_rows(rows)
        if cols is not None:
            self.drop_columns(cols)

    def drop_rows(self, rows):
        """Drops specified rows from the table.

        Same as :meth:`drop` with ``rows`` set.

        Parameters
        ----------
        rows : Collection
            See :meth:`drop`.

        See Also
        --------
        drop : Parameters and exceptions.
        """
        drop.can_drop_rows_from_table(self, rows)

        index_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]

        rows = common.format_rows_arg(rows, to_dtype=index_type)

        partition_names = drop.get_partition_names(self, rows)
        stored_df = read.read_table(self, partition_names)

        df = drop.drop_rows_from_data(stored_df, rows, index_name)
        df = common.format_table(df, index_name=index_name, warnings=False)
        partitions = drop.create_partitions(df, rows_per_partition, partition_names)

        has_default_index = drop.has_still_default_index(self, rows)
        metadata = common.update_metadata(
            self, partitions, partition_names, has_default_index=has_default_index
        )

        partitions_to_drop = drop.get_partitions_to_drop(partitions, partition_names)
        drop.drop_partitions(self, partitions_to_drop)
        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def drop_columns(self, cols):
        """Drops specified columns from the table.

        Same as :meth:`drop` with ``cols`` set.

        Parameters
        ----------
        cols : Collection
            See :meth:`drop`.

        See Also
        --------
        drop : Parameters and exceptions.
        """
        drop.can_drop_cols_from_table(self, cols)

        index_name = self._table_data["index_name"]
        partition_size = self._table_data["partition_size"]
        stored_cols = self._table_data["columns"]
        old_rows_per_partition = self._table_data["rows_per_partition"]

        cols = common.format_cols_arg(cols, like=stored_cols)

        partition_names = drop.get_partition_names(self, None)
        stored_df = read.read_table(self, partition_names)

        df = drop.drop_cols_from_data(stored_df, cols)
        df = common.format_table(df, index_name=index_name, warnings=False)

        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        rows_per_partition = max(rows_per_partition, old_rows_per_partition)
        partitions = drop.create_partitions(df, rows_per_partition, partition_names)

        columns = df.column_names
        metadata = common.update_metadata(
            self,
            partitions,
            partition_names,
            rows_per_partition=rows_per_partition,
            columns=columns,
        )

        partitions_to_drop = drop.get_partitions_to_drop(partitions, partition_names)
        drop.drop_partitions(self, partitions_to_drop)
        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def rename_columns(self, cols, *, to=None):
        """Renames one or more columns.

        ``rename_columns`` supports two different call syntaxes:

        - ``rename_columns({'c1': 'new_c1', 'c2': 'new_c2'})``
        - ``rename_columns(['c1', 'c2'], to=['new_c1', 'new_c2'])``

        Parameters
        ----------
        cols : Collection
            Either a list of columns to be renamed, or a dict mapping columns
            to be renamed to new column names.
        to : Collection[str], optional
            New column names. Default is ``None``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`AttributeError`
            If ``to`` is provided twice or not provided when required.
        :exc:`~featherstore.exceptions.ColumnNotFoundError`
            If any column to rename is not in the table.
        :exc:`~featherstore.exceptions.DuplicateColumnNamesError`
            If renamed columns would not be unique.
        :exc:`~featherstore.exceptions.IndexNameInColumnsError`
            If a column is renamed to the index name.
        :exc:`TypeError`
            If ``cols`` or ``to`` has an invalid type.
        :exc:`ValueError`
            If the number of columns and new names do not match.
        """
        rename_cols.can_rename_columns(self, cols, to)

        index_name = self._table_data["index_name"]
        rows_per_partition = self._table_data["rows_per_partition"]

        cols_mapping = common.format_cols_and_to_args(cols, to)

        partition_names = read.get_partition_names(self, None)
        df = read.read_table(self, partition_names)

        df = rename_cols.rename_columns(df, cols_mapping)
        df = common.format_table(df, index_name=index_name, warnings=False)
        partitions = write.create_partitions(df, rows_per_partition, partition_names)

        rename_cols.write_metadata(self, partitions)
        write.write_partitions(partitions, self._table_path)

    @property
    def columns(self):
        """Names of the table columns.

        The table must exist.

        Returns
        -------
        list
            Column names, including the index column name.
        """
        return self._table_data["columns"]

    @columns.setter
    def columns(self, cols):
        """Same as :meth:`reorder_columns`.

        *Note*: You cannot use this method to rename columns; use
        :meth:`rename_columns` instead.

        Parameters
        ----------
        cols : Sequence[str]
            See :meth:`reorder_columns`.

        See Also
        --------
        reorder_columns : Parameters and exceptions.
        """
        misc.can_reorder_columns(self, cols)
        index_name = self._table_data["index_name"]
        self._table_data["columns"] = [index_name, *cols]

    def reorder_columns(self, cols):
        """Reorders the table columns.

        Parameters
        ----------
        cols : Sequence[str]
            The new column ordering. Must be the table's data column names
            in the desired order, without the index name.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`~featherstore.exceptions.ColumnMismatchError`
            If column names do not match the stored table.
        :exc:`~featherstore.exceptions.DuplicateColumnNamesError`
            If column names are not unique.
        :exc:`~featherstore.exceptions.IndexNameInColumnsError`
            If the index name is included in ``cols``.
        :exc:`TypeError`
            If ``cols`` has an invalid type.
        """
        self.columns = cols

    @property
    def index(self):
        """The table index.

        Returns
        -------
        pandas.Index

        See Also
        --------
        read_arrow : Exceptions.
        """
        index = self.read_arrow(cols=[])
        index = index.to_pandas().index
        return index

    def astype(self, cols, *, to=None):
        """Changes the data type of one or more columns.

        ``astype`` supports two different call syntaxes:

        - ``astype({'c1': pa.int64(), 'c2': pa.int16()})``
        - ``astype(['c1', 'c2'], to=[pa.int64(), pa.int16()])``

        The index can be cast by using the index name as a column.

        Parameters
        ----------
        cols : Sequence[str] or dict
            Either a sequence of columns to have their data types changed, or a
            dict mapping columns to new column data types.
        to : Sequence[pyarrow.DataType or numpy.dtype], optional
            New column data types. Default is ``None``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.TableNotFoundError`
            If the table does not exist.
        :exc:`AttributeError`
            If ``to`` is provided twice or not provided when required.
        :exc:`~featherstore.exceptions.ColumnNotFoundError`
            If any column is not in the table.
        :exc:`~featherstore.exceptions.DuplicateColumnNamesError`
            If column names are not unique.
        :exc:`~featherstore.exceptions.UnsupportedIndexTypeError`
            If a forbidden index dtype is requested.
        :exc:`TypeError`
            If ``cols`` or ``to`` has an invalid type.
        :exc:`ValueError`
            If the number of columns and dtypes do not match.
        """
        astype.can_change_type(self, cols, to)
        index_name = self._table_data["index_name"]
        partition_size = self._table_data["partition_size"]

        astype_mapping = common.format_cols_and_to_args(cols, to)

        partition_names = read.get_partition_names(self, None)
        df = read.read_table(self, partition_names)

        df = astype.change_type(df, astype_mapping)
        df = common.format_table(df, index_name=index_name, warnings=False)
        has_default_index = astype.has_still_default_index(self, df)

        rows_per_partition = common.compute_rows_per_partition(df, partition_size)
        partitions = astype.create_partitions(df, rows_per_partition, partition_names)

        metadata = common.update_metadata(
            self,
            partitions,
            partition_names,
            rows_per_partition=rows_per_partition,
            has_default_index=has_default_index,
        )

        partitions_to_drop = astype.get_partitions_to_drop(partitions, partition_names)
        drop.drop_partitions(self, partitions_to_drop)

        write.write_metadata(self, metadata)
        write.write_partitions(partitions, self._table_path)

    def rename_table(self, *, to):
        """Renames the current table.

        Parameters
        ----------
        to : str
            The new name of the table.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.ForbiddenTableNameError`
            If ``to`` is reserved or not a valid path name.
        :exc:`~featherstore.exceptions.TableAlreadyExistsError`
            If a table with the new name already exists.
        :exc:`TypeError`
            If ``to`` is not a str.
        """
        new_table_name = to
        store_path = os.path.split(self._table_path)[0]
        new_path = os.path.join(store_path, new_table_name)
        misc.can_rename_table(new_table_name, new_path)

        os.rename(self._table_path, new_path)
        self._table_path = new_path
        self._table_data = Metadata(self._table_path, "table")
        self._partition_data = Metadata(self._table_path, "partition")

    def drop_table(self, *, warnings="warn"):
        """Deletes the current table.

        Parameters
        ----------
        warnings : str, optional
            Whether to warn if the table does not exist. Can be ``'warn'`` or
            ``'ignore'``. Default is ``'warn'``.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`ValueError`
            If ``warnings`` is not ``'warn'`` or ``'ignore'``.
        """
        misc.can_drop_table(self, warnings)
        if self.exists():
            _utils.delete_folder_tree(self._table_path, current_db())
            # Reset the metadata indices:
            self._table_data = Metadata(self._table_path, "table")
            self._partition_data = Metadata(self._table_path, "partition")

    def create_snapshot(self, path):
        """Creates a compressed backup of the table.

        The table can later be restored with
        :func:`~featherstore.snapshot.restore_table`.

        Parameters
        ----------
        path : str
            Path to the snapshot archive. ``.tar.xz`` is appended unless
            ``path`` already ends with that suffix. An existing file at the
            resulting path is overwritten.

        Raises
        ------
        :exc:`~featherstore.exceptions.NotConnectedError`
            If FeatherStore is not connected to a database.
        :exc:`~featherstore.exceptions.SnapshotTargetNotFoundError`
            If the table path does not exist.
        :exc:`TypeError`
            If ``path`` is not a str.
        """
        _create_snapshot(path, self._table_path, "table")

    def repartition(self, new_partition_size):
        """Repartitions the table so that each partition is ``new_partition_size`` bytes.

        This reads the full table into memory and rewrites it.

        Parameters
        ----------
        new_partition_size : int
            The size of each partition in bytes. ``-1`` disables partitioning.

        See Also
        --------
        read_arrow : Exceptions when reading.
        write : Exceptions when rewriting.
        """
        df = self.read_arrow()
        has_default_index = self._table_data["has_default_index"]
        if has_default_index:
            index_name = None
        else:
            index_name = self._table_data["index_name"]
        self.write(
            df, index=index_name, partition_size=new_partition_size, errors="ignore"
        )

    @property
    def shape(self):
        """Shape of the stored table as ``(rows, columns)``.

        The table must exist.

        Returns
        -------
        tuple of int
            The shape of the table. The column count includes the index column.
        """
        rows = self._table_data["num_rows"]
        cols = self._table_data["num_columns"]
        return (rows, cols)

    @property
    def partition_size(self):
        """Partition size in bytes.

        The table must exist.

        Returns
        -------
        int
            The partition size in bytes.
        """
        return self._table_data["partition_size"]

    def exists(self):
        """Returns whether the table exists on disk.

        Returns
        -------
        bool
        """
        return os.path.exists(self._table_path)

    @property
    def name(self):
        """The table name.

        Returns
        -------
        str
        """
        table_name = os.path.split(self._table_path)[-1]
        return table_name

    def _create_table(self):
        os.makedirs(self._table_path)
        self._table_data.create()
        self._partition_data.create()
