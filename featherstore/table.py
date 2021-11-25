import os

from featherstore.connection import current_db
from featherstore._metadata import Metadata
from featherstore import _utils

from featherstore._table.read import (
    can_read_table,
    get_partition_names,
    read_partitions,
    filter_table_rows,
    drop_default_index,
    can_be_converted_to_series,
    can_be_converted_to_rangeindex,
    convert_to_rangeindex,
    convert_table_to_polars,
)
from featherstore._table.write import (
    can_write_table,
    calculate_rows_per_partition,
    make_partitions,
    make_partition_ids,
    make_table_metadata,
    write_partitions,
)
from featherstore._table.append import (
    can_append_table,
    format_default_index,
    sort_columns,
    append_new_partition_ids,
)
from featherstore._table.update import (
    can_update_table,
    update_data,
)
from featherstore._table.insert import (
    can_insert_table,
    insert_data,
    insert_new_partition_ids,
)
from featherstore._table.drop import (
    can_drop_rows_from_table,
    get_adjacent_partition_name,
    drop_rows_from_data,
)
from featherstore._table.common import (
    can_init_table,
    can_rename_table,
    combine_partitions,
    format_table,
    format_cols,
    format_rows,
    make_partition_metadata,
    update_table_metadata,
    assign_ids_to_partitions,
    delete_partition,
    delete_partition_metadata,
)

DEFAULT_PARTITION_SIZE = 128 * 1024**2


class Table:
    def __init__(self, table_name, store_name):
        """A class for saving and loading DataFrames as partitioned Feather files.

        Tables supports several operations that can be done without loading in the
        full data:

        - Predicate Filtering when reading
        - Appends
        - Fetching Columns
        - Fetching Index
        - Updates
        - Inserts

        It will also support the following operations down the line:

        - Drop columns/Drop rows
        - Changing types
        - Changing index

        Parameters
        ----------
        table_name : str
            The name of the table
        store_name : str
            The name of the store
        """
        can_init_table(table_name, store_name)

        self.table_name = table_name
        self.store = store_name
        self._table_path = os.path.join(current_db(), store_name, table_name)
        self.exists = os.path.exists(self._table_path)

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
        can_read_table(cols, rows, self.exists, self._table_data)

        index_col_name = self._table_data["index_name"]
        has_default_index = self._table_data["has_default_index"]
        index_type = self._table_data["index_dtype"]

        rows = format_rows(rows, index_type)
        cols = format_cols(cols, self._table_data)

        partition_names = get_partition_names(rows, self._table_path)
        partitions = read_partitions(partition_names, self._table_path, cols)
        df = combine_partitions(partitions)
        df = filter_table_rows(df, rows, index_col_name)
        if has_default_index and rows is None:
            df = drop_default_index(df, index_col_name)

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

        if can_be_converted_to_series(df):
            df = df.squeeze()
        if can_be_converted_to_rangeindex(df):
            df = convert_to_rangeindex(df)

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
        can_read_table(cols, rows, self.exists, self._table_data)

        index_col_name = self._table_data["index_name"]
        has_default_index = self._table_data["has_default_index"]
        index_type = self._table_data["index_dtype"]

        rows = format_rows(rows, index_type)
        cols = format_cols(cols, self._table_data)

        partition_names = get_partition_names(rows, self._table_path)
        partitions = read_partitions(partition_names, self._table_path, cols)
        df = combine_partitions(partitions)
        df = filter_table_rows(df, rows, index_col_name)
        if has_default_index and rows is None:
            df = drop_default_index(df, index_col_name)
        df = convert_table_to_polars(df)

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
        can_write_table(
            df,
            index,
            errors,
            warnings,
            partition_size,
            self.exists,
            self.table_name,
        )

        self.drop_table()
        self.exists = False

        formatted_df = format_table(df, index, warnings)
        rows_per_partition = calculate_rows_per_partition(
            formatted_df, partition_size)
        partitioned_df = make_partitions(formatted_df, rows_per_partition)
        partition_names = make_partition_ids(partitioned_df)
        partitioned_df = assign_ids_to_partitions(partitioned_df,
                                                  partition_names)

        collected_metadata = (partition_size, rows_per_partition)
        table_metadata = make_table_metadata(partitioned_df,
                                             collected_metadata)
        partition_metadata = make_partition_metadata(partitioned_df)

        self._create_table()
        self._table_data.write(table_metadata)

        self._partition_data.write(partition_metadata)
        write_partitions(partitioned_df, self._table_path)
        self.exists = True

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
        can_append_table(
            df,
            warnings,
            self._table_path,
            self.exists,
        )

        partition_names = self._partition_data.keys()
        last_partition_name = partition_names[-1]

        last_partition, = read_partitions([last_partition_name],
                                          self._table_path, None)

        index = self._table_data["index_name"]
        df = format_table(df, index, warnings)
        has_default_index = self._table_data["has_default_index"]
        if has_default_index:
            df = format_default_index(df, self._table_path)
        df = sort_columns(df, last_partition.column_names)

        df = combine_partitions([last_partition, df])
        rows_per_partition = self._table_data["rows_per_partition"]
        partitioned_df = make_partitions(df, rows_per_partition)
        del last_partition, df  # Closes memory-map

        new_partition_names = append_new_partition_ids(partitioned_df,
                                                       last_partition_name)
        partitioned_df = assign_ids_to_partitions(partitioned_df,
                                                  new_partition_names)

        old_table_metadata = {'num_rows': None, 'num_partitions': None}
        for key in old_table_metadata.keys():
            old_table_metadata[key] = self._table_data[key]
        old_partition_metadata = {
            last_partition_name: self._partition_data[last_partition_name]
        }
        new_partition_metadata = make_partition_metadata(partitioned_df)

        table_metadata = update_table_metadata(old_table_metadata,
                                               new_partition_metadata,
                                               old_partition_metadata)

        self._table_data.write(table_metadata)
        self._partition_data.write(new_partition_metadata)
        write_partitions(partitioned_df, self._table_path)

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
        can_update_table(df, self._table_path, self.exists, self)

        index_type = self._table_data["index_dtype"]
        rows = format_rows(df.index, index_type)

        partition_names = get_partition_names(rows, self._table_path)
        stored_df = read_partitions(partition_names,
                                    self._table_path,
                                    columns=None)

        stored_df = combine_partitions(stored_df)
        try:
            df = update_data(stored_df, to=df)
        except Exception:
            raise
        finally:
            del stored_df
        df = format_table(df, index=None, warnings=False)

        rows_per_partition = self._table_data["rows_per_partition"]
        partitioned_df = make_partitions(df, rows_per_partition)
        partitioned_df = assign_ids_to_partitions(partitioned_df,
                                                  partition_names)

        write_partitions(partitioned_df, self._table_path)

    def insert(self, df):
        """Insert one or more rows into the current table.

        Parameters
        ----------
        df : Pandas DataFrame or Pandas Series
            The data to be inserted. `df` must have the same index and column
            types as the stored data.
        """
        can_insert_table(df, self._table_path, self.exists)

        index_type = self._table_data["index_dtype"]
        rows = format_rows(df.index, index_type)

        partition_names = get_partition_names(rows, self._table_path)
        stored_df = read_partitions(partition_names,
                                    self._table_path,
                                    columns=None)
        stored_df = combine_partitions(stored_df)

        try:
            df = insert_data(stored_df, to=df)
        except Exception:
            raise
        finally:
            del stored_df
        df = format_table(df, index=None, warnings='ignore')

        rows_per_partition = self._table_data["rows_per_partition"]
        partitioned_df = make_partitions(df, rows_per_partition)

        new_partition_names = insert_new_partition_ids(partitioned_df,
                                                       partition_names)
        partitioned_df = assign_ids_to_partitions(partitioned_df,
                                                  new_partition_names)

        old_table_metadata = {
            'num_rows': self._table_data['num_rows'],
            'num_partitions': self._table_data['num_partitions']
        }
        old_partition_metadata = {
            name: self._partition_data[name]
            for name in partition_names
        }
        new_partition_metadata = make_partition_metadata(partitioned_df)

        table_metadata = update_table_metadata(old_table_metadata,
                                               new_partition_metadata,
                                               old_partition_metadata)

        self._table_data.write(table_metadata)
        self._partition_data.write(new_partition_metadata)
        write_partitions(partitioned_df, self._table_path)

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
        can_drop_rows_from_table(rows, self._table_path, self.exists)

        index_col_name = self._table_data["index_name"]
        index_type = self._table_data["index_dtype"]
        rows_per_partition = self._table_data["rows_per_partition"]

        rows = format_rows(rows, index_type)
        partition_names = get_partition_names(rows, self._table_path)
        partition_names = get_adjacent_partition_name(partition_names, self._table_path)

        stored_df = read_partitions(partition_names,
                                    self._table_path,
                                    columns=None)
        stored_df = combine_partitions(stored_df)

        try:
            df = drop_rows_from_data(stored_df, rows, index_col_name)
        except Exception:
            raise
        finally:
            del stored_df
        df = format_table(df, index=None, warnings=False)
        if not df:
            raise IndexError("Can't drop all rows from stored table")

        partitioned_df = make_partitions(df, rows_per_partition)
        kept_partition_names = partition_names[:len(partitioned_df)]
        dropped_partition_names = partition_names[len(partitioned_df):]
        partitioned_df = assign_ids_to_partitions(partitioned_df,
                                                  kept_partition_names)

        old_table_metadata = {
            'num_rows': self._table_data['num_rows'],
            'num_partitions': self._table_data['num_partitions']
        }
        old_partition_metadata = {
            name: self._partition_data[name]
            for name in partition_names
        }
        new_partition_metadata = make_partition_metadata(partitioned_df)
        table_metadata = update_table_metadata(old_table_metadata,
                                               new_partition_metadata,
                                               old_partition_metadata)
        table_metadata['has_default_index'] = False

        for name in dropped_partition_names:
            delete_partition(self._table_path, name)
            delete_partition_metadata(self._table_path, name)

        self._table_data.write(table_metadata)
        self._partition_data.write(new_partition_metadata)
        write_partitions(partitioned_df, self._table_path)

    def drop_columns(self, cols):
        """Drops specified rows from table

        Same as `Table.drop(cols=val)`
        """
        raise NotImplementedError

    def _add_columns(self, cols):
        raise NotImplementedError

    @property
    def columns(self):
        """Fetches the table columns

        Returns
        -------
        columns : list
        """
        return self._table_data["columns"]

    @columns.setter
    def columns(self, new_cols):
        raise NotImplementedError

    def _rename_columns(self, cols, *, to=None):
        raise NotImplementedError

    def _astype(self, cols, dtypes):
        raise NotImplementedError

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

    def rename_table(self, *, to):
        """Renames the current table

        Parameters
        ----------
        to : str
            The new name of the table.
        """
        new_table_name = to
        new_path = os.path.join(current_db(), self.store, new_table_name)
        can_rename_table(new_table_name, self._table_path, new_path)

        os.rename(self._table_path, new_path)
        self.table_name = new_table_name
        self._table_path = new_path

    def drop_table(self, skip_bin=False):
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

    def _create_table(self):
        os.makedirs(self._table_path)
        self._table_data.create()
        self._partition_data.create()
