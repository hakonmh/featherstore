=======================
Intro to the data model
=======================

FeatherStore organizes your data in three levels: a **database** holds
**stores**, and each store holds **tables**. A table is not a single file.
Instead, it is a folder of Feather files, called partitions, together with a
small amount of metadata. Every row in a table is identified by a sorted,
unique index.

This page walks through that layout from the top down and explains why it makes
some operations fast. If you would rather learn by doing, start with
:doc:`10min`. If you are deciding between FeatherStore and a plain Feather or
Parquet file, see :doc:`/Overview`.

Database, store, table
----------------------

A database is simply a directory on disk. When you create one, FeatherStore
writes a ``.featherstore`` marker file into it so the folder can be recognized
as a database later. Each store is a subdirectory of the database, and each
table is a subdirectory of a store:

.. code-block:: text

    path/to/db/                         # database
        .featherstore                   # marker file
        weather/                        # store
            bergen/                     # table
                .metadata/
                    table.db            # columns, dtypes, row count, partition size
                    partition.db        # min / max index and row count per partition
                00000001000000.feather  # partitions, in index order
                00000002000000.feather
                ...

There is no server or background process to start. You point FeatherStore at a
folder with :func:`~featherstore.connection.connect` or
:func:`~featherstore.connection.create_database`, and then read and write
directly.

The APIs for each level are covered in :doc:`database`, :doc:`stores`, and
:doc:`tables`.

Partitions and the index
------------------------

Compared with a plain Feather file, a FeatherStore table adds two things.

The first is **partitions**. Rather than keeping everything in one large file,
the table is split into several Feather files, each up to a target size (128 MB by
default). The partition size is a trade-off. Smaller partitions let a query
that only needs a few rows skip more files, but they add overhead when you read
or write the whole table. Larger partitions mean fewer files and faster
full-table reads and writes, but queries can skip less. See
:doc:`partitioning` for how to choose.

The second is **a sorted, unique index**. Rows are stored in index order, so
each partition holds one continuous range of index values. The index itself is
saved as a column in every partition.

For the index to work as a fast lookup, it has to follow three rules.

**It is sorted.** Because rows are kept in order, the first and last index
value of a partition tell you exactly which rows it can contain. FeatherStore
sorts your data by the index when writing, and warns you if the input was not
already sorted. This lets FeatherStore use ``partition.db`` to find which files
a query or edit needs, without opening any of them.

**It is unique.** Each index value belongs to exactly one row, so an update,
drop, or insert always knows exactly where to go. Duplicate index values are
rejected.

**Its values can be ordered.** Range filters and partition lookups work by
comparing index values, so every value must be comparable with every other.
FeatherStore accepts integers (signed and unsigned), floats, decimals, strings,
binary, durations, and temporal values (dates, times, and timestamps). See
:doc:`table_index` for details.

The ``.metadata`` folder keeps track of the first and last index value in each
partition, as shown below:

.. image:: /images/featherstore_table_layout.svg
   :alt: FeatherStore table layout: a .metadata folder and four index-sorted
         partitions; a range read on one column opens two partitions and only
         the index and temp columns
   :width: 920
   :align: center

When you read a range of rows, FeatherStore first checks ``partition.db`` to
find the partitions that overlap your range. It then opens only those files and
loads only the columns you asked for, plus the index. This works because each
column is stored as its own block of data inside the file (the last section of
this page shows this in more detail). Any rows that fall outside your range are
filtered out after reading.

How many partitions get opened depends on how you select rows. A ``before``,
``after``, or ``between`` range opens only the partitions that overlap it. A
list of labels, on the other hand, opens every partition from the smallest
label to the largest, so labels that are spread far apart can pull in many
partitions. See :doc:`indexing` for all the ways to select rows.

What stays cheap
----------------

The table at the bottom of the figure above summarizes which partitions each
kind of operation touches.

The following operations skip partitions they do not need:

* **Partial reads**, such as selecting rows by label or range, or columns by
  name or pattern. Only the overlapping partitions are opened, and within each
  file only the requested columns are read. See :doc:`indexing`.
* **Appends** where the new index values are all larger than the current
  largest one. Only the last partition is rewritten. See :doc:`append`.
* **Row inserts, updates, and drops.** Only the partitions that overlap the
  affected index range are rewritten. See :doc:`insert`, :doc:`update`, and
  :doc:`drop`.
* **Metadata reads**, such as column names, index length, or table dimensions.
  These never open a data file at all. See :doc:`metadata`.

Reading or writing the full table always opens every partition. In that case,
smaller partitions mean more files and therefore more overhead. See
:doc:`/Benchmarks` for measurements.

Some operations have to rewrite every partition, either because every
partition contains every column, or because the whole table is rebuilt:

* Inserting or dropping columns
* Renaming or reordering columns
* Changing column types (:meth:`~featherstore.table.Table.astype`)
* Changing the partition size (:meth:`~featherstore.table.Table.repartition`)

Inside a partition: the Feather file
------------------------------------

Each partition is a **Feather V2** file, which is another name for the
`Arrow IPC file format <https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format>`_.
This is the same format that Arrow, Polars, and pandas use for ``.feather`` and
``.arrow`` files, so any of these tools can open a partition directly.
FeatherStore writes each partition uncompressed, as a single record batch. The
figure below zooms in on partition P2 from the table figure above.

A Feather file describes its own contents and is designed so a reader can jump
straight to the part it needs. It is made up of:

* The ``ARROW1`` magic string at both the start and the end, which marks the
  file as Arrow.
* A **schema** that records the column names and types.
* Optional **dictionary** batches, which hold lookup tables for categorical
  columns.
* One or more **record batches** that hold the actual data. Each batch has a
  small header followed by the column data. Every column takes up one or two
  buffers: the values, and optionally a bitmap marking which values are null.
* A **footer** that records where each batch starts and how large it is, so a
  reader can jump to it without scanning the whole file.

.. image:: /images/feather_file_layout.svg
   :alt: Feather V2 / Arrow IPC file layout of partition P2: magic, schema, no
         dictionaries, one record batch with a contiguous buffer per column, footer
   :width: 920
   :align: center

Because the data is stored **by column**, all the bytes for one column sit next
to each other in the file. With memory mapping, a reader can build Arrow arrays
that point straight at those bytes instead of copying them (as long as you stay
in Arrow or Polars), and the columns you did not ask for are never read from
disk. Memory mapping is turned on by default on every platform except Windows;
see :doc:`gotchas` for how to override it.

Not a query engine
------------------

FeatherStore lets you keep working with the DataFrames you already use. It is a
place to store them, not a SQL engine or a database server. If you need to run
ad-hoc queries across many files, a tool like DuckDB may be a better fit; see
:doc:`/Overview`. For full-table read and write timings, see
:doc:`/Benchmarks`.
