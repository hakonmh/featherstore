============
Partitioning
============

A FeatherStore table is split into Feather files of a target size. Partitioning
is what lets range queries and row edits skip most of the data.

Choosing a size
---------------

``partition_size`` is measured in **bytes**:

* default is 128 MB (``128 * 1024**2``), suitable for large tables
* pass ``-1`` to disable partitioning (single partition)
* examples in :doc:`10min` use a tiny value so a few rows still split across
  files

FeatherStore converts the byte target into an approximate rows-per-partition
count from the data being written, then splits accordingly.

Tradeoffs
---------

* **Smaller partitions** — more files can be skipped on selective row filters;
  more overhead on full-table reads and writes.
* **Larger partitions** — fewer files and faster full-table IO; less precise
  skipping on row filters.

The default 128 MB is a middle ground. See the predicate-filtering section of
:doc:`/Benchmarks`.

Which operations rewrite which partitions
-----------------------------------------

+----------------------------------+----------------------------------+
| Operation                        | Partitions rewritten             |
+==================================+==================================+
| Range / column read              | None (read only overlapping)     |
+----------------------------------+----------------------------------+
| Append                           | Last partition                   |
+----------------------------------+----------------------------------+
| Row insert / update / drop       | Overlapping index range          |
+----------------------------------+----------------------------------+
| Column insert / drop             | All partitions                   |
+----------------------------------+----------------------------------+
| Rename / reorder / astype        | All partitions                   |
+----------------------------------+----------------------------------+
| Repartition                      | All (full rewrite)               |
+----------------------------------+----------------------------------+

Repartitioning
--------------

:meth:`~featherstore.table.Table.repartition` reads the full table and rewrites
it with a new ``partition_size`` (again in bytes; ``-1`` disables partitioning).

.. code-block:: python

    table.repartition(64 * 1024**2)

Safe overwrites
---------------

Partition writes use Arrow IPC with an atomic replace so an in-progress overwrite
does not corrupt a memory-mapped reader of the previous file. You usually do not
need to think about this; it matters when ``mmap=True`` readers and writers share
a table.

Inspecting the current size
---------------------------

:attr:`~featherstore.table.Table.partition_size` returns the configured size in
bytes from metadata. See :doc:`metadata`.

See also
--------

* :doc:`data_model` — why partitions exist
* :doc:`indexing` — row filters that benefit from pruning
* :doc:`io` — ``partition_size`` on write
