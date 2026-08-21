Getting started
===============

This page walks through FeatherStore from installation to the operations you will
use most often: writing and reading tables, querying a subset of rows and columns,
appending data, and editing stored tables. The examples share one small dataset so
you can follow them in order. For the full API, see the
:doc:`API Reference`.

Installing FeatherStore
-----------------------

Install FeatherStore from PyPI with pip or uv:

.. code-block:: bash

    pip install featherstore

.. code-block:: bash

    uv add featherstore

Or install the latest version from GitHub:

.. code-block:: bash

    pip install git+https://github.com/hakonmh/featherstore.git

.. code-block:: bash

    uv add git+https://github.com/hakonmh/featherstore.git

FeatherStore requires Python 3.11 or newer. pandas, Polars, and PyArrow are
installed automatically.

Connecting to a database
------------------------

A FeatherStore database is a directory on disk. ``create_database`` creates that
directory and connects to it:

.. code-block:: python

    import featherstore as fs

    fs.create_database("path/to/db")

Use ``connect`` when the database already exists:

.. code-block:: python

    fs.connect("path/to/db")

You can check a path with ``fs.database_exists()``, see the active directory with
``fs.current_db()``, and leave the database with ``fs.disconnect()``.

Stores
------

A database contains one or more stores. A store is a named folder that groups
related tables.

.. code-block:: python

    store = fs.create_store("weather")
    print(fs.list_stores())

.. code-block:: text

    ['weather']

``create_store`` returns a :class:`~featherstore.store.Store`. If the store already
exists, FeatherStore warns (unless ``warnings="ignore"``) and returns it. To open an
existing store later, use ``fs.Store("weather")``.

You can rename a store with ``fs.rename_store("weather", to="obs")`` and delete an
empty store with ``fs.drop_store("obs")``. A store that still contains tables cannot
be dropped.

Reading and writing
-------------------

FeatherStore stores Pandas DataFrames and Series, Polars DataFrames and Series, and
PyArrow Tables as partitioned Feather files.

The examples below use a few days of weather observations. January 3 is missing on
purpose; we will insert it later.

.. code-block:: python

    import pandas as pd

    df = pd.DataFrame(
        {
            "temperature": [2.1, 1.4, 0.8, 3.2],
            "humidity": [88, 91, 79, 74],
            "rainfall": [4.2, 0.0, 0.0, 0.3],
        },
        index=pd.DatetimeIndex(
            ["2024-01-01", "2024-01-02", "2024-01-04", "2024-01-05"],
            name="date",
        ),
    )
    print(df)

.. code-block:: text

                temperature  humidity  rainfall
    date
    2024-01-01          2.1        88       4.2
    2024-01-02          1.4        91       0.0
    2024-01-04          0.8        79       0.0
    2024-01-05          3.2        74       0.3

If the DataFrame has an index, FeatherStore uses it. The index must be unique and
of a supported type (integer, unsigned integer, float, decimal, string, binary,
duration, or temporal). FeatherStore sorts rows by the index before writing.

``partition_size`` is the size of each partition in bytes. The default is 128 MB,
which suits large tables. The examples use a tiny value so a few rows still split
across partitions. Pass ``-1`` to disable partitioning.

.. code-block:: python

    store.write_table("oslo", df, partition_size=128)
    print(store.list_tables())

.. code-block:: text

    ['oslo']

Read the table back as a Pandas DataFrame, a Polars DataFrame, or a PyArrow Table:

.. code-block:: python

    print(store.read_pandas("oslo"))

.. code-block:: text

                temperature  humidity  rainfall
    date
    2024-01-01          2.1        88       4.2
    2024-01-02          1.4        91       0.0
    2024-01-04          0.8        79       0.0
    2024-01-05          3.2        74       0.3

.. code-block:: python

    print(store.read_polars("oslo"))

.. code-block:: text

    shape: (4, 4)
    ┌─────────────────────┬─────────────┬──────────┬──────────┐
    │ date                ┆ temperature ┆ humidity ┆ rainfall │
    │ ---                 ┆ ---         ┆ ---      ┆ ---      │
    │ datetime[μs]        ┆ f64         ┆ i64      ┆ f64      │
    ╞═════════════════════╪═════════════╪══════════╪══════════╡
    │ 2024-01-01 00:00:00 ┆ 2.1         ┆ 88       ┆ 4.2      │
    │ 2024-01-02 00:00:00 ┆ 1.4         ┆ 91       ┆ 0.0      │
    │ 2024-01-04 00:00:00 ┆ 0.8         ┆ 79       ┆ 0.0      │
    │ 2024-01-05 00:00:00 ┆ 3.2         ┆ 74       ┆ 0.3      │
    └─────────────────────┴─────────────┴──────────┴──────────┘

``store.read_arrow("oslo")`` returns the same data as a PyArrow Table. Pandas keeps a
named index as the DataFrame index; Polars and PyArrow return it as a column. A
default integer index is omitted from Arrow and Polars results.

Polars and PyArrow have no index, so pass the column that should become the stored
index:

.. code-block:: python

    import datetime as dt
    import polars as pl

    bergen = pl.DataFrame(
        {
            "date": [dt.datetime(2024, 1, 1), dt.datetime(2024, 1, 2)],
            "temperature": [1.8, 0.6],
            "humidity": [92, 87],
            "rainfall": [6.4, 0.2],
        }
    )
    store.write_table("bergen", bergen, index="date")

Querying rows and columns
-------------------------

Partitioned Feather files let FeatherStore load only the partitions and columns a
query needs. Range filters on the sorted index are
``{"before": end}``, ``{"after": start}``, and ``{"between": [start, end]}``.
All three are inclusive.

.. code-block:: python

    print(store.read_pandas("oslo", rows={"after": "2024-01-02"}, cols=["rainfall", "temperature"]))

.. code-block:: text

                rainfall  temperature
    date
    2024-01-02       0.0          1.4
    2024-01-04       0.0          0.8
    2024-01-05       0.3          3.2

You can also pass explicit row labels, or filter column names with SQL-style
wildcards (``%`` for any number of characters, ``?`` for a single character):

.. code-block:: python

    store.read_pandas("oslo", rows={"between": ["2024-01-02", "2024-01-04"]})
    store.read_pandas("oslo", cols={"like": "temp%"})

Appending data
--------------

``append_table`` adds rows whose index values fall after the stored data. Only the
last partition is loaded.

.. code-block:: python

    new_day = pd.DataFrame(
        {"temperature": [1.9], "humidity": [83], "rainfall": [2.6]},
        index=pd.DatetimeIndex(["2024-01-06"], name="date"),
    )
    store.append_table("oslo", new_day)
    print(store.read_pandas("oslo"))

.. code-block:: text

                temperature  humidity  rainfall
    date
    2024-01-01          2.1        88       4.2
    2024-01-02          1.4        91       0.0
    2024-01-04          0.8        79       0.0
    2024-01-05          3.2        74       0.3
    2024-01-06          1.9        83       2.6

Editing tables
--------------

:class:`~featherstore.store.Store` covers write, read, and append.
:meth:`~featherstore.store.Store.select_table` returns a
:class:`~featherstore.table.Table` with additional methods for inserting, updating,
and dropping data.

.. code-block:: python

    table = store.select_table("oslo")
    print(table.exists())

.. code-block:: text

    True

``Table.update()``, ``Table.insert()``, ``Table.insert_rows()``, and
``Table.insert_columns()`` accept Pandas DataFrames and Series, Polars DataFrames,
and PyArrow Tables. Polars Series is not supported for these edit methods.

Inserting rows
^^^^^^^^^^^^^^

``Table.insert()`` looks at column names. If they match the stored table, rows are
inserted at their sorted index positions; otherwise columns are inserted.

.. code-block:: python

    delayed = pd.DataFrame(
        {"temperature": [-0.3], "humidity": [85], "rainfall": [1.1]},
        index=pd.DatetimeIndex(["2024-01-03"], name="date"),
    )
    table.insert(delayed)  # matching column names -> inserts rows
    print(table.read_pandas())

.. code-block:: text

                temperature  humidity  rainfall
    date
    2024-01-01          2.1        88       4.2
    2024-01-02          1.4        91       0.0
    2024-01-03         -0.3        85       1.1
    2024-01-04          0.8        79       0.0
    2024-01-05          3.2        74       0.3
    2024-01-06          1.9        83       2.6

Call ``Table.insert_rows()`` or ``Table.insert_columns()`` when you want the
operation to be explicit. Pass ``warnings="ignore"`` to suppress sorting warnings
when the input index is unsorted (the default is ``warnings="warn"``).

Inserting columns
^^^^^^^^^^^^^^^^^

To add columns, pass data whose column names are not already in the table. ``idx``
controls where they are placed: a single integer inserts a block of columns at that
position, and a sequence places each new column individually. The default is to
append columns at the end (``idx=-1``).

.. code-block:: python

    index = table.read_pandas().index
    wind = pd.DataFrame(
        {"wind_speed": [4.5, 6.1, 3.2, 5.8, 2.0, 7.4]},
        index=index,
    )
    table.insert(wind, idx=2)
    print(table.read_pandas())

.. code-block:: text

                temperature  humidity  wind_speed  rainfall
    date
    2024-01-01          2.1        88         4.5       4.2
    2024-01-02          1.4        91         6.1       0.0
    2024-01-03         -0.3        85         3.2       1.1
    2024-01-04          0.8        79         5.8       0.0
    2024-01-05          3.2        74         2.0       0.3
    2024-01-06          1.9        83         7.4       2.6

Updating data
^^^^^^^^^^^^^

``Table.update()`` overwrites stored values for the given index labels and columns.
Index values themselves cannot be updated this way; drop the old rows and insert
new ones instead.

.. code-block:: python

    correction = pd.DataFrame(
        {"temperature": [2.4], "rainfall": [3.8]},
        index=pd.DatetimeIndex(["2024-01-01"], name="date"),
    )
    table.update(correction)
    print(table.read_pandas())

.. code-block:: text

                temperature  humidity  wind_speed  rainfall
    date
    2024-01-01          2.4        88         4.5       3.8
    2024-01-02          1.4        91         6.1       0.0
    2024-01-03         -0.3        85         3.2       1.1
    2024-01-04          0.8        79         5.8       0.0
    2024-01-05          3.2        74         2.0       0.3
    2024-01-06          1.9        83         7.4       2.6

Dropping rows and columns
^^^^^^^^^^^^^^^^^^^^^^^^^

``Table.drop()`` removes rows and/or columns. Row filters use the same predicates
as reads.

.. code-block:: python

    table.drop(rows=["2024-01-06"])
    print(table.read_pandas())

.. code-block:: text

                temperature  humidity  wind_speed  rainfall
    date
    2024-01-01          2.4        88         4.5       3.8
    2024-01-02          1.4        91         6.1       0.0
    2024-01-03         -0.3        85         3.2       1.1
    2024-01-04          0.8        79         5.8       0.0
    2024-01-05          3.2        74         2.0       0.3

Drop columns with ``table.drop(cols=["humidity"])``. You can also call
``Table.drop_rows()`` and ``Table.drop_columns()`` directly.

Table metadata
--------------

Several methods inspect a table without loading the full dataset:

.. code-block:: python

    print(table.columns)
    print(table.shape)
    print(table.index)

.. code-block:: text

    ['date', 'temperature', 'humidity', 'wind_speed', 'rainfall']
    (5, 5)
    DatetimeIndex(['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04',
                   '2024-01-05'],
                  dtype='datetime64[us]', name='date', freq=None)

``shape`` is ``(rows, columns)`` and includes the index column. Other useful
methods include ``table.partition_size``, ``table.rename_columns()``,
``table.reorder_columns()``, ``table.astype()``, and ``table.repartition()``.

Snapshots
---------

Create a compressed backup of a table or a whole store, then restore it later.
The table or store is restored under the name stored in the snapshot.
``.tar.xz`` is appended to the path unless it already has that suffix.

.. code-block:: python

    from featherstore import snapshot

    table.create_snapshot("path/to/oslo_backup")
    # store.create_snapshot("path/to/weather_backup")

    snapshot.restore_table("weather", "path/to/oslo_backup")
    # snapshot.restore_store("path/to/weather_backup")

See the :doc:`API/Snapshot` page for details.

Next steps
----------

* :doc:`API Reference` — every class, function, and method
* :doc:`Benchmarks` — read and write performance compared with other formats
* :doc:`Overview` — requirements, source code, and contributing
