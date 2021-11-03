Quickstart
==========
This is a short introduction to FeatherStore, geared mainly for new users.

Installation
------------
To install FeatherStore, simply use *pip*

.. code-block::

    pip install featherstore

Starting Up
-----------
.. code-block:: python

    import featherstore as fs

To create and connect to a new database simply use:

.. code-block:: python

    fs.create_database('/path/to/database_folder')
    fs.connect('/path/to/database_folder')

You can later disconnect from the database by using ``fs.disconnect()``

Working with Stores
-------------------
A database consists of one or more stores. A store is the basic unit for organization
and where you can store your tables.

.. code-block:: python

    fs.create_store('store_1')
    fs.create_store('store_2')
    fs.list_stores()

    >> ['store_1', 'store_2']

.. code-block:: python

    fs.drop_store('store_2')
    fs.rename_store('store_1', 'example_store')
    # Connect to store
    store = fs.Store('example_store')

Reading and Writing tables
--------------------------
FeatherStore supports reading and writing of Pandas DataFrames and Series, Polars DataFrames
and PyArrow tables.

First lets create a DataFrame to store.

.. code-block:: python

    import pandas as pd
    from numpy.random import randn

    dates = pd.date_range("2021-01-01", periods=5)
    df = pd.DataFrame(randn(5, 4), index=dates, columns=list("ABCD"))

    >>                 A         B         C         D
    2021-01-01  0.402138 -0.016436 -0.565256  0.520086
    2021-01-02 -1.071026 -0.326358 -0.692681  1.188319
    2021-01-03  0.777777 -0.665146  1.017527 -0.064830
    2021-01-04 -0.835711 -0.575801 -0.650543 -0.411509
    2021-01-05 -0.649335 -0.830602  1.191749  0.396745

FeatherStore stores the tables as partitioned Feather files. The size of each partition is defined
by using the ``partition_size`` parameter when writing a table.

.. code-block:: python

    PARTITION_SIZE = 128  # bytes
    store.write_table('example_table', df, partition_size=PARTITION_SIZE)
    store.list_tables()

    >> ['example_table']

The advantage with using partitioned Feather files that you can do different operations without loading
in the full data.

.. code-block:: python

    # Creating a new DataFrame
    new_dates = pd.date_range("2021-01-06", periods=1)
    df1 = pd.DataFrame(randn(1, 4), index=new_dates, columns=list("ABCD"))
    # Appending to a FeatherStore table only loads in the last partition
    store.append_table('example_table', df1)

FeatherStore uses sorted indices to keep track of which partitions to open during
a given operation.

We can now read the stored data as Pandas DataFrame, Polars DataFrame or PyArrow Tables.

.. code-block:: python

    store.read_pandas('example_table')
    # store.read_table('example_table') for reading to Arrow Tables
    # store.read_polars('example_table') for reading to Polars DataFrames

    >>                 A         B         C         D
    2021-01-01  1.122392  0.265080  0.908843 -0.546288
    2021-01-02 -2.189536  0.593536  0.428618  1.159518
    2021-01-03  1.344019  0.723140  1.266272 -0.707655
    2021-01-04 -1.755134 -0.399792 -0.229055  0.733093
    2021-01-05 -0.871126  1.192000  0.425984  0.275433
    2021-01-06 -0.408125 -0.420920  0.632606  0.606950

We can also query parts of the data. FeatherStore uses predicate filtering to only load The
partitions and columns specified by the query.

By using sorted indices, FeatherStore allows for range-queries on rows by using
``['before', end]``, ``['after', start]`` and ``['between', start, end]``

.. code-block:: python

    store.read_pandas('example_table', rows=['after', '2021-01-05'], cols=['D', 'A'])

    # All range queries are inclusive
    >>                 D         A
    2021-01-05  0.275433 -0.871126
    2021-01-06  0.606950  0.408125
