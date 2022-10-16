Quickstart
==========
This is a short introduction to FeatherStore basic functionality. For a complete guide to
FeatherStores classes, functions, and methods please visit the `API reference <API%20Reference.html>`_.

Installation
------------
To install FeatherStore, simply use pip

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

Reading and Writing Tables
--------------------------
FeatherStore supports reading and writing of Pandas DataFrames and Series, Polars DataFrames
and PyArrow tables.

First lets create a DataFrame to store.

.. code-block:: python

    import pandas as pd
    from numpy.random import randn

    dates = pd.date_range("2021-01-01", periods=5)
    df = pd.DataFrame(randn(5, 4), index=dates, columns=list("ABCD"))
    df

    >>                 A         B         C         D
    2021-01-01  0.402138 -0.016436 -0.565256  0.520086
    2021-01-02 -1.071026 -0.326358 -0.692681  1.188319
    2021-01-03  0.777777 -0.665146  1.017527 -0.064830
    2021-01-04 -0.835711 -0.575801 -0.650543 -0.411509
    2021-01-05 -0.649335 -0.830602  1.191749  0.396745

FeatherStore stores the tables as partitioned Feather files. The size of each partition
is defined by using the ``partition_size`` parameter when writing a table.

.. code-block:: python

    PARTITION_SIZE = 128  # bytes
    store.write_table('example_table', df, partition_size=PARTITION_SIZE)
    store.list_tables()

    >> ['example_table']

The advantage with using partitioned Feather files that you can do different operations
without loading in the full data.

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
    # store.read_arrow('example_table') for reading to Arrow Tables
    # store.read_polars('example_table') for reading to Polars DataFrames

    >>                 A         B         C         D
    2021-01-01  0.402138 -0.016436 -0.565256  0.520086
    2021-01-02 -1.071026 -0.326358 -0.692681  1.188319
    2021-01-03  0.777777 -0.665146  1.017527 -0.064830
    2021-01-04 -0.835711 -0.575801 -0.650543 -0.411509
    2021-01-05 -0.649335 -0.830602  1.191749  0.396745
    2021-01-06 -0.408125 -0.420920  0.632606  0.606950

We can also query parts of the data. FeatherStore uses predicate filtering to
only load the partitions and columns specified by the query.

By using sorted indices, FeatherStore allows for range-queries on rows by using
``{'before': end}``, ``{'after': start}`` and ``{'between': [start, end]}``

.. code-block:: python

    store.read_pandas('example_table', rows={'after': '2021-01-05'}, cols=['D', 'A'])

    # All range queries are inclusive
    >>                 D         A
    2021-01-05  0.396745 -0.649335
    2021-01-06  0.606950  0.408125

Inserting, Updating and Deleting Data
-------------------------------------
First, let's create a new table to work with:

.. code-block:: python

    index = [1, 3, 5, 6]
    df = pd.DataFrame(randn(4, 2), index=index, columns=list("AB"))
    df

    >>        A         B
    1 -0.041727  0.957139
    3 -0.272294 -1.758717
    5 -0.353684  1.550073
    6  1.275938  1.054702

We can use ``Store.select_table()`` to select a ``Table`` object, which contains
more features for working with tables.

.. code-block:: python

    table = store.select_table('example_table2')
    table.exists  # False
    table.write(df)
    table.exists

    >> True

One of those features is ``Table.insert()``, which allows for adding extra rows
into the table.

.. note::
    You can use ``Table.add_columns()`` to add extra columns.


.. code-block:: python

    df2 = pd.DataFrame(randn(2, 2), index=[4, 2], columns=list("AB"))
    table.insert(df2)  # Must have the same index and col dtypes as the stored df
    table.read_pandas()

    # The data will inserted into its sorted index position
    >>        A         B
    1 -0.041727  0.957139
    2  2.163615 -0.708871
    3 -0.272294 -1.758717
    4 -1.263981 -0.961670
    5 -0.353684  1.550073
    6  1.275938  1.054702

Other features include ``Table.update()`` and ``Table.drop()`` which updates and deletes data.

.. code-block:: python

    df3 = pd.DataFrame([[0, 2], [1, 3]], index=[1, 2], columns=list("AB"))
    #    A  B
    # 1  0  1
    # 2  2  3
    table.update(df3)
    table.drop(rows={'after': 5})
    # You can also drop columns using table.drop(cols=['col1', 'col2'])

    >>        A         B
    1  0.000000  1.000000
    2  2.000000  3.000000
    3 -0.272294 -1.758717
    4 -1.263981 -0.961670
