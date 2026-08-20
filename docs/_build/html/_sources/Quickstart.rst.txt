Quickstart
==========

This is a short introduction to FeatherStore and its basic features.
For a complete guide to FeatherStore's classes, functions, and methods, see the
`API reference <API%20Reference.html>`_.

| The project is hosted on PyPI at:
| https://pypi.org/project/FeatherStore/

Installation
++++++++++++

| To install FeatherStore, use pip:

.. code-block::

    pip install featherstore

| or

.. code-block::

    pip install git+https://github.com/hakonmh/featherstore.git

| to install the latest version from GitHub.

Requirements
------------

FeatherStore 0.3.0 requires Python 3.11 or newer and the following packages:

* pandas >= 2.2.0
* polars[timezone] >= 1.21.0
* pyarrow >= 14.0.0

Getting started
---------------

.. code-block:: python

    import featherstore as fs

To create and connect to a new database, use:

.. code-block:: python

    fs.create_database('/path/to/database_folder')
    fs.connect('/path/to/database_folder')

You can check whether a path is already a database with ``fs.database_exists()``.
You can later disconnect from the database with ``fs.disconnect()``.

Working with stores
-------------------

A database contains one or more stores. A store groups tables and is the main
unit of organization.

.. code-block:: python

    fs.create_store('store_1')
    fs.create_store('store_2')
    fs.list_stores()

    >> ['store_1', 'store_2']

.. code-block:: python

    fs.drop_store('store_2')
    fs.rename_store('store_1', to='example_store')
    # Connect to the store
    store = fs.Store('example_store')

Reading and writing tables
--------------------------

FeatherStore can read and write Pandas DataFrames and Series, Polars DataFrames
and Series, and PyArrow Tables.

First, let's create a DataFrame to store.

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

FeatherStore stores tables as partitioned Feather files. Set the size of each
partition with the ``partition_size`` parameter when writing a table.

.. code-block:: python

    PARTITION_SIZE = 128  # bytes
    store.write_table('example_table', df, partition_size=PARTITION_SIZE)
    store.list_tables()

    >> ['example_table']

Partitioned Feather files let you run many operations without loading the full
dataset.

.. code-block:: python

    # Create a new DataFrame
    new_dates = pd.date_range("2021-01-06", periods=1)
    df1 = pd.DataFrame(randn(1, 4), index=new_dates, columns=list("ABCD"))
    # Appending to a FeatherStore table only loads the last partition
    store.append_table('example_table', df1)

FeatherStore uses sorted indices to decide which partitions to open for a given
operation.

You can read the stored data as a Pandas DataFrame, a Polars DataFrame, or a
PyArrow Table.

.. code-block:: python

    store.read_pandas('example_table')
    # store.read_arrow('example_table') for Arrow Tables
    # store.read_polars('example_table') for Polars DataFrames

    >>                 A         B         C         D
    2021-01-01  0.402138 -0.016436 -0.565256  0.520086
    2021-01-02 -1.071026 -0.326358 -0.692681  1.188319
    2021-01-03  0.777777 -0.665146  1.017527 -0.064830
    2021-01-04 -0.835711 -0.575801 -0.650543 -0.411509
    2021-01-05 -0.649335 -0.830602  1.191749  0.396745
    2021-01-06 -0.408125 -0.420920  0.632606  0.606950

You can also query parts of the data. FeatherStore uses predicate filtering to
load only the partitions and columns specified by the query.

Sorted indices also allow range queries on rows with
``{'before': end}``, ``{'after': start}``, and ``{'between': [start, end]}``.

.. code-block:: python

    store.read_pandas('example_table', rows={'after': '2021-01-05'}, cols=['D', 'A'])

    # All range queries are inclusive
    >>                 D         A
    2021-01-05  0.396745 -0.649335
    2021-01-06  0.606950  0.408125

Inserting, updating, and deleting data
--------------------------------------

First, create a new table to work with:

.. code-block:: python

    index = [1, 3, 5, 6]
    df = pd.DataFrame(randn(4, 2), index=index, columns=list("AB"))
    df

    >>        A         B
    1 -0.041727  0.957139
    3 -0.272294 -1.758717
    5 -0.353684  1.550073
    6  1.275938  1.054702

Use ``Store.select_table()`` to select a ``Table`` object, which includes more
methods for working with tables.

.. code-block:: python

    table = store.select_table('example_table2')
    table.exists()  # False
    table.write(df)
    table.exists()

    >> True

One of those methods is ``Table.insert()``, which inserts rows or columns
depending on the column names of the input data. If the input column names
match the stored table, rows are inserted; otherwise columns are inserted.

``Table.update()``, ``Table.insert_rows()``, ``Table.insert_columns()``, and
``Table.insert()`` accept Pandas DataFrames and Series, Polars DataFrames, and
PyArrow Tables as input. Polars Series is not supported for these edit APIs.

You can also call ``Table.insert_rows()`` or ``Table.insert_columns()`` directly
when you want to be explicit about the operation.

Pass ``warnings='ignore'`` to suppress sorting warnings when inserting rows or
columns with an unsorted index (the default is ``warnings='warn'``).

.. code-block:: python

    df2 = pd.DataFrame(randn(2, 2), index=[4, 2], columns=list("AB"))
    table.insert(df2)  # matching column names -> inserts rows
    table.read_pandas()

    # The data is inserted at its sorted index position
    >>        A         B
    1 -0.041727  0.957139
    2  2.163615 -0.708871
    3 -0.272294 -1.758717
    4 -1.263981 -0.961670
    5 -0.353684  1.550073
    6  1.275938  1.054702

To add columns, pass data whose column names are not already in the table.
Use ``idx`` to control where the new columns are placed. A single integer
inserts a block of columns at that position; a sequence places each column
individually.

.. code-block:: python

    index = table.read_pandas().index
    new_cols = pd.DataFrame(randn(6, 2), index=index, columns=['C', 'D'])
    table.insert(new_cols, idx=[1, 3])

    # Append a single column to the end
    table.insert_columns(pd.DataFrame({'E': randn(6)}, index=index), idx=-1)

Other methods include ``Table.update()`` and ``Table.drop()``, which update and
delete data.

.. code-block:: python

    df3 = pd.DataFrame([[0, 1], [2, 3]], index=[1, 2], columns=list("AB"))
    #    A  B
    # 1  0  1
    # 2  2  3
    table.update(df3)
    table.drop(rows={'after': 5})
    # You can also drop columns with table.drop(cols=['col1', 'col2'])

    >>        A         B
    1  0.000000  1.000000
    2  2.000000  3.000000
    3 -0.272294 -1.758717
    4 -1.263981 -0.961670
