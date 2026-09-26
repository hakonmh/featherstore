=========================
Databases and connections
=========================

A FeatherStore database is a directory on disk. The process holds at most one
active connection at a time. All store and table operations use that connection.

Creating and connecting
-----------------------

:func:`~featherstore.connection.create_database` creates the directory (if
needed), writes the database marker, and connects by default:

.. code-block:: python

    import featherstore as fs

    fs.create_database("path/to/db")
    print(fs.is_connected())
    print(fs.current_db())

.. code-block:: text

    True
    .../path/to/db

Useful parameters:

* ``errors='raise'`` (default) refuses a non-empty directory that is not already
  a database. ``errors='ignore'`` will create a marker in an existing directory
  when it is not already a database, or reconnect if it already is.
* ``connect=True`` (default) opens a connection to the new database. A new
  connection replaces any previous one.
* Paths expand ``~`` to the user home directory.

When the database already exists, use :func:`~featherstore.connection.connect`:

.. code-block:: python

    fs.connect("path/to/db")

:func:`~featherstore.connection.database_exists` checks for the marker without
connecting. :func:`~featherstore.connection.disconnect` leaves the current
database.

One process-global connection
-----------------------------

FeatherStore tracks a single active database path. Calling ``connect`` or
``create_database(..., connect=True)`` replaces the previous connection. There is
no connection object to pass around in application code; helpers such as
:func:`~featherstore.connection.current_db` and
:func:`~featherstore.connection.is_connected` inspect the global state.

Database marker and format versions
-----------------------------------

The marker file ``.featherstore`` identifies the directory as a database and
records format versions (metadata schema and partition layout). Connecting to a
database whose versions do not match this install raises
:exc:`~featherstore.exceptions.IncompatibleDatabaseVersionError`. Connecting to a
directory without a valid marker raises
:exc:`~featherstore.exceptions.NotADatabaseError`.

Dropping a database
-------------------

:func:`~featherstore.connection.drop_database` deletes the database directory.
Constraints:

* You must be connected to that database (``path`` must be the current database).
* The database must contain no stores
  (:exc:`~featherstore.exceptions.DatabaseNotEmptyError` otherwise).
* On success, FeatherStore disconnects.
* ``warnings='warn'`` (default) warns if the path does not exist;
  ``warnings='ignore'`` stays quiet.

.. code-block:: python

    fs.drop_store("weather")  # stores must be empty first, then gone
    fs.drop_database("path/to/db")

See also
--------

* :doc:`stores` — creating stores inside a database
* :doc:`errors` — domain exceptions and ``errors`` / ``warnings`` parameters
* :doc:`/API/Connection` — full function reference
