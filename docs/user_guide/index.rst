==========
User Guide
==========

The User Guide covers FeatherStore by topic area. Each page introduces a topic
(such as writing tables or selecting rows) and shows how FeatherStore approaches
it, with examples throughout.

Users new to FeatherStore should start with :doc:`10min`.

For a product overview, installation, and when to use FeatherStore, see
:doc:`/Overview`. For method signatures and exception classes, see the
:doc:`/API Reference`. For performance numbers, see :doc:`/Benchmarks`.

How to read these guides
------------------------

In these guides you will see input code inside code blocks such as:

.. code-block:: python

    import featherstore as fs
    fs.create_database("path/to/db")

and printed output in a matching text block:

.. code-block:: text

    ['weather']

Examples are ordinary Python. Where a guide shows ``print(...)``, the text block
beneath it is what that call prints.

Guides
------

.. toctree::
   :maxdepth: 2

   10min
   data_model
   database
   stores
   tables
   io
   backends
   indexing
   table_index
   append
   insert
   update
   drop
   columns
   metadata
   partitioning
   snapshots
   errors
   gotchas
