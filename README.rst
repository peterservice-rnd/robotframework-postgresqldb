RobotFramework PostgreSQL Library
=================================

|Build Status|

Short Description
-----------------

`Robot Framework`_ library for working with PostgreSQL database, using `psycopg2`_.

Installation
------------

::

    pip install robotframework-postgresqldb

Documentation
-------------

See keyword documentation for robotframework-postgresqldb library in
folder ``docs``.

Example
-------
+-----------+------------------+
| Settings  |      Value       |
+===========+==================+
|  Library  | PostgreSQLDB     |
+-----------+------------------+

+---------------+---------------------------------------+--------------------+---------------------+----------+
|  Test cases   |                 Action                |   Argument         | Argument            | Argument |
+===============+=======================================+====================+=====================+==========+
|  Simple Test  | PostgreSQLDB.Connect To Postgresql    | postgres           | postgres            | password |
+---------------+---------------------------------------+--------------------+---------------------+----------+
|               | @{query}=                             | Execute Sql String | SELECT CURRENT_DATE |          |
+---------------+---------------------------------------+--------------------+---------------------+----------+
|               | Close All Postgresql Connections      |                    |                     |          |
+---------------+---------------------------------------+--------------------+---------------------+----------+

License
-------

Apache License 2.0

.. _Robot Framework: http://www.robotframework.org
.. _psycopg2: http://initd.org/psycopg/

.. |Build Status| image:: https://travis-ci.org/peterservice-rnd/robotframework-postgresqldb.svg?branch=master
   :target: https://travis-ci.org/peterservice-rnd/robotframework-postgresqldb