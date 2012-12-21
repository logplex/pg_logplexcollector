pg_logplexcollector
-------------------
    
This implements a tool to accept the protocol emitted by `pg_logfebe`_
(version `PG9.2.x/1`) and send it to logplex_ using the library
``logplexc``.

It is necessary to download submodules and set GOPATH to build the
program with the most convenience by writing::

  $ export GOPATH=`pwd`
  $ git submodule init
  $ git submodule update
  $ go install pg_logplexcollector

This is because git submodules are used to version and retrieve other
libraries, such as femebe and logplexc.  Having done this though, "go
build" and "go install" should work without complaint.

The single weakest aspect of this implementation is that it does not
support log dropping or timeouts at this time, nor does it think very
hard about how to format the logs.  However, the entire system can be
demonstrated to work end-to-end.  Having set up pg_logfebe and
postgresql.conf something like this way::

  shared_preload_libraries='pg_logfebe'
  logfebe.unix_socket = '/tmp/log.sock'
  logfebe.identity = 't.my-logplex-token-string'

The placing of the logplex identity in postgresql.conf needs
revisiting: it appears to be both identification and authorization, so
it's not a good idea to expose this via PostgreSQL "SHOW" necessarily.

After this, one can run Postgres and start up pg_logplexcollector::

  $ go install pg_logplexcollector
  $ ./bin/pg_logplexcollector ./pg_logplexcollector /tmp/log.sock

And be rewarded by viewing one's Logplex output from the token
specified in logfebe.identity.

.. _pg_logfebe: https://github.com/fdr/pg_logfebe

.. _logplex: https://github.com/heroku/logplex
