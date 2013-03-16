pg_logplexcollector
-------------------

This implements a tool to accept the protocol emitted by `pg_logfebe`_
(version ``PG-9.2.x/logfebe-1``) and send it to logplex_ using the
library logplexc_.

It is necessary to download submodules and set GOPATH to build the
program with the most convenience by writing::

  $ git submodule init
  $ git submodule update
  $ make

Alternatively, if getting pg_logplexcollector for the first time, one
can initialize submodules upon ``git clone`` and build by running::

  $ git clone <pg_logplexcollector remote> --recursive
  $ cd pg_logplexcollector
  $ make

This is because git submodules are used to version and retrieve other
libraries, such as femebe and logplexc.  Having done this though, "go
build" and "go install" should work without complaint.

Quick Demo Setup
================

There exists a ``Makefile`` target for setting up most of what one
needs to demonstrate the entire system end-to-end.  It installs
everything into a subdirectory ``tmp``::

  $ PGSRC=git-repo-directory-for-postgres make testdb

  $ ./bin/logplexd &
  https://127.0.0.1:44786 # (dynamically generated)

  $ LOGPLEX_URL=https://127.0.0.1:44786 \
    SERVE_DB_DIR=tmp			\

  $ ./tmp/postgres/bin/postgres -D tmp/testdb

After this, one should be rewarded with printed HTTP requests, written
to standard output from ``logplexd``, forwarded by
``pg_logplexcollector``, emitted by the configured ``pg_logfebe`` and
the PostgreSQL server in which it resides.

Configuration
=============

For production use, two pieces of software must be configured:
``pg_logfebe``, and ``pg_logplexcollector``.

==========
pg_logfebe
==========

Configuring ``pg_logfebe`` requires Postgres 9.2.

It can be installed via ``make install``, per standard Postgres
extension mechanisms.  As with all such extensions, the most important
thing to confirm is that the ``pg_config`` program is both in path and
points to the correct binary Postgres installation.  You may need a
developer package from a distributor, such as the
``postgresql-server-dev-9.2`` package on Debian and Ubuntu.

Having done this, configure postgresql.conf with something like the
following::

  shared_preload_libraries='pg_logfebe'
  logfebe.unix_socket = '/tmp/log.sock'
  logfebe.identity = 'a-logging-identity'

``logfebe.unix_socket`` must be an absolute path to the unix socket
``pg_logfebe`` will attempt to connect to deliver logs.  When one sets
up ``pg_logplexcollector``, it must listen at that address.

``logfebe.identity`` is the non-secret 'identity' of the PostgreSQL
installation that will be delivered by ``pg_logfebe`` so that
``pg_logplexcollector`` can determine what logplex token (which is
secret) to use.

``pg_logfebe`` delivers logs on a best-effort basis, so it is
relatively harmless to start Postgres at this point if
``pg_logplexcollector`` is not yet running.

===================
pg_logplexcollector
===================

Configuring ``pg_logplexcollector`` consists of two concepts:

* LOGPLEX_URL: What logplex service to submit HTTP POSTs to.

* SERVE_DB_DIR: What directory contains the 'serve database'

SERVE_DB_DIR deserves more explanation:

In order to preserve the secrecy of logplex tokens and provide greater
security for tenants, ``pg_logplexcollector`` ties together three
pieces of information:

* A non-secret identity (configured in ``postgresql.conf`` with the
  GUC ``pg_logfebe.identity``)

* A secret logplex token

* A specific unix socket that will only accept connections for a given
  identity.

This is done by writing a JSON file into ``$SERVE_DB_DIR/serves.new``
that looks like this::

    {"serves": [
        {"i": "identity-1", "t": "token-1", "p": "/path/unix.socket-1"},
        {"i": "identity-2", "t": "token-2", "p": "/path/unix.socket-2"}
      ]
    }

One can confirm that the ``serves.new`` file has been loaded by
watching it be copied to ``$SERVE_DB_DIR/serves.loaded``.  At that
time, ``serves.new``, and any existing ``serves.rej`` or
``last_error`` file, if any, will be removed.

If one submits invalid input, ``serves.new`` is removed and
``serves.rej`` and a ``last_error`` file are emitted for inspection.
``serves.loaded`` does not change in this case.

``pg_logplexcollector`` will check for ``serves.new`` at various
arbitrary times.  Right now it occurs every ten seconds.

Putting these together, an invocation of ``pg_logplexcollector`` looks
like this::

    $ SERVE_DB_DIR=/path/to/servedb LOGPLEX_URL=https://somewhere.com/logs \
      ./pg_logplexcollector

``pg_logplexcollector`` logs client connections, disconnections, and
errors.  The former is to help determine if one's configuration is
working as intended.

Open Issues
===========

* Does not support HTTP client timeouts.  Unfortunately this doesn't
  look easy to do without implementing an entire Go ``net/http``
  ``RoundTripper``.

* Log formatting is not designed at all: it's just the first thing
  anyone has implemented.

.. _logplexc: https://github.com/fdr/logplexc

.. _pg_logfebe: https://github.com/fdr/pg_logfebe

.. _logplex: https://github.com/heroku/logplex
