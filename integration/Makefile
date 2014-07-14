#!/usr/bin/make -f
.PHONY: testdb postgres pg_logfebe

DIR := $(dir $(lastword $(MAKEFILE_LIST)))
TMP := $(DIR)tmp/
SRC := $(DIR)src/

testdb: $(TMP)testdb $(TMP)serves.new

# Everything below here is a monstrous hack to make playing with the
# toolchain a bit easier.  If one sets
# PGSRC=/a/local/git/repo/with/postgres then typing "make testdb"
# should copy in Postgres, configure, install it locally, check out
# the pg_logfebe extension, compile and install *that*, initdb, and
# then configure that initdb's postgresql.conf to load the extension
# and configure it with some defaults.

clean:
	rm -rf $(TMP)postgres
	rm -rf $(TMP)testdb
	rm -f $(TMP)serves*
	rm -f $(TMP)last_error*

veryclean:
	rm -rf $(SRC)postgres
	rm -rf $(SRC)pg_logfebe

postgres: $(TMP)postgres/bin/pg_config
pg_logfebe: $(TMP)postgres/lib/pg_logfebe.so

$(TMP)serves.new:
	printf '{"serves": [{"i": "test identity", "url": "%s", "p": "%s", "name": "humanname"}]}\n' \
		$(LOGPLEX_URL) $(realpath $(TMP))/testdb/log.sock > $@

# Copy files from other projects useful for testing.

$(SRC)pg_logfebe:
	mkdir -p $(SRC)
	git clone https://github.com/logplex/pg_logfebe $@

$(SRC)postgres/configure:
	mkdir -p $(SRC)

	(cd $(SRC) && \
	 git archive --remote=$(PGSRC) --prefix='postgres/' \
	   REL9_3_4 --prefix='postgres/' \
	| tar x)

# Compilation-oriented rules.

$(TMP)/postgres:
	mkdir -p $(TMP)/postgres

$(TMP)postgres/bin/pg_config: $(SRC)postgres/configure $(TMP)/postgres
	(cd $(SRC)postgres &&				\
	env CFLAGS='-O0 -g'				\
		./configure				\
		--prefix=$(realpath $(TMP))/postgres	\
		--enable-debug				\
		--enable-cassert			\
		--enable-depend &&			\
	make -sj8 install)

$(TMP)postgres/lib/pg_logfebe.so: $(SRC)pg_logfebe $(TMP)postgres/bin/pg_config
	(PATH=$(realpath $(TMP)/postgres/bin):$(PATH)	&& \
	cd $(SRC)pg_logfebe				&& \
	make -s install)

# Create a database directory and configure it.

$(TMP)testdb: $(TMP)postgres/bin/pg_config pg_logfebe
	$(TMP)postgres/bin/initdb -D $(TMP)testdb

	(echo "fsync='off'\n"\
	"shared_preload_libraries='pg_logfebe'\n"\
	"listen_addresses=''\n"\
	"unix_socket_directories='$(realpath $(TMP))'\n"\
	"logfebe.unix_socket='$(realpath $(TMP))/testdb/log.sock'\n" \
	"logfebe.identity='test identity'\n"\
	>> $(TMP)testdb/postgresql.conf)
