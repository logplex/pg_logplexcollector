.PHONY: all clean veryclean postgres pg_logfebe testdb

all:
	GOPATH=`pwd`:$(GOPATH) go install pg_logplexcollector tool/logplexd

fmt:
	GOPATH=`pwd` go fmt pg_logplexcollector tool/logplexd

test:
	GOPATH=`pwd` go test pg_logplexcollector tool/logplexd

clean:
	rm -f bin/pg_logplexcollector
	rm -f bin/logplexd

# Everything below here is a monstrous hack to make playing with the
# toolchain a bit easier.  If one sets
# PGSRCDIR=/a/local/git/repo/with/postgres then typing "make testdb"
# should copy in Postgres, configure, install it locally, check out
# the pg_logfebe extension, compile and install *that*, initdb, and
# then configure that initdb's postgresql.conf to load the extension
# and configure it with some defaults.

veryclean: clean
	rm -rf tmp/src/postgres
	rm -rf tmp/src/pg_logfebe
	rm -rf tmp/postgres
	rm -rf tmp/testdb

postgres: tmp/postgres/bin/pg_config
pg_logfebe: tmp/postgres/lib/pg_logfebe.so
testdb: tmp/testdb

# Following targets copy and grab files

tmp/src/postgres/configure:
	mkdir -p tmp/src
	(cd tmp/src && \
	 git archive --remote=$(PGSRC) --prefix='postgres/' \
	   REL9_2_2 --prefix='postgres/' \
	| tar x)

tmp/src/pg_logfebe:
	cp -a src/pg_logfebe tmp/src/

# Compilation-oriented targets

tmp/postgres/bin/pg_config: tmp/src/postgres/configure
	(cd tmp/src/postgres &&			\
	env CFLAGS='-O0 -g'			\
		./configure			\
		--prefix=`pwd`/../../postgres	\
		--enable-debug			\
		--enable-cassert		\
		--enable-depend &&		\
	make -sj8 install)

tmp/postgres/lib/pg_logfebe.so: tmp/src/pg_logfebe postgres
	(PATH=`pwd`/tmp/postgres/bin:$(PATH)	&& \
	cd tmp/src/pg_logfebe			&& \
	make -s install)

# Creating a database and configuring it

tmp/testdb: postgres pg_logfebe
	tmp/postgres/bin/initdb -D tmp/testdb

	(echo "fsync='off'\n"\
	"shared_preload_libraries='pg_logfebe'\n"\
	"listen_addresses=''\n"\
	"unix_socket_directory='`pwd`/tmp'\n"\
	"logfebe.unix_socket='`pwd`/tmp/testdb/log.sock'\n"\
	"logfebe.identity='test identity'\n"\
	>> tmp/testdb/postgresql.conf)
