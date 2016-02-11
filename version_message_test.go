// Tests error handling and support of various version ('V') messages.
//
// Also serves as an example on how to use indirections provided by
// pg_logplexcollector to do testing.
package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/deafbybeheading/femebe/buf"
	"github.com/deafbybeheading/femebe/core"
)

var versionCheckTests = []struct {
	Version string
	Ok      bool
}{
	{"PG-7.4.15/1", false},
	{"PG-7.4.15/logfebe-1", false},
	{"PG-9.0.0/logfebe-1", true},
	{"PG-9.1.1/logfebe-1", true},
	{"PG-9.2.2/logfebe-1", true},
	{"PG-9.2alpha1/logfebe-1", true},
	{"PG-9.3.0/logfebe-1", true},
	{"PG-9.3beta2/logfebe-1", true},
	{"PG-9.4.0/logfebe-1", true},
	{"PG-9.4devel/logfebe-1", true},
	{"PG-9.5.1/logfebe-1", true},
	{"PG-9.5beta2/logfebe-1", true},
	{"PG7.4.15/1", false},
}

func TestVersionCheck(t *testing.T) {
	for i, tt := range versionCheckTests {
		msgInit := func(dst *core.Message, exit exitFn) {
			b := bytes.Buffer{}
			buf.WriteCString(&b, tt.Version)
			dst.InitFromBytes('V', b.Bytes())
		}

		ok := true
		onBadVersion := func(args ...interface{}) {
			ok = false
		}
		processVerMsg(msgInit, onBadVersion)
		if ok != tt.Ok {
			t.Errorf("%d: Ver Message well formed: %v; want %v",
				i, ok, tt.Ok)
		}
	}
}

func TestVersionMsgInitErr(t *testing.T) {
	theErr := errors.New("An error; e.g. network difficulties")

	msgInit := func(dst *core.Message, exit exitFn) {
		exit(theErr)
	}

	sentinel := &msgInit

	exit := func(args ...interface{}) {
		// Since the error instance raised is injected, test that it
		// is precisely the error propagated to the exitFn.
		if args[0] != theErr {
			t.Fatal("Error propagated, but not the expected one")
		}

		panic(sentinel)
	}

	defer func() {
		if r := recover(); r != nil && r != sentinel {
			t.Fatal("Paniced, but not with the sentinel value")
		}

		// Success
	}()

	processVerMsg(msgInit, exit)
	t.Fatal("Message should call exit, aborting execution before this")
}
