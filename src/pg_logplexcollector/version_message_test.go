// Tests error handling and support of various version ('V') messages.
//
// Also serves as an example on how to use indirections provided by
// pg_logplexcollector to do testing.
package main

import (
	"bytes"
	"errors"
	"femebe"
	"testing"
)

func TestVersionOK(t *testing.T) {
	msgInit := func(dst *femebe.Message, exit exitFn) {
		buf := bytes.Buffer{}
		femebe.WriteCString(&buf, "PG-9.2.2/logfebe-1")

		dst.InitFromBytes('V', buf.Bytes())
	}

	exit := func(args ...interface{}) {
		t.Fatal("Ver Message is thought to be well formed. " +
			"Invariant broken.")
	}

	processVerMsg(msgInit, exit)
}

func TestVersionBadValue(t *testing.T) {
	msgInit := func(dst *femebe.Message, exit exitFn) {
		buf := bytes.Buffer{}
		femebe.WriteCString(&buf, "PG7.4.15/1")

		dst.InitFromBytes('V', buf.Bytes())
	}

	sentinel := &msgInit

	exit := func(args ...interface{}) {
		// Because this kind of error simply reports a string (with no
		// type taxonomy to go with it), and that seems good enough,
		// elide confirming precisely what the error is.
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

func TestVersionMsgInitErr(t *testing.T) {
	theErr := errors.New("An error; e.g. network difficulties")

	msgInit := func(dst *femebe.Message, exit exitFn) {
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
