package main

import (
	"io/ioutil"
	"net/url"
	"os"
	"reflect"
	"testing"
)

type fixturePair struct {
	json     []byte
	triplets []serveRecord
}

func (f *fixturePair) check(t *testing.T, sdb *serveDb) {
	for _, triplet := range f.triplets {
		rec, ok := sdb.identToServe[sKey{I: triplet.I, P: triplet.P}]
		if !ok {
			t.Fatalf("Expected to find identifier %q", triplet.I)
		}

		if !reflect.DeepEqual(triplet.u, rec.u) {
			t.Fatalf("Expected to resolve to %+v, "+
				"but got %v instead", triplet.u, rec.u)
		}
	}

}

func mustParseURL(us string) url.URL {
	u, err := url.Parse(us)
	if err != nil {
		panic(err)
	}
	return *u
}

var fixtures = []fixturePair{
	{
		json: []byte(`{"serves": ` +
			`[{"i": "apple", "url": "https://token:chocolate@localhost", ` +
			`"p": "/p1/log.sock"}, ` +
			`{"i": "banana", "url": "https://token:vanilla@localhost", ` +
			`"p": "/p2/log.sock"}]}`),
		triplets: []serveRecord{
			{sKey{I: "apple", P: "/p1/log.sock"},
				mustParseURL(
					"https://token:chocolate@localhost"),
				nil, "logfebe", "postgres", "[purple-rain-1984]",
				"brown"},
			{sKey{I: "banana", P: "/p2/log.sock"},
				mustParseURL(
					"https://token:vanilla@localhost"),
				nil, "logfebe", "postgres", "[purple-rain-1984]",
				"white"},
		},
	},
	{
		json: []byte(`{"serves": ` +
			`[{"i": "bed", ` +
			`"url": "https://token:pillow@localhost", ` +
			`"p": "/p1/log.sock"}, ` +
			`{"i": "nightstand", ` +
			`"url": "https://token:alarm-clock@localhost", ` +
			`"p": "/p2/log.sock"}]}`),
		triplets: []serveRecord{
			{sKey{I: "bed", P: "/p1/log.sock"},
				mustParseURL(
					"https://token:pillow@localhost"),
				nil, "logfebe", "postgres", "[purple-rain-1984]",
				"white"},
			{sKey{I: "nightstand", P: "/p2/log.sock"},
				mustParseURL(
					"https://token:alarm-clock@localhost"),
				nil, "logfebe", "postgres", "[purple-rain-1984]",
				"black"},
		},
	},
}

func newTmpDb(t *testing.T) string {
	name, err := ioutil.TempDir("", "test_")
	if err != nil {
		t.Fatalf("Could not create temporary directory for test: %v",
			err)
	}

	return name
}

func TestEmptyDB(t *testing.T) {
	name := newTmpDb(t)
	defer os.RemoveAll(name)

	sdb := newServeDb(name)
	updates, err := sdb.Poll()

	if err != nil {
		t.Fatalf("Poll on an empty directory should succeed, "+
			"instead failed: %v", err)
	}

	if !updates {
		t.Fatal("Expect updates for first poll in an empty database")
	}

	updates, err = sdb.Poll()
	if err != nil {
		t.Fatalf("Poll on an empty directory should succeed, "+
			"instead failed: %v", err)
	}

	if updates {
		t.Fatal("Expect no updates for second poll")
	}

}

func TestMultipleLoad(t *testing.T) {
	name := newTmpDb(t)
	defer os.RemoveAll(name)

	sdb := newServeDb(name)
	for i := range fixtures {
		fixture := &fixtures[i]
		ioutil.WriteFile(sdb.newPath(), fixture.json, 0400)

		if _, err := sdb.Poll(); err != nil {
			t.Fatalf("Poll should succeed with valid input, "+
				"instead: %v", err)
		}

		_, err := os.Stat(sdb.loadedPath())
		if err != nil {
			t.Fatalf("Input should be successfully loaded to %v, "+
				"but the file could not be stat()ed for some "+
				"reason: %v", sdb.loadedPath(), err)
		}

		fixture.check(t, sdb)
	}
}

// Write out some valid input to serves.new.
func writeLoadFixture(t *testing.T, sdb *serveDb, fixture *fixturePair) {
	ioutil.WriteFile(sdb.newPath(), fixture.json, 0400)
	if _, err := sdb.Poll(); err != nil {
		t.Fatalf("Poll should succeed with valid input, "+
			"instead: %v", err)
	}
}

func TestIntermixedGoodBadInput(t *testing.T) {
	name := newTmpDb(t)
	defer os.RemoveAll(name)

	sdb := newServeDb(name)

	fixture := &fixtures[0]
	writeLoadFixture(t, sdb, fixture)

	// Write a bad serves.new file.
	ioutil.WriteFile(sdb.newPath(), []byte(`{}`), 0400)
	if _, err := sdb.Poll(); err != nil {
		t.Fatalf("Poll should succeed with invalid input, "+
			"instead: %v", err)
	}

	// Confirm that the original, good fixture's data is still in
	// place.
	fixture.check(t, sdb)

	// Confirm that the serves.rej and last_error file have been
	// made.
	_, err := os.Stat(sdb.errPath())
	if err != nil {
		t.Fatalf("last_error file should exist: %v", err)
	}

	_, err = os.Stat(sdb.rejPath())
	if err != nil {
		t.Fatalf("serves.rej should exist: %v", err)
	}

	// Submit a new set of good input, to see if the last_error
	// and serves.rej are unlinked.
	secondFixture := &fixtures[1]
	writeLoadFixture(t, sdb, secondFixture)

	// Make sure new data was loaded properly.
	secondFixture.check(t, sdb)

	// Check that the old reject file and error file are gone.
	_, err = os.Stat(sdb.errPath())
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf("last_error file shouldn't exist: %v", err)
	}

	_, err = os.Stat(sdb.rejPath())
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf("serves.rej shouldn't exist: %v", err)
	}
}

func TestFirstTimeLoadPoll(t *testing.T) {
	name := newTmpDb(t)
	defer os.RemoveAll(name)

	sdb := newServeDb(name)

	// Write directly to the serves.loaded file, which is not the
	// normal way thing are done; Poll() should move things around
	// outside a test environment.
	fixture := &fixtures[0]
	ioutil.WriteFile(sdb.loadedPath(), fixture.json, 0400)

	if _, err := sdb.Poll(); err != nil {
		t.Fatalf("Poll should succeed with valid input, "+
			"instead: %v", err)
	}

	fixture.check(t, sdb)
}

func TestEmptyPoll(t *testing.T) {
	name := newTmpDb(t)
	defer os.RemoveAll(name)

	sdb := newServeDb(name)
	_, err := sdb.Poll()
	if err != nil {
		t.Fatalf("An empty database should not cause an error, "+
			"but got: %v", err)
	}

	if sdb.identToServe == nil {
		t.Fatal("An empty database should yield an " +
			"empty routing table.")
	}
}

func TestFirstLoadBad(t *testing.T) {
	name := newTmpDb(t)
	defer os.RemoveAll(name)

	sdb := newServeDb(name)

	// Write a bad serves.new file.
	ioutil.WriteFile(sdb.newPath(), []byte(`{}`), 0400)
	if _, err := sdb.Poll(); err != nil {
		t.Fatalf("Poll should succeed with invalid input, "+
			"instead: %v", err)
	}

	_, err := sdb.Poll()
	if err != nil {
		t.Fatalf("Rejected input should not cause an error, "+
			"but got: %v", err)
	}

	// Confirm that the serves.rej and last_error file have been
	// made.
	_, err = os.Stat(sdb.errPath())
	if err != nil {
		t.Fatalf("last_error file should exist: %v", err)
	}

	_, err = os.Stat(sdb.rejPath())
	if err != nil {
		t.Fatalf("serves.rej should exist: %v", err)
	}
}

func TestSnapshot(t *testing.T) {
	name := newTmpDb(t)
	defer os.RemoveAll(name)

	sdb := newServeDb(name)
	snap := sdb.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("Expect snapshot to have be empty")
	}

	fix := &fixtures[0]
	writeLoadFixture(t, sdb, fix)

	snap = sdb.Snapshot()
	if len(snap) != len(fix.triplets) {
		t.Fatalf("Expect snapshot to be filled, got %v", snap)
	}
}

func TestSnapReload(t *testing.T) {
	// Sketch how to use Poll() and Snapshot() together with
	// goroutines.
	name := newTmpDb(t)
	defer os.RemoveAll(name)

	sdb := newServeDb(name)
	writeLoadFixture(t, sdb, &fixtures[0])

	die := make(chan struct{})
	deaths := make(chan bool)

	justWait := func(die <-chan struct{}, r serveRecord) {
		t.Logf("justWait started with %v", r)

		for {
			t.Logf("justWait 'running' with %v", r)
			select {
			case <-die:
				t.Logf("justWait 'dies' with %v", r)
				deaths <- true
				return
			default:
				break
			}
		}
	}

	snapLoad := func(fix *fixturePair) {
		writeLoadFixture(t, sdb, fix)

		snap := sdb.Snapshot()
		for i := range snap {
			go justWait(die, snap[i])
		}

		close(die)

		// Confirm everyone dies
		for _ = range snap {
			<-deaths
		}
	}

	// Simulates a case where one calls Poll() repeatedly -- in
	// fact, writeLoadFixture used above does do this and checks
	// the results.
	for i := range fixtures {
		snapLoad(&fixtures[i])

		// Have to make a new 'die' channel for each
		// generation of goroutines.
		die = make(chan struct{})
	}

	// If anyone gorooutines are left and trying to write to the
	// deaths channel, (due to a programming error), try to force
	// a panic by closing the death channel.
	close(deaths)
}
