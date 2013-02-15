package main

import (
	"bytes"
	"crypto/tls"
	"femebe"
	"io"
	"log"
	"logplexc"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

const (
	KB = 1024
	MB = 1048576
)

// A function that, when called, panics.  The provider of the function
// is assumed to be able to recover from the panic, usually by using a
// sentinel value to ensure that only panics as a result of calling
// the exitFn is called.
//
// If an exitFn is part of the parameter list for a function, it is
// customary for it not to have an error return.
//
// This is useful when it's fairly clear that an error should be
// handled in one part of a program all the time, e.g. abort a
// goroutine after logging the cause of the exit.
type exitFn func(args ...interface{})

// Fills a message on behalf of the caller.  Often the closure will
// close over a femebe.MessageStream to provide a source of data for
// the filled message.
type msgInit func(dst *femebe.Message, exit exitFn)

// Used only in the close-to-broadcast style to exit goroutines.
type dieCh <-chan struct{}

// Read the version message, calling exit if this is not a supported
// version.
func processVerMsg(msgInit msgInit, exit exitFn) {
	var m femebe.Message

	msgInit(&m, exit)

	if m.MsgType() != 'V' {
		exit("expected version ('V') message, "+
			"but received %c", m.MsgType())
	}

	// hard-coded lengh limit, but it's very generous
	if m.Size() > 10*KB {
		log.Printf("oversized message string, msg size is %d",
			m.Size())
	}

	s, err := femebe.ReadCString(m.Payload())
	if err != nil {
		exit("couldn't read version string: %v", err)
	}

	if !strings.HasPrefix(s, "PG-9.2.") ||
		!strings.HasSuffix(s, "/logfebe-1") {
		exit("protocol version not supported: %s", s)
	}
}

// Process the identity ('I') message, reporting the identity therein.
func processIdentMsg(msgInit msgInit, exit exitFn) string {
	var m femebe.Message

	msgInit(&m, exit)

	// Read the remote system identifier string
	if m.MsgType() != 'I' {
		exit("expected identification ('I') message, "+
			"but received %c", m.MsgType())
	}

	// hard-coded lengh limit, but it's very generous
	if m.Size() > 10*KB {
		log.Printf("oversized message string, msg size is %d",
			m.Size())
	}

	s, err := femebe.ReadCString(m.Payload())
	if err != nil {
		exit("couldn't read identification string: %v",
			err)
	}

	return s
}

// Process a log message, sending it to the client.
func processLogMsg(die dieCh, lpc *logplexc.Client, msgInit msgInit,
	exit exitFn) {
	var m femebe.Message

	for {
		// Poll request to exit
		select {
		case <-die:
			return
		default:
			break
		}

		msgInit(&m, exit)

		// Refuse to handle any log message above an arbitrary
		// size.  Furthermore, exit the worker, closing the0
		// connection, so that the client doesn't even bother
		// to wait for this process to drain the oversized
		// item and anything following it; these will be
		// dropped.  It's on the client to gracefully handle
		// the error and re-connect after this happens.
		if m.Size() > 1*MB {
			exit("client %q sent oversized log record")
		}

		payload, err := m.Force()
		if err != nil {
			exit("could not retrieve payload of message: %v",
				err)
		}

		var lr logRecord
		parseLogRecord(&lr, payload, exit)
		processLogRec(&lr, lpc, exit)
	}
}

// Process a single logRecord value, buffering it in the logplex
// client.
func processLogRec(lr *logRecord, lpc *logplexc.Client, exit exitFn) {
	// Buffer to format the complete log message in.
	msgFmtBuf := bytes.Buffer{}

	// Helps with formatting a series of nullable strings.
	catOptionalField := func(prefix string, maybePresent *string) {
		if maybePresent != nil {
			if prefix != "" {
				msgFmtBuf.WriteString(prefix)
				msgFmtBuf.WriteString(": ")
			}

			msgFmtBuf.WriteString(*maybePresent)
			msgFmtBuf.WriteByte('\n')
		}
	}

	catOptionalField("", lr.ErrMessage)
	catOptionalField("Detail", lr.ErrDetail)
	catOptionalField("Hint", lr.ErrHint)
	catOptionalField("Query", lr.UserQuery)

	err := lpc.BufferMessage(time.Now(),
		"postgres",
		"postgres."+strconv.Itoa(int(lr.Pid)),
		msgFmtBuf.Bytes())
	if err != nil {
		exit(err)
	}
}

func logWorker(die dieCh, rwc io.ReadWriteCloser, cfg logplexc.Config,
	sr *serveRecord) {
	var err error
	stream := femebe.NewServerMessageStream("", rwc)

	var exit exitFn
	exit = func(args ...interface{}) {
		if len(args) == 1 {
			log.Printf("Disconnect client: %v", args[0])
		} else if len(args) > 1 {
			if s, ok := args[0].(string); ok {
				log.Printf(s, args[1:])
			} else {
				// Not an intended use case, but do
				// one's best to print something.
				log.Printf("Got a malformed exit: %v", args)
			}
		}

		panic(&exit)
	}

	// Recovers from panic and exits in an orderly manner if (and
	// only if) exit() is called; otherwise propagate the panic
	// normally.
	defer func() {
		rwc.Close()

		// &exit is used as a sentinel value.
		if r := recover(); r != nil && r != &exit {
			panic(r)
		}
	}()

	var msgInit msgInit
	msgInit = func(m *femebe.Message, exit exitFn) {
		err = stream.Next(m)
		if err == io.EOF {
			exit("client disconnects")
		} else if err != nil {
			exit("could not read next message: %v", err)
		}
	}

	// Protocol start-up; packets that are only received once.
	processVerMsg(msgInit, exit)
	ident := processIdentMsg(msgInit, exit)
	log.Printf("client connects with identifier %q", ident)

	// Resolve the identifier to a serve
	if sr.I != ident {
		exit("got unexpected identifier for socket: "+
			"path %s, expected %s, got %s", sr.P, sr.I, ident)
	}

	// Set up client with serve
	cfg.Token = sr.T
	client, err := logplexc.NewClient(&cfg)
	if err != nil {
		exit(err)
	}

	defer client.Close()

	processLogMsg(die, client, msgInit, exit)
}

func listen(die dieCh, logplexUrl url.URL, sr *serveRecord) {
	// Begin listening
	l, err := net.Listen("unix", sr.P)
	if err != nil {
		log.Fatalf(
			"exiting, cannot listen to %q: %v",
			sr.P, err)
	}

	// Make world-writable so anything can connect and send logs.
	// This may be be worth locking down more, but as-is unless
	// pg_logplexcollector and the Postgres server share the same
	// running user common umasks will be useless.
	fi, err := os.Stat(sr.P)
	if err != nil {
		log.Fatalf(
			"exiting, cannot stat just creatd socket %q: %v",
			sr.P, err)
	}

	err = os.Chmod(sr.P, fi.Mode().Perm()|0222)
	if err != nil {
		log.Fatalf(
			"exiting, cannot make just created socket "+
				"world-writable %q: %v",
			sr.P, err)
	}

	// Create a template config in each listening goroutine, for a
	// tiny bit more defensive programming against accidental
	// mutations of the base template that could cause
	// cross-tenant spillage.
	client := *http.DefaultClient
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	templateConfig := logplexc.Config{
		Logplex:            logplexUrl,
		HttpClient:         client,
		RequestSizeTrigger: 100 * KB,
		Concurrency:        3,
		Period:             3 * time.Second,

		// Set at connection start-up when the client
		// self-identifies.
		Token: "",
	}

	for {
		select {
		case <-die:
			log.Print("listener exits normally from die request")
			return
		default:
			break
		}

		conn, err := l.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
		}

		if err != nil {
			log.Fatalf("serve database suffers unrecoverable "+
				"error: %v", err)
		}

		go logWorker(die, conn, templateConfig, sr)
	}
}

func main() {
	// Input checking
	if len(os.Args) != 1 {
		log.Printf("Usage: pg_logplexcollector\n")
		os.Exit(1)
	}

	// Set up log prefix for all future system diagnostic
	// messages.
	log.SetPrefix("pg_logplexcollector ")

	// Signal handling: print dying gasp and and exit
	sigch := make(chan os.Signal)
	signal.Notify(sigch, os.Interrupt, os.Kill)
	go func() {
		for sig := range sigch {
			log.Printf("got signal %v", sig)
			if sig == os.Kill {
				os.Exit(2)
			} else if sig == os.Interrupt {
				os.Exit(0)
			}
		}
	}()

	if os.Getenv("LOGPLEX_URL") == "" {
		log.Fatal("LOGPLEX_URL is unset")
	}

	logplexUrl, err := url.Parse(os.Getenv("LOGPLEX_URL"))
	if err != nil {
		log.Fatalf("LOGPLEX_URL: could not parse: %q",
			os.Getenv("LOGPLEX_URL"))
	}

	// Set up serve database and perform its input checking
	sdbDir := os.Getenv("SERVE_DB_DIR")
	if sdbDir == "" {
		log.Fatal("SERVE_DB_DIR is unset: it must have the value " +
			"of an existing serve database.  " +
			"This can be an be an empty directory.")
	}

	sdb := newServeDb(sdbDir)

	var die chan struct{} = make(chan struct{})

	for {
		nw, err := sdb.Poll()
		if err != nil {
			if os.IsNotExist(err) {
				log.Fatal("SERVE_DB_DIR is set to a non-existant "+
					"directory: %v", err)
			}

			log.Fatalf(
				"serve database suffers an unrecoverable error: %v",
				err)
		}

		// New database state discovered: refresh the
		// listeners and signal all existing server goroutines
		// to exit.
		if nw {
			// Tell the generation of goroutines from the
			// last version of the database to die.
			close(die)

			// A new channel value for a new generation of
			// listen/accept goroutines
			die = make(chan struct{})

			// Set up new servers for the new database state.
			snap := sdb.Snapshot()
			for i := range snap {
				os.Remove(snap[i].P)
				go listen(die, *logplexUrl, &snap[i])
			}
		}

		time.Sleep(10 * time.Second)
	}
}
