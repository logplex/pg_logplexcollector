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

	if !strings.HasPrefix(s, "PG9.2.") ||
		!strings.HasSuffix(s, "/1") {
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
func processLogMsg(lpc *logplexc.Client, msgInit msgInit, exit exitFn) {
	var m femebe.Message

	for {
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

	err := lpc.BufferMessage(time.Now(), "postgres-"+lr.SessionId,
		msgFmtBuf.Bytes())
	if err != nil {
		exit(err)
	}
}

func logWorker(rwc io.ReadWriteCloser, cfg logplexc.Config) {
	var m femebe.Message
	var err error
	stream := femebe.NewServerMessageStream("", rwc)

	// Exit in an orderly manner if (and only if) exit() is
	// called; otherwise propagate the panic normally.
	defer func() {
		rwc.Close()

		if r := recover(); r != nil && r != &m {
			panic(r)
		}
	}()

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

		panic(&m)
	}

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

	cfg.Token = ident
	client, err := logplexc.NewClient(&cfg)
	if err != nil {
		exit(err)
	}

	defer client.Close()

	processLogMsg(client, msgInit, exit)
}

func main() {
	// Input checking
	if len(os.Args) != 2 {
		log.Printf("Usage: pg_logplexcollector LISTENADDR\n")
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

	logplexUrl, err := url.Parse(os.Getenv("LOGPLEX_URL"))
	if err != nil {
		log.Fatalf("Could not parse logplex endpoint %q: %v",
			os.Getenv("LOGPLEX_URL"), err)
	}

	client := *http.DefaultClient
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	templateConfig := logplexc.Config{
		Logplex:            *logplexUrl,
		HttpClient:         client,
		RequestSizeTrigger: 100 * KB,
		Concurrency:        3,
		Period:             3 * time.Second,

		// Set at connection start-up when the client
		// self-identifies.
		Token: "",
	}

	// Begin listening
	l, err := net.Listen("unix", os.Args[1])
	if err != nil {
		log.Fatalf(
			"exiting, cannot listen to %q: %v",
			os.Args[1], err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
		}

		go logWorker(conn, templateConfig)
	}
}
