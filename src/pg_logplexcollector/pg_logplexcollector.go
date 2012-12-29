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
	KB uint32 = 1024
	MB        = 1048576
)

func logWorker(rwc io.ReadWriteCloser, cfg logplexc.Config) {
	var m femebe.Message
	var err error
	stream := femebe.NewServerMessageStream("", rwc)

	// Exit in an orderly manner if (and only if) exitFn() is
	// called.
	defer func() {
		rwc.Close()

		if r := recover(); r != nil && r != &m {
			panic(r)
		}
	}()

	// Convention to exit the function; useful in closures that
	// may need to jump block scopes. 
	exitFn := func(args ...interface{}) {
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

	// Function to get the next message and fill m, or immediately
	// exit the parent function after logging the offending error.
	mustFillNext := func() {
		err = stream.Next(&m)
		if err != nil {
			log.Printf("could not read next message: %v", err)
			exitFn(err)
		}
	}

	// Read the version message and bomb out if it looks funny in
	// any way.  This is also the area that will need amendment as
	// new versions of the protocol and new versions of Postgres
	// (which may change what error fields are available) are
	// supported.
	mustCheckVersion := func() {
		if m.MsgType() != 'V' {
			log.Printf("expected version ('V') message, "+
				"but received %c", m.MsgType())
			exitFn(nil)
		}

		// hard-coded lengh limit, but it's very generous
		if m.Size() > 10*KB {
			log.Printf("oversized message string, msg size is %d",
				m.Size())
		}

		s, err := femebe.ReadCString(m.Payload())
		if err != nil {
			log.Printf("couldn't read version string: %v", err)
			exitFn(nil)
		}

		if !strings.HasPrefix(s, "PG9.2.") ||
			!strings.HasSuffix(s, "/1") {
			exitFn("protocol version not supported: %s", s)
		}
	}

	// Read the remote system identifier string
	mustReadIdentifier := func() string {
		if m.MsgType() != 'I' {
			log.Printf("expected identification ('I') message, "+
				"but received %c", m.MsgType())
			exitFn()
		}

		// hard-coded lengh limit, but it's very generous
		if m.Size() > 10*KB {
			log.Printf("oversized message string, msg size is %d",
				m.Size())
		}

		s, err := femebe.ReadCString(m.Payload())
		if err != nil {
			log.Printf("couldn't read identification string: %v",
				err)
			exitFn()
		}

		return s
	}

	// Protocol start-up; packets that are only received once.
	mustFillNext()
	mustCheckVersion()
	mustFillNext()
	ident := mustReadIdentifier()
	log.Printf("client connects with identifier %q", ident)

	cfg.Token = ident
	client, err := logplexc.NewClient(&cfg)
	if err != nil {
		exitFn(err)
	}

	for {
		mustFillNext()

		// Refuse to handle any log message above an arbitrary
		// size.  Furthermore, exit the worker, closing the
		// connection, so that the client doesn't even bother
		// to wait for this process to drain the oversized
		// item and anything following it; these will be
		// dropped.  It's on the client to gracefully handle
		// the error and re-connect after this happens.
		if m.Size() > 1*MB {
			exitFn("client %q sent oversized log record, "+
				"disconnecting", ident)
		}

		payload, err := m.Force()
		if err != nil {
			exitFn("could not retreive payload of message: %v",
				err)
		}

		var lr logRecord
		parseLogRecord(&lr, payload, exitFn)

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

		client.BufferMessage(time.Now(), "postgres-"+lr.SessionId,
			msgFmtBuf.Bytes())

		// Post the buffered message
		resp, err := client.PostMessages()
		if err != nil {
			exitFn(err)
		}

		// Can be more carefully checked and reported on to
		// indicate configuration or Logplex issues.
		resp.Body.Close()
	}
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
		Logplex: *logplexUrl,

		// Set at connection start-up when the client self-identifies.
		Token: "",

		HttpClient: client,
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
