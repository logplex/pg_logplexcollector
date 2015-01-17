package main

import (
	"bufio"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/logplex/logplexc"
)

const (
	KB = 1024
	MB = 1048576
)

// Used only in the close-to-broadcast style to exit goroutines.
type dieCh <-chan struct{}

func listen(die dieCh, sr *serveRecord) {
	// Begin listening
	var l net.Listener
	var pc net.PacketConn
	var f io.Reader
	var err error

	switch sr.protocol {
	case "syslog":
		pc, err = net.ListenPacket("unixgram", sr.P)
	case "logfile":
		f, err = os.Open(sr.P)
	default:
		l, err = net.Listen("unix", sr.P)
	}

	if err != nil {
		log.Fatalf(
			"exiting, cannot listen to %q: %v",
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
		HttpClient:         client,
		RequestSizeTrigger: 100 * KB,
		Concurrency:        3,
		Period:             time.Second / 4,
	}

	switch sr.protocol {
	case "logfebe":
		logWorker(die, l, templateConfig, sr)
	case "syslog":
		go syslogWorker(die, pc, templateConfig, sr)
	case "logfile":
		go lineWorker(die, bufio.NewReader(f), templateConfig, sr)
	default:
		log.Fatalf("cannot comprehend protocol %v specified in "+
			"servedb.", sr.protocol)
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

	// Set up serve database and perform its input checking
	sdbDir := os.Getenv("SERVE_DB_DIR")
	if sdbDir == "" {
		log.Fatal("SERVE_DB_DIR is unset: it must have the value " +
			"of an existing serve database.  " +
			"This can be an be an empty directory.")
	}

	sdb := newServeDb(sdbDir)

	die := make(chan struct{})

	// Brutal hack to get around pathological Go use of virtual
	// memory: die once in a while.  A supervisor (e.g. Upstart)
	// should restart the process.
	deathClock := time.Now().Add(time.Hour)

	for {
		nw, err := sdb.Poll()
		if err != nil {
			if os.IsNotExist(err) {
				log.Fatal("SERVE_DB_DIR is set to a non-existant "+
					"directory: %v", err)
			}

			log.Fatalf(
				"serve database suffers an unrecoverable error in listen: %v",
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
				go listen(die, &snap[i])
			}
		}

		time.Sleep(10 * time.Second)

		if time.Now().After(deathClock) {
			log.Printf("Exiting on account of deadline, "+
				"to prevent memory bloat: %v", deathClock)
			os.Exit(101)
		}
	}
}
