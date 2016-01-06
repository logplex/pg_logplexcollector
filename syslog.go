package main

import (
	"log"
	"net"
	"os"
	"time"
)

func syslogWorker(die dieCh, conn net.PacketConn, cfg *LoggerConfig, sr *serveRecord) {
	// Make world-writable so anything can connect and send logs.
	// This may be be worth locking down more, but as-is unless
	// pg_logplexcollector and the Postgres server share the same
	// running user common umasks will be useless.
	fi, err := os.Stat(sr.P)
	if err != nil {
		log.Fatalf(
			"exiting, cannot stat just created socket %q: %v",
			sr.P, err)
	}

	err = os.Chmod(sr.P, fi.Mode().Perm()|0222)
	if err != nil {
		log.Fatalf(
			"exiting, cannot make just created socket "+
				"world-writable %q: %v",
			sr.P, err)
	}

	cfg.URL = sr.u

	buf := make([]byte, 9*KB)
	cfg.Hostname = "audit"
	cfg.ProcID = "-"
	target, err := NewShuttle(cfg)
	if err != nil {
		log.Fatalf("could not create auditing client: %v", err)
	}

	for {
		select {
		case <-die:
			target.Close()
			return
		default:
			break
		}

		err := conn.SetReadDeadline(time.Now().Add(time.Duration(1 * time.Second)))
		if err != nil {
			log.Fatalf("could not set connection deadline: %v", err)
		}

		n, _, err := conn.ReadFrom(buf)
		if n > 0 {
			// Just send the message wholesale, which
			// leads to some weird syslog-in-syslog
			// framing, but perhaps it's good enough.
			target.Log(append([]byte("instance_type=shogun identity="+sr.I+" "),
				buf[:n]...), "audit", "-", time.Now())
		}

		if err != nil {
			if err, ok := err.(net.Error); ok {
				if err.Timeout() || err.Temporary() {
					continue
				}
			}

			log.Fatalf("got syslog datagram error %v", err)
		}
	}
}
