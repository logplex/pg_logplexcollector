package main

import (
	"log"
	"net"
	"time"

	"github.com/logplex/logplexc"
)

func syslogWorker(die dieCh, conn net.PacketConn, cfg logplexc.Config,
	sr *serveRecord) {
	cfg.Logplex = sr.u
	buf := make([]byte, 9*KB)
	target, err := logplexc.NewClient(&cfg)
	if err != nil {
		log.Fatalf("could not create auditing client: %v", err)
	}

	for {
		select {
		case <-die:
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
			target.BufferMessage(134, time.Now(),
				"audit", "-", append([]byte(
					"instance_type=shogun identity="+
						sr.I+" "), buf[:n]...))
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
