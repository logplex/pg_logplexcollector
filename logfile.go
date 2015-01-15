package main

import (
	"bufio"
	"io"
	"log"
	"time"

	"github.com/logplex/logplexc"
)

func lineWorker(die dieCh, r *bufio.Reader, cfg logplexc.Config, sr *serveRecord) {
	cfg.Logplex = sr.u

	target, err := logplexc.NewClient(&cfg)
	if err != nil {
		log.Fatalf("could not create logging client: %v", err)
	}

	for {
		select {
		case <-die:
			return
		default:
			break
		}

		l, _, err := r.ReadLine()
		if len(l) > 0 {
			target.BufferMessage(134, time.Now(), "app", "redis", l)
		}

		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Fatal(err)
		}
	}
}
