package main

import (
	"bufio"
	"io"
	"log"
	"regexp"
	"time"

	"github.com/logplex/logplexc"
)

var prefix = regexp.MustCompile(`^(\[\d*\] [^-*#]+|.*)`)

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
		l = prefix.ReplaceAll(l, []byte(""))
		if len(l) > 0 {
			target.BufferMessage(134, time.Now(), "redis",
				sr.Name, l)
		}

		if err != nil {
			if err == io.EOF {
				continue
			}
			log.Fatal(err)
		}
	}
}
