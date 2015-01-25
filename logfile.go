package main

import (
	"bufio"
	"io"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/go-fsnotify/fsnotify"
	"github.com/logplex/logplexc"
)

var prefix = regexp.MustCompile(`^(\[\d*\] [^-*#]+|.*)`)

func lineWorker(die dieCh, f *os.File, cfg logplexc.Config, sr *serveRecord) {
	cfg.Logplex = sr.u

	target, err := logplexc.NewClient(&cfg)
	if err != nil {
		log.Fatalf("could not create logging client: %v", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	r := bufio.NewReader(f)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write {
					for {
						l, err := r.ReadBytes('\n')
						m := prefix.ReplaceAll(l, []byte(""))
						if len(m) > 1 {
							target.BufferMessage(134, time.Now(), "redis",
								sr.Name, m)
						}

						if err != nil {
							if err == io.EOF {
								break
							}
							log.Fatal(err)
						}
					}
				}
			case err := <-watcher.Errors:
				log.Fatal(err)
			}
		}
	}()

	if err := watcher.Add(f.Name()); err != nil {
		log.Fatal(err)
	}

	<-die
}
