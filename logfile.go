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

var prefix = regexp.MustCompile(`([-*#] .*)`)

func lineWorker(die dieCh, f *os.File, cfg logplexc.Config, sr *serveRecord) {
	cfg.Logplex = sr.u

	target, err := logplexc.NewClient(&cfg)
	if err != nil {
		log.Fatalf("could not create logging client: %v", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("can't create watcher: %v", err)
	}
	defer watcher.Close()

	r := bufio.NewReader(f)

	go func() {
		for {
			select {
			case <-die:
				return
			case event := <-watcher.Events:
				if event.Op&fsnotify.Write == fsnotify.Write {
					for {
						l, err := r.ReadBytes('\n')
						m := prefix.Find(l)
						if len(m) > 1 {
							target.BufferMessage(134, time.Now(), "redis",
								sr.Name, m)
						}

						if err != nil {
							if err == io.EOF {
								break
							}
							log.Printf("unexpected read error: %v", err)
						}
					}
				}
			case err := <-watcher.Errors:
				log.Printf("unexpected fs watch error %v:", err)
			}
		}
	}()

	if err := watcher.Add(f.Name()); err != nil {
		log.Printf("can't add watcher: %v", err)
	}

	<-die
}
