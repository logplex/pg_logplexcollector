package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"time"

	"github.com/go-fsnotify/fsnotify"
	"github.com/logplex/logplexc"
)

var redisPrefix = regexp.MustCompile(`([-*#] .*)`)

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
						// Allow service specific changes
						l = parseLog(sr, l)

						// Don't emit empty lines
						l = bytes.TrimSpace(l)
						if len(l) == 0 {
							continue
						}

						// Append log prefix
						l = append([]byte(fmt.Sprintf("%s ", sr.Prefix)), l...)

						// Send the log line
						target.BufferMessage(134, time.Now(), sr.Service,
							sr.Service, l)

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

func parseLog(sr *serveRecord, l []byte) []byte {
	switch sr.Service {
	case "redis":
		m := redisPrefix.Find(l)
		if len(m) > 1 {
			return m
		}
	}
	return l
}
