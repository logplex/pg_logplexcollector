package main

import (
	"crypto/tls"
	"errors"
	"net/http"
	"net/url"
	"time"

	"github.com/heroku/log-shuttle"
	"github.com/logplex/logplexc"
)

// LoggerConfig represents a logger config.
type LoggerConfig struct {
	URL      url.URL
	Hostname string
	ProcID   string
}

// Logger represents a logger client.
type Logger interface {
	BufferMessage(priority int, when time.Time, host string, procID string, log []byte) error
	Close()
}

// Shuttle is a logger client using log-shuttle.
type Shuttle struct {
	*shuttle.Shuttle
}

// NewShuttle creates a shuttle client from a Config.
func NewShuttle(cfg *LoggerConfig) (*Shuttle, error) {
	token, ok := cfg.URL.User.Password()
	if !ok {
		return nil, errors.New("no logplex password provided")
	}

	// Create shuttle config.
	conf := shuttle.NewConfig()
	conf.LogsURL = cfg.URL.String()
	conf.Appname = token
	conf.Hostname = cfg.Hostname
	conf.Procid = cfg.ProcID
	conf.ComputeHeader()

	// Create shuttle.
	s := shuttle.NewShuttle(conf)
	s.Launch()
	return &Shuttle{s}, nil
}

// BufferMessage enqueues the given log.
func (s *Shuttle) BufferMessage(priority int, when time.Time, host string, procID string, log []byte) error {
	s.Enqueue(shuttle.NewLogLine(log, when))
	return nil
}

// Close close the shuttle
func (s *Shuttle) Close() {
	s.WaitForReadersToFinish()
	s.Land()
}

// NewLogplex creates a new logplexc Client from a Config.
func NewLogplex(cfg *LoggerConfig) (*logplexc.Client, error) {
	client := *http.DefaultClient
	client.Transport = &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}

	config := &logplexc.Config{
		HttpClient:         client,
		RequestSizeTrigger: 100 * KB,
		Concurrency:        3,
		Period:             time.Second / 4,
	}

	return logplexc.NewClient(config)
}
