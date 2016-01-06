package main

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/heroku/log-shuttle"
)

// LoggerConfig represents a logger config.
type LoggerConfig struct {
	URL      url.URL
	Hostname string
	ProcID   string
}

const priority = 134

// Logger represents a logger client.
type Logger interface {
	Log(log []byte, host, procID string, when time.Time)
	Close()
}

// Shuttle is a logger client using log-shuttle.
type Shuttle struct {
	*shuttle.Shuttle

	Appname string
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
	conf.InputFormat = shuttle.InputFormatRFC5424
	conf.ComputeHeader()

	// Create shuttle.
	s := shuttle.NewShuttle(conf)
	s.Launch()
	return &Shuttle{
		Shuttle: s,
		Appname: conf.Appname,
	}, nil
}

// Log enqueues the given log.
func (s *Shuttle) Log(log []byte, host, procID string, when time.Time) {
	ts := when.UTC().Format(time.RFC3339)
	syslogPrefix := "<" + strconv.Itoa(priority) + ">1 " + ts + " " +
		host + " " + s.Appname + " " + procID + " - - "

	l := fmt.Sprintf("%s%s", syslogPrefix, log)
	ll := shuttle.NewLogLine([]byte(l), when)
	s.Enqueue(ll)
}

// Close close the shuttle
func (s *Shuttle) Close() {
	s.WaitForReadersToFinish()
	s.Land()
}
