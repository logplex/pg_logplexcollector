package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"os"
	"os/signal"
)

type LogplexPrint struct{}

func (*LogplexPrint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	dump, err := httputil.DumpRequest(r, true)
	if err != nil {
		log.Printf("Could not dump request: %#v", err)
	}

	log.Printf("%s", dump)

	// Respond saying everything is OK.
	w.WriteHeader(http.StatusNoContent)
}

func main() {
	s := httptest.NewTLSServer(&LogplexPrint{})
	fmt.Println(s.URL)

	// Signal handling:
	sigch := make(chan os.Signal)
	signal.Notify(sigch, os.Interrupt, os.Kill)
	for sig := range sigch {
		log.Printf("got signal %v", sig)
		if sig == os.Kill {
			os.Exit(2)
		} else if sig == os.Interrupt {
			os.Exit(0)
		}
	}
}
