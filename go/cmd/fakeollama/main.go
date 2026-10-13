// Command fakeollama runs a stand-in Ollama server for offline demos. It
// speaks Ollama's /api/chat and /api/tags formats and prints its URL.
//
//	go run ./cmd/fakeollama [-delay 60ms]
//	go run ./cmd/ollama -url <printed URL>
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"agentmesh/ollama/ollamatest"
)

func main() {
	delay := flag.Duration("delay", 60*time.Millisecond, "pause between tokens") // flag.Duration parses "60ms", "1s", ...
	flag.Parse()
	s := ollamatest.New("llama3.2:latest")
	defer s.Close()
	s.TokenDelay = *delay
	fmt.Println(s.URL)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
}
