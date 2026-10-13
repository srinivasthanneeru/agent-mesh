// Command echo is the smallest compliant agent: it streams the prompt back
// word by word and reports any attachments.
//
//	go run ./cmd/echo [-name main] [-owner you]
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"regexp"
	"time"

	"agentmesh/agent"
	"agentmesh/internal/env"
	"agentmesh/protocol"
)

func main() {
	name := flag.String("name", "main", "instance name (5th subject token)")
	owner := flag.String("owner", env.Owner(), "owner (4th subject token)")
	flag.Parse()

	nc, err := env.Connect("echo-agent")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	words := regexp.MustCompile(`\S+|\s+`) // words and the spaces between them
	h, err := agent.New(agent.Options{
		Conn:          nc,
		Agent:         "echo",
		Owner:         *owner, // flag.String returns a pointer; * reads the value
		Name:          *name,
		Description:   "Echoes prompts back, word by word",
		AttachmentsOK: true,
		Heartbeat:     5 * time.Second,
		OnPrompt: func(ctx context.Context, e protocol.Envelope, res *agent.Responder) error {
			for _, w := range words.FindAllString("echo: "+e.Prompt, -1) {
				res.Send(w)
				time.Sleep(25 * time.Millisecond)
			}
			for _, a := range e.Attachments {
				n, _ := base64.StdEncoding.DecodeString(a.Content)
				res.Send(fmt.Sprintf("\n[attachment %s: %d bytes]", a.Filename, len(n)))
			}
			return nil
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := h.Start(); err != nil {
		log.Fatal(err)
	}

	ctx, stop := env.SignalContext()
	defer stop()
	<-ctx.Done() // block until Ctrl-C
	_ = h.Stop()
}
