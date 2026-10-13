// A minimal agent in Go that speaks the same protocol as agent-mesh.
package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

// chunk is one streamed message: {"type": "...", "data": ...}
type chunk struct {
	Type string `json:"type"`
	Data any    `json:"data"`
}

func main() {
	// 1. Connect to the NATS server.
	nc, err := nats.Connect(nats.DefaultURL) // nats://127.0.0.1:4222
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()

	// 2. Register as a micro service named "agents" so callers can discover us.
	svc, err := micro.AddService(nc, micro.Config{
		Name:        "agents",
		Version:     "0.1.0",
		Description: "Upper-casing agent written in Go",
		Metadata: map[string]string{
			"agent": "goupper", "owner": "srini", "protocol_version": "0.3",
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	send := func(to string, c chunk) {
		b, _ := json.Marshal(c)
		nc.Publish(to, b)
	}

	// 3. The prompt endpoint: stream an ack, the answer, then an empty terminator.
	err = svc.AddEndpoint("prompt", micro.HandlerFunc(func(req micro.Request) {
		inbox := req.Reply() // the caller's private reply subject
		send(inbox, chunk{"status", "ack"})
		for _, word := range strings.Fields(string(req.Data())) {
			send(inbox, chunk{"response", strings.ToUpper(word) + " "})
			time.Sleep(50 * time.Millisecond) // pretend to think
		}
		nc.Publish(inbox, nil) // zero bytes, no headers = end of stream
	}),
		micro.WithEndpointSubject("agents.prompt.goupper.srini.main"),
		micro.WithEndpointQueueGroup("agents"),
		micro.WithEndpointMetadata(map[string]string{"max_payload": "1MB", "attachments_ok": "false"}),
	)
	if err != nil {
		log.Fatal(err)
	}

	heartbeat := func() []byte {
		b, _ := json.Marshal(map[string]any{
			"agent": "goupper", "owner": "srini", "instance_id": svc.Info().ID,
			"ts": time.Now().UTC().Format(time.RFC3339), "interval_s": 5,
		})
		return b
	}

	// 4. The status endpoint: reply once with a heartbeat-shaped payload.
	err = svc.AddEndpoint("status", micro.HandlerFunc(func(req micro.Request) {
		req.Respond(heartbeat())
	}),
		micro.WithEndpointSubject("agents.status.goupper.srini.main"),
		micro.WithEndpointQueueGroup("agents"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// 5. Heartbeats every 5 seconds so callers know we're alive.
	go func() {
		for range time.Tick(5 * time.Second) {
			nc.Publish("agents.hb.goupper.srini.main", heartbeat())
		}
	}()

	log.Println("listening on agents.prompt.goupper.srini.main")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	<-stop
	svc.Stop()
}
