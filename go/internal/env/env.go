// Package env holds small helpers shared by the programs in cmd/.
// Packages under internal/ can only be imported by code in this module.
package env

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/nats-io/nats.go"

	"agentmesh/protocol"
)

// Connect opens a NATS connection using:
//
//	NATS_URL    server address (default nats://127.0.0.1:4222)
//	NATS_CREDS  optional .creds file (operator mode / Synadia Cloud)
func Connect(name string) (*nats.Conn, error) {
	url := os.Getenv("NATS_URL")
	if url == "" {
		url = nats.DefaultURL
	}
	opts := []nats.Option{nats.Name(name), nats.MaxReconnects(-1)} // -1 = keep reconnecting forever
	if creds := os.Getenv("NATS_CREDS"); creds != "" {
		opts = append(opts, nats.UserCredentials(creds))
	}
	return nats.Connect(url, opts...) // `opts...` spreads a slice into variadic arguments
}

// Owner is the owner token: $MESH_OWNER, else the sanitized $USER.
func Owner() string {
	for _, v := range []string{os.Getenv("MESH_OWNER"), os.Getenv("USER")} {
		if t := protocol.SanitizeToken(v); t != "" {
			return t
		}
	}
	return "anon"
}

// SignalContext returns a context cancelled on Ctrl-C or SIGTERM.
func SignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
