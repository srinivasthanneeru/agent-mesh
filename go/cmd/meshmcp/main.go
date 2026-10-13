// Command meshmcp is an MCP server for the agent mesh. MCP clients (Claude
// Desktop, Claude Code, Codex, ...) start it and talk to it over stdin and
// stdout, so it must never print anything else to stdout: logs go to stderr.
//
//	Claude Code:  claude mcp add agent-mesh -- /path/to/bin/meshmcp
//	Codex:        [mcp_servers.agent-mesh] command = "/path/to/bin/meshmcp"  (~/.codex/config.toml)
//	Claude Desktop: "mcpServers": {"agent-mesh": {"command": "/path/to/bin/meshmcp"}}
//	                in ~/Library/Application Support/Claude/claude_desktop_config.json
//
// Environment: NATS_URL (default nats://127.0.0.1:4222), NATS_CREDS.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"agentmesh/internal/env"
	"agentmesh/kvstore"
	"agentmesh/mesh"
	"agentmesh/meshmcp"
)

func main() {
	log.SetOutput(os.Stderr) // stdout belongs to the MCP protocol
	log.SetPrefix("[meshmcp] ")

	nc, err := env.Connect("meshmcp")
	if err != nil {
		log.Fatalf("cannot connect to NATS (is it running? go/scripts/demo.sh starts it): %v", err)
	}
	defer nc.Drain()

	m := mesh.New(nc)
	if err := m.Watch(); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	store, warning := kvstore.Open(ctx, nc)
	cancel()
	if warning != "" {
		log.Printf("warning: %s", warning)
	}

	sig, stop := env.SignalContext()
	defer stop()
	if err := meshmcp.New(m, store).Run(sig, &mcp.StdioTransport{}); err != nil && sig.Err() == nil {
		log.Fatal(err)
	}
}
