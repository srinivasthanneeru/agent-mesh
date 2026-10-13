package claude

import "agentmesh/agent"

// Compile-time proof that *agent.Responder satisfies Sink: if it ever stops
// matching, this line fails to build. (The blank identifier _ discards the value.)
var _ Sink = (*agent.Responder)(nil)
