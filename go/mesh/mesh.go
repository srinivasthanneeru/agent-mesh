// Package mesh is the caller side: discover agents, track liveness, prompt
// one agent, or fan a prompt out to many and merge their streams.
//
//	m := mesh.New(nc)
//	m.Watch()                                  // heartbeats first (§8.5)
//	agents, _ := m.Discover(750 * time.Millisecond)
//	for ev := range m.Prompt(ctx, agents[0], "hi", mesh.PromptOptions{}) { ... }
//	for ev := range m.Fanout(ctx, agents, "hi", mesh.PromptOptions{}) { ... }
//
// Prompt and Fanout return iterators (iter.Seq), so callers simply range over
// them. Breaking out of the loop cancels the stream and unsubscribes.
package mesh

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"agentmesh/protocol"
)

// Endpoint and Agent mirror the JSON that $SRV.INFO.agents returns.
type Endpoint struct {
	Name       string            `json:"name"`
	Subject    string            `json:"subject"`
	QueueGroup string            `json:"queue_group"`
	Metadata   map[string]string `json:"metadata"`
}

// Agent is one discovered instance.
type Agent struct {
	InstanceID      string
	AgentID         string // metadata.agent, e.g. "claude-code"
	Token           string // 3rd subject token, e.g. "cc"
	Owner           string
	Name            string // 5th subject token
	Session         string
	ProtocolVersion string
	Version         string
	Description     string
	Metadata        map[string]string
	Endpoints       map[string]Endpoint
}

// Label is the human-readable "token/owner/name".
func (a Agent) Label() string { return a.Token + "/" + a.Owner + "/" + a.Name }

// PromptSubject is where prompts go; always taken from discovery (§4.3).
func (a Agent) PromptSubject() string { return a.Endpoints["prompt"].Subject }

// EventType says what an Event carries. Defining a named string type and
// constants is Go's usual way to write an enum.
type EventType string

const (
	EvAck     EventType = "ack"
	EvText    EventType = "text"
	EvStatus  EventType = "status"
	EvQuery   EventType = "query"
	EvError   EventType = "error"
	EvDone    EventType = "done"
	EvTimeout EventType = "timeout"
	EvUnknown EventType = "unknown" // a chunk type we don't know; forward-compat (§6.6)
)

// Event is one thing that happened on a stream. Only the fields relevant to
// Type are set.
type Event struct {
	Type        EventType
	Text        string                // EvText
	Attachments []protocol.Attachment // EvText
	Status      string                // EvStatus
	Query       *protocol.QueryData   // EvQuery
	Code        int                   // EvError
	Message     string                // EvError
	Body        json.RawMessage       // EvError (optional JSON detail)
	Chunk       *protocol.Chunk       // EvUnknown
	From        *Agent                // set by Fanout

	answer func(string)
}

// Answer replies to a query event (§7.2). Calling it twice sends once.
func (e Event) Answer(reply string) {
	if e.answer != nil {
		e.answer(reply)
	}
}

// PromptOptions are optional request settings.
type PromptOptions struct {
	Attachments []protocol.Attachment
	Extra       map[string]any // extra envelope fields, passed through untouched
	Inactivity  time.Duration  // gap allowed between chunks (§6.6); default 60s
}

type seen struct {
	hb protocol.Heartbeat
	at time.Time
}

// Mesh is a caller bound to one NATS connection.
type Mesh struct {
	nc        *nats.Conn
	mu        sync.Mutex // guards lastSeen and listeners (touched by the heartbeat goroutine)
	lastSeen  map[string]seen
	listeners []func(protocol.Heartbeat, string)
	hbSub     *nats.Subscription
}

// New creates a Mesh. Call Watch before Discover.
func New(nc *nats.Conn) *Mesh {
	return &Mesh{nc: nc, lastSeen: map[string]seen{}}
}

// Conn exposes the underlying connection.
func (m *Mesh) Conn() *nats.Conn { return m.nc }

// Watch subscribes to every heartbeat. NATS runs the callback on its own
// goroutine, which is why the shared map is protected by m.mu.
func (m *Mesh) Watch() error {
	if m.hbSub != nil {
		return nil
	}
	sub, err := m.nc.Subscribe(protocol.HeartbeatAll, func(msg *nats.Msg) {
		var hb protocol.Heartbeat
		if json.Unmarshal(msg.Data, &hb) != nil || hb.InstanceID == "" {
			return
		}
		m.mu.Lock()
		m.lastSeen[hb.InstanceID] = seen{hb: hb, at: time.Now()}
		ls := append([]func(protocol.Heartbeat, string){}, m.listeners...) // copy, then call without the lock
		m.mu.Unlock()
		for _, l := range ls {
			l(hb, msg.Subject)
		}
	})
	if err != nil {
		return err
	}
	m.hbSub = sub
	return m.nc.Flush() // make sure the server has our subscription before we go on
}

// OnHeartbeat registers a callback for every heartbeat received.
func (m *Mesh) OnHeartbeat(fn func(hb protocol.Heartbeat, subject string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = append(m.listeners, fn)
}

// Liveness is "online" if a heartbeat arrived within 3 × interval (§8.2).
func (m *Mesh) Liveness(instanceID string) string {
	m.mu.Lock()
	s, ok := m.lastSeen[instanceID]
	m.mu.Unlock()
	switch {
	case !ok:
		return "unknown"
	case time.Since(s.at) <= time.Duration(3*s.hb.IntervalS*float64(time.Second)):
		return "online"
	default:
		return "offline"
	}
}

// Discover asks $SRV.INFO.agents and collects every answer that arrives
// within wait (scatter-gather: one request, many replies).
func (m *Mesh) Discover(wait time.Duration) ([]Agent, error) {
	inbox := nats.NewInbox()
	sub, err := m.nc.SubscribeSync(inbox)
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	if err := m.nc.PublishRequest("$SRV.INFO."+protocol.ServiceName, inbox, nil); err != nil {
		return nil, err
	}

	var out []Agent
	deadline := time.Now().Add(wait)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			break
		}
		msg, err := sub.NextMsg(left)
		if err != nil {
			break // timeout: nobody else is answering
		}
		if a, ok := parseInfo(msg.Data); ok {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label() < out[j].Label() })
	return out, nil
}

func parseInfo(data []byte) (Agent, bool) {
	var info struct {
		ID          string            `json:"id"`
		Version     string            `json:"version"`
		Description string            `json:"description"`
		Metadata    map[string]string `json:"metadata"`
		Endpoints   []Endpoint        `json:"endpoints"`
	}
	if json.Unmarshal(data, &info) != nil {
		return Agent{}, false
	}
	a := Agent{
		InstanceID:      info.ID,
		Version:         info.Version,
		Description:     info.Description,
		Metadata:        info.Metadata,
		Endpoints:       map[string]Endpoint{},
		AgentID:         info.Metadata["agent"],
		Owner:           info.Metadata["owner"],
		Session:         info.Metadata["session"],
		ProtocolVersion: info.Metadata["protocol_version"],
	}
	if a.Metadata == nil {
		a.Metadata = map[string]string{}
	}
	for _, e := range info.Endpoints {
		a.Endpoints[e.Name] = e
	}
	prompt, ok := a.Endpoints["prompt"]
	if !ok {
		return Agent{}, false // not a protocol agent
	}
	if p, ok := protocol.ParseSubject(prompt.Subject); ok {
		a.Token, a.Name = p.Agent, p.Name
		if a.Owner == "" {
			a.Owner = p.Owner
		}
	} else {
		a.Token, a.Name = a.AgentID, a.Session
	}
	return a, true
}

// Status is the one-shot §8.7 status request; it also feeds Liveness.
func (m *Mesh) Status(a Agent, timeout time.Duration) (protocol.Heartbeat, error) {
	var hb protocol.Heartbeat
	ep, ok := a.Endpoints["status"]
	if !ok {
		return hb, fmt.Errorf("%s has no status endpoint", a.Label())
	}
	msg, err := m.nc.Request(ep.Subject, nil, timeout)
	if err != nil {
		return hb, err
	}
	if err := json.Unmarshal(msg.Data, &hb); err != nil {
		return hb, err
	}
	m.mu.Lock()
	m.lastSeen[hb.InstanceID] = seen{hb: hb, at: time.Now()}
	m.mu.Unlock()
	return hb, nil
}

// Call does request/reply on an extension verb and decodes the JSON answer into out.
func (m *Mesh) Call(a Agent, verb string, body any, out any, timeout time.Duration) error {
	ep, ok := a.Endpoints[verb]
	if !ok {
		return fmt.Errorf("%s has no %q endpoint", a.Label(), verb)
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	msg, err := m.nc.Request(ep.Subject, data, timeout)
	if err != nil {
		return err
	}
	if code := msg.Header.Get(protocol.ErrCodeHeader); code != "" {
		return fmt.Errorf("%s failed (%s): %s", verb, code, msg.Header.Get(protocol.ErrDescHeader))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(msg.Data, out)
}

// IsCompatible applies §11.2: same major, and in the 0.x line the same minor.
func IsCompatible(v string) bool {
	maj, min, _ := strings.Cut(v, ".")
	ourMaj, ourMin, _ := strings.Cut(protocol.Version, ".")
	return maj == ourMaj && (maj != "0" || min == ourMin)
}

// Prompt sends text to one agent and yields its reply as events.
//
// It returns an iterator: a function that takes a `yield` callback. Each
// yield hands one event to the caller's `for ev := range ...` loop; if the
// loop breaks, yield returns false and we stop (and the deferred Unsubscribe
// cancels the stream, §6.7).
func (m *Mesh) Prompt(ctx context.Context, a Agent, text string, opts PromptOptions) iter.Seq[Event] {
	return func(yield func(Event) bool) {
		fail := func(code int, msg string) { yield(Event{Type: EvError, Code: code, Message: msg}) }
		if !IsCompatible(a.ProtocolVersion) {
			fail(400, "incompatible protocol_version "+a.ProtocolVersion)
			return
		}
		ep := a.Endpoints["prompt"]

		// Client-side validation against endpoint metadata (§5.4): fail before publishing.
		if len(opts.Attachments) > 0 && ep.Metadata["attachments_ok"] != "true" {
			fail(400, "agent does not accept attachments")
			return
		}
		payload, err := protocol.EncodeEnvelope(text, opts.Attachments, opts.Extra)
		if err != nil {
			fail(400, err.Error())
			return
		}
		if limit, err := protocol.ParseSize(ep.Metadata["max_payload"]); err == nil && len(payload) > limit {
			fail(400, fmt.Sprintf("payload %dB exceeds max_payload %s", len(payload), ep.Metadata["max_payload"]))
			return
		}

		inactivity := opts.Inactivity
		if inactivity == 0 {
			inactivity = 60 * time.Second
		}
		inbox := nats.NewInbox()
		sub, err := m.nc.SubscribeSync(inbox)
		if err != nil {
			fail(500, err.Error())
			return
		}
		defer sub.Unsubscribe()
		if err := m.nc.PublishRequest(ep.Subject, inbox, payload); err != nil {
			fail(500, err.Error())
			return
		}

		for {
			waitCtx, cancel := context.WithTimeout(ctx, inactivity)
			msg, err := sub.NextMsgWithContext(waitCtx)
			cancel()
			if err != nil {
				if ctx.Err() == nil { // our caller didn't cancel, so the agent went quiet
					yield(Event{Type: EvTimeout})
				}
				return
			}
			ev, last := m.decode(msg)
			if ev.Type == "" {
				continue // unparseable chunk
			}
			if !yield(ev) || last {
				return
			}
		}
	}
}

// decode turns one NATS message into an Event. last reports end-of-stream.
func (m *Mesh) decode(msg *nats.Msg) (ev Event, last bool) {
	// Headers are checked before the body on every message (§12).
	if code := msg.Header.Get(protocol.ErrCodeHeader); code != "" {
		ev = Event{Type: EvError, Message: msg.Header.Get(protocol.ErrDescHeader), Body: msg.Data}
		fmt.Sscan(code, &ev.Code)
		var body struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(msg.Data, &body) == nil && body.Message != "" {
			ev.Message = body.Message
		}
		return ev, false // the terminator follows
	}
	if len(msg.Data) == 0 && len(msg.Header) == 0 {
		return Event{Type: EvDone}, true
	}

	var c protocol.Chunk
	if json.Unmarshal(msg.Data, &c) != nil {
		return Event{}, false
	}
	switch c.Type {
	case protocol.ChunkStatus:
		var s string
		_ = json.Unmarshal(c.Data, &s)
		if s == protocol.StatusAck {
			return Event{Type: EvAck}, false
		}
		return Event{Type: EvStatus, Status: s}, false
	case protocol.ChunkResponse:
		text, atts := c.Text()
		return Event{Type: EvText, Text: text, Attachments: atts}, false
	case protocol.ChunkQuery:
		var q protocol.QueryData
		if json.Unmarshal(c.Data, &q) != nil {
			return Event{}, false
		}
		var once sync.Once
		return Event{Type: EvQuery, Query: &q, answer: func(reply string) {
			once.Do(func() { _ = m.nc.Publish(q.ReplySubject, []byte(reply)) })
		}}, false
	default:
		return Event{Type: EvUnknown, Chunk: &c}, false
	}
}

// Fanout sends the same prompt to several agents at once and merges their
// streams. Every event has From set to the agent it came from.
//
// Inside, each agent gets its own goroutine; they all send into one channel
// and this function yields from that channel. That's the classic Go
// "fan-in" pattern.
func (m *Mesh) Fanout(ctx context.Context, agents []Agent, text string, opts PromptOptions) iter.Seq[Event] {
	return func(yield func(Event) bool) {
		agents = UniqueBySubject(agents)
		ctx, cancel := context.WithCancel(ctx)
		defer cancel() // stops every goroutine below if the caller breaks early

		events := make(chan Event)
		var wg sync.WaitGroup
		for _, a := range agents {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ev := range m.Prompt(ctx, a, text, opts) {
					ev.From = &a
					select {
					case events <- ev:
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		go func() {
			wg.Wait()
			close(events) // ends the range loop below once every agent is done
		}()

		for ev := range events {
			if !yield(ev) {
				return
			}
		}
	}
}

// UniqueBySubject keeps one instance per prompt subject. Instances sharing a
// subject are one logical agent: the "agents" queue group load-balances
// between them (§3.4), so each subject should be addressed once.
func UniqueBySubject(agents []Agent) []Agent {
	seen := map[string]bool{}
	var out []Agent
	for _, a := range agents {
		if s := a.PromptSubject(); !seen[s] {
			seen[s] = true
			out = append(out, a)
		}
	}
	return out
}

// MatchTargets resolves "cc", "cc,echo", "@shell box1", "cc/srini" or "all".
// A part matches an agent's token, name, canonical id, or a label prefix.
// "all" selects every worker: agents whose role is controller or
// orchestrator are only matched when named, so meta-agents can't loop.
func MatchTargets(agents []Agent, target string) []Agent {
	parts := strings.FieldsFunc(target, func(r rune) bool { return r == ',' || r == ' ' })
	for i := range parts {
		parts[i] = strings.TrimPrefix(parts[i], "@")
	}
	isWorker := func(a Agent) bool {
		role := a.Metadata["role"]
		return role != "controller" && role != "orchestrator"
	}
	hit := func(a Agent, p string) bool {
		if p == "" {
			return false
		}
		prefix := p
		if !strings.HasSuffix(prefix, "/") {
			prefix += "/"
		}
		return a.Token == p || a.Name == p || a.AgentID == p || strings.HasPrefix(a.Label()+"/", prefix)
	}

	var out []Agent
	for _, a := range agents {
		for _, p := range parts {
			if ((p == "all" || p == "*") && isWorker(a)) || hit(a, p) {
				out = append(out, a)
				break
			}
		}
	}
	return UniqueBySubject(out)
}

// ErrNoAgents is a sentinel error: callers compare with errors.Is.
var ErrNoAgents = errors.New("no matching agents")
