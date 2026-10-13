// Package agent puts any piece of Go code on the NATS fabric as a protocol agent.
//
//	h, err := agent.New(agent.Options{Conn: nc, Agent: "echo", Owner: "me", Name: "main",
//		OnPrompt: func(ctx context.Context, env protocol.Envelope, res *agent.Responder) error {
//			res.Send("echo: " + env.Prompt)
//			return nil
//		}})
//	err = h.Start()
//
// A Host registers the "agents" micro service, serves `prompt` (streaming)
// and `status` (request/reply), publishes heartbeats, and lets extension
// verbs (spawn/stop/list, ...) be added with AddEndpoint.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	"agentmesh/protocol"
)

// PromptFunc answers one prompt by writing to res. Returning an error sends
// it to the caller: a *protocol.Error keeps its code, anything else is a 500.
// ctx is cancelled when the host stops, so long work should watch ctx.Done().
type PromptFunc func(ctx context.Context, env protocol.Envelope, res *Responder) error

// Options configures a Host. Zero values mean "use the default".
type Options struct {
	Conn    *nats.Conn
	Agent   string // 3rd subject token, e.g. "cc", "echo"
	AgentID string // canonical id for metadata.agent (e.g. "claude-code"); defaults to Agent
	Owner   string // 4th subject token
	Name    string // 5th subject token (instance or session name)
	Session string // metadata.session, for session-aware harnesses

	Description   string
	Version       string
	AttachmentsOK bool
	MaxPayload    int               // defaults to the server's max_payload
	Heartbeat     time.Duration     // default 30s
	KeepAlive     time.Duration     // re-send "ack" this often while a prompt runs; 0 = off
	Metadata      map[string]string // extra service metadata (e.g. role)
	OnPrompt      PromptFunc
	Logger        *log.Logger
}

// Subjects are the default endpoint subjects of one agent.
type Subjects struct{ Prompt, Status, Heartbeat string }

// Host is a running agent. Create it with New, then call Start.
type Host struct {
	opts     Options
	svc      micro.Service
	ctx      context.Context
	cancel   context.CancelFunc
	inflight sync.WaitGroup // counts prompts currently being answered
	busy     atomic.Int32   // atomics are safe to update from many goroutines
	served   atomic.Int64
	started  time.Time
	hbStop   chan struct{}
}

// New validates options and returns a Host that is not yet on the network.
func New(opts Options) (*Host, error) {
	for label, tok := range map[string]string{"agent": opts.Agent, "owner": opts.Owner, "name": opts.Name} {
		if !protocol.ValidToken(tok) {
			return nil, fmt.Errorf("%s %q is not a valid subject token ([a-z0-9_-], 1-63 chars)", label, tok)
		}
	}
	if opts.Conn == nil || opts.OnPrompt == nil {
		return nil, errors.New("Conn and OnPrompt are required")
	}
	if opts.AgentID == "" {
		opts.AgentID = opts.Agent
	}
	if opts.Heartbeat == 0 {
		opts.Heartbeat = 30 * time.Second
	}
	if opts.Version == "" {
		opts.Version = "0.1.0"
	}
	if opts.Description == "" {
		opts.Description = opts.Agent + " agent"
	}
	if opts.Logger == nil {
		opts.Logger = log.New(os.Stderr, fmt.Sprintf("[%s/%s] ", opts.Agent, opts.Name), 0)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Host{opts: opts, ctx: ctx, cancel: cancel}, nil
}

// Subjects returns this agent's default endpoint subjects.
func (h *Host) Subjects() Subjects {
	o := h.opts
	return Subjects{
		Prompt:    protocol.Subject("prompt", o.Agent, o.Owner, o.Name),
		Status:    protocol.Subject("status", o.Agent, o.Owner, o.Name),
		Heartbeat: protocol.Subject("hb", o.Agent, o.Owner, o.Name),
	}
}

// InstanceID is the micro service id; it identifies this process among
// several instances of the same agent.
func (h *Host) InstanceID() string {
	if h.svc == nil {
		return ""
	}
	return h.svc.Info().ID
}

// Context is cancelled when the host stops.
func (h *Host) Context() context.Context { return h.ctx }

// Start registers the service and endpoints and begins heartbeating.
func (h *Host) Start() error {
	o := h.opts
	md := map[string]string{}
	for k, v := range o.Metadata {
		md[k] = v
	}
	md["agent"], md["owner"], md["protocol_version"] = o.AgentID, o.Owner, protocol.Version
	if o.Session != "" {
		md["session"] = o.Session
	}

	svc, err := micro.AddService(o.Conn, micro.Config{
		Name:        protocol.ServiceName,
		Version:     o.Version,
		Description: o.Description,
		Metadata:    md,
	})
	if err != nil {
		return fmt.Errorf("register service: %w", err) // %w wraps err so callers can inspect it
	}
	h.svc = svc
	h.started = time.Now()

	maxPayload := o.MaxPayload
	if maxPayload == 0 {
		maxPayload = int(o.Conn.MaxPayload())
	}
	subj := h.Subjects()

	err = svc.AddEndpoint("prompt",
		micro.HandlerFunc(func(req micro.Request) {
			// NATS calls handlers one at a time per subscription. Answering can
			// take minutes, so hand it to its own goroutine and return at once.
			h.inflight.Add(1)
			go func() {
				defer h.inflight.Done()
				h.handlePrompt(req, maxPayload)
			}()
		}),
		micro.WithEndpointSubject(subj.Prompt),
		micro.WithEndpointQueueGroup(protocol.QueueGroup), // §3.3: explicit, never the default
		micro.WithEndpointMetadata(map[string]string{
			"max_payload":    protocol.FormatSize(maxPayload),
			"attachments_ok": fmt.Sprint(o.AttachmentsOK),
		}),
	)
	if err != nil {
		return err
	}

	err = svc.AddEndpoint("status",
		micro.HandlerFunc(func(req micro.Request) {
			b, err := json.Marshal(h.Heartbeat())
			if err != nil {
				_ = req.Error("500", err.Error(), nil)
				return
			}
			_ = req.Respond(b)
		}),
		micro.WithEndpointSubject(subj.Status),
		micro.WithEndpointQueueGroup(protocol.QueueGroup),
	)
	if err != nil {
		return err
	}

	// Heartbeats start only after registration, so discovery finds our metadata (§8.2).
	h.hbStop = make(chan struct{})
	go h.heartbeatLoop(subj.Heartbeat, h.hbStop) // pass the channel: the goroutine never reads h.hbStop
	o.Logger.Printf("listening on %s (instance %s)", subj.Prompt, h.InstanceID())
	return nil
}

// heartbeatLoop publishes a heartbeat now and on every tick until Stop.
// `select` waits on several channels at once and runs whichever is ready.
func (h *Host) heartbeatLoop(subject string, stop <-chan struct{}) {
	ticker := time.NewTicker(h.opts.Heartbeat)
	defer ticker.Stop()
	for {
		if b, err := json.Marshal(h.Heartbeat()); err == nil {
			_ = h.opts.Conn.Publish(subject, b)
		}
		select {
		case <-ticker.C:
		case <-stop:
			return
		}
	}
}

// Heartbeat builds the §8.3 payload; the status endpoint reuses it.
func (h *Host) Heartbeat() protocol.Heartbeat {
	return protocol.Heartbeat{
		Agent:      h.opts.AgentID,
		Owner:      h.opts.Owner,
		Session:    h.opts.Session,
		InstanceID: h.InstanceID(),
		TS:         time.Now().UTC().Format(time.RFC3339),
		IntervalS:  h.opts.Heartbeat.Seconds(),
		Busy:       int(h.busy.Load()),
		Served:     h.served.Load(),
		UptimeS:    int64(time.Since(h.started).Seconds()),
	}
}

// ExtensionFunc handles an extension verb; its result is sent back as JSON.
type ExtensionFunc func(req micro.Request) (any, error)

// AddEndpoint adds an extension verb, e.g. AddEndpoint("spawn", fn) serves
// agents.spawn.{agent}.{owner}.{name}. Plain request/reply.
func (h *Host) AddEndpoint(verb string, fn ExtensionFunc) (string, error) {
	if h.svc == nil {
		return "", errors.New("call Start first")
	}
	subject := protocol.Subject(verb, h.opts.Agent, h.opts.Owner, h.opts.Name)
	err := h.svc.AddEndpoint(verb,
		micro.HandlerFunc(func(req micro.Request) {
			out, err := fn(req)
			if err != nil {
				pe := asProtocolError(err)
				var body []byte
				if pe.Body != nil {
					body, _ = json.Marshal(pe.Body)
				}
				_ = req.Error(fmt.Sprint(pe.Code), pe.Message, body)
				return
			}
			if out == nil {
				out = map[string]bool{"ok": true}
			}
			_ = req.RespondJSON(out)
		}),
		micro.WithEndpointSubject(subject),
	)
	return subject, err
}

func (h *Host) handlePrompt(req micro.Request, maxPayload int) {
	if req.Reply() == "" {
		return // nobody is listening for an answer
	}
	res := &Responder{nc: h.opts.Conn, reply: req.Reply()}
	res.Status(protocol.StatusAck) // mandatory first chunk, before any slow work (§6.4)
	h.served.Add(1)
	h.busy.Add(1)
	defer h.busy.Add(-1)

	// A keep-alive goroutine re-sends "ack" so the caller's inactivity timer
	// doesn't fire while we think. Closing `done` tells it to stop.
	done := make(chan struct{})
	defer close(done)
	if h.opts.KeepAlive > 0 {
		go func() {
			t := time.NewTicker(h.opts.KeepAlive)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					res.Status(protocol.StatusAck)
				case <-done:
					return
				}
			}
		}()
	}

	err := func() error {
		if len(req.Data()) > maxPayload {
			return protocol.Errorf(400, "request exceeds max_payload")
		}
		env, err := protocol.DecodeEnvelope(req.Data(), h.opts.AttachmentsOK)
		if err != nil {
			return err
		}
		return h.opts.OnPrompt(h.ctx, env, res)
	}()

	if err != nil {
		pe := asProtocolError(err)
		if pe.Code == 500 {
			h.opts.Logger.Printf("prompt failed: %s", pe.Message)
		}
		res.Error(pe.Code, pe.Message, pe.Body)
		return
	}
	res.End()
}

// asProtocolError keeps a *protocol.Error as is and turns anything else into a 500.
// errors.As looks through wrapped errors (fmt.Errorf("...: %w", err)) too.
func asProtocolError(err error) *protocol.Error {
	var pe *protocol.Error
	if errors.As(err, &pe) {
		return pe
	}
	return &protocol.Error{Code: 500, Message: err.Error()}
}

// Stop cancels running prompts, waits for them, and leaves the network.
func (h *Host) Stop() error {
	h.cancel()
	if h.hbStop != nil {
		close(h.hbStop)
		h.hbStop = nil
	}
	h.inflight.Wait()
	if h.svc == nil {
		return nil
	}
	return h.svc.Stop()
}

// ---------------------------------------------------------------------------

// Responder writes one prompt's response stream. Several goroutines may use
// it at once (the handler and the keep-alive), so a mutex guards `ended`.
type Responder struct {
	nc    *nats.Conn
	reply string
	mu    sync.Mutex
	ended bool
}

// NewResponder is exported for tests and adapters that stream to a known inbox.
func NewResponder(nc *nats.Conn, reply string) *Responder {
	return &Responder{nc: nc, reply: reply}
}

// publish sends one message unless the stream has already ended.
func (r *Responder) publish(data []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended {
		return
	}
	_ = r.nc.Publish(r.reply, data)
}

// Send emits a "response" chunk. Callers concatenate these in order.
func (r *Responder) Send(text string, attachments ...protocol.Attachment) {
	if text == "" && len(attachments) == 0 {
		return
	}
	if len(attachments) > 0 {
		r.publish(protocol.EncodeChunk(protocol.ChunkResponse, map[string]any{"text": text, "attachments": attachments}))
		return
	}
	r.publish(protocol.EncodeChunk(protocol.ChunkResponse, text))
}

// Status emits a "status" chunk. Only "ack" is standard.
func (r *Responder) Status(token string) {
	r.publish(protocol.EncodeChunk(protocol.ChunkStatus, token))
}

// Raw emits a chunk of any type; callers that don't know the type skip it (§6.6).
func (r *Responder) Raw(typ string, data any) {
	r.publish(protocol.EncodeChunk(typ, data))
}

// Ask sends a mid-stream query (§7) and waits for exactly one reply on a
// fresh inbox. ok is false on timeout or cancellation, which is also the
// only way an agent learns that its caller went away (§6.7).
func (r *Responder) Ask(ctx context.Context, prompt string, timeout time.Duration) (answer string, ok bool) {
	inbox := nats.NewInbox()
	sub, err := r.nc.SubscribeSync(inbox)
	if err != nil {
		return "", false
	}
	defer sub.Unsubscribe()
	id, _ := newID()
	r.Raw(protocol.ChunkQuery, protocol.QueryData{ID: id, ReplySubject: inbox, Prompt: prompt})

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	msg, err := sub.NextMsgWithContext(ctx)
	if err != nil {
		return "", false
	}
	// The reply follows the §5.1 envelope rules: plain text or {"prompt": ...}.
	env, err := protocol.DecodeEnvelope(msg.Data, true)
	if err != nil {
		return string(msg.Data), true
	}
	return env.Prompt, true
}

// Error sends an error-headered message followed by the terminator (§9.3).
func (r *Responder) Error(code int, message string, body map[string]any) {
	msg := nats.NewMsg(r.reply)
	msg.Header.Set(protocol.ErrCodeHeader, fmt.Sprint(code))
	msg.Header.Set(protocol.ErrDescHeader, message)
	if body != nil {
		msg.Data, _ = json.Marshal(body)
	}
	r.mu.Lock()
	if !r.ended {
		_ = r.nc.PublishMsg(msg)
	}
	r.mu.Unlock()
	r.End()
}

// End sends the zero-byte, header-less terminator (§6.5). Safe to call twice.
func (r *Responder) End() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended {
		return
	}
	r.ended = true
	_ = r.nc.Publish(r.reply, nil)
}

// Done reports whether End has been called.
func (r *Responder) Done() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ended
}
