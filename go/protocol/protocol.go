// Package protocol holds the wire contract shared by agents and callers:
// subject names, the request envelope, response chunks, heartbeats and
// error codes of the Synadia Agent Protocol for NATS v0.3.
//
// It has no networking code, which keeps it easy to test (see protocol_test.go).
//
//	agents.{verb}.{agent}.{owner}.{name}      verb ∈ prompt | hb | status | <extension>
//
// A prompt's reply is a stream of JSON chunks on the caller's inbox:
//
//	{"type":"status","data":"ack"}      always first
//	{"type":"response","data":"..."}    content
//	{"type":"query","data":{...}}       the agent asks the caller something
//	<zero bytes, no headers>            end of stream
package protocol

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Constants are grouped in one const block. Untyped string constants like
// these can be used anywhere a string is expected.
const (
	ServiceName     = "agents"
	QueueGroup      = "agents"
	Version         = "0.3"
	HeartbeatAll    = "agents.hb.*.*.*"
	ErrCodeHeader   = "Nats-Service-Error-Code"
	ErrDescHeader   = "Nats-Service-Error"
	StatusAck       = "ack"
	ChunkResponse   = "response"
	ChunkStatus     = "status"
	ChunkQuery      = "query"
	defaultMaxBytes = 1 << 20 // 1 MiB: `<<` is a bit shift, 1<<20 == 1048576
)

// Subject builds a verb-first subject such as agents.prompt.cc.srini.main.
func Subject(verb, agent, owner, name string) string {
	return "agents." + verb + "." + agent + "." + owner + "." + name
}

// SubjectParts is what ParseSubject returns. Exported (capitalised) fields are
// visible to other packages; lower-case ones would be private.
type SubjectParts struct {
	Verb, Agent, Owner, Name string
}

// ParseSubject splits a verb-first subject back into its tokens. The second
// return value reports whether s had the right shape ("comma ok" idiom).
func ParseSubject(s string) (SubjectParts, bool) {
	t := strings.Split(s, ".")
	if len(t) != 5 || t[0] != "agents" {
		return SubjectParts{}, false
	}
	return SubjectParts{Verb: t[1], Agent: t[2], Owner: t[3], Name: t[4]}, true
}

// Package-level variables are initialised once, when the program starts.
var (
	tokenRE    = regexp.MustCompile(`^[a-z0-9_-]{1,63}$`)
	notTokenRE = regexp.MustCompile(`[^a-z0-9_-]+`)
	dashesRE   = regexp.MustCompile(`-{2,}`)
)

// ValidToken reports whether s may be used as one subject token.
func ValidToken(s string) bool { return tokenRE.MatchString(s) }

// SanitizeToken turns arbitrary text ("My Project.v2") into a token ("my-project-v2").
func SanitizeToken(s string) string {
	s = notTokenRE.ReplaceAllString(strings.ToLower(s), "-")
	s = strings.Trim(dashesRE.ReplaceAllString(s, "-"), "-")
	if len(s) > 63 {
		s = s[:63]
	}
	return s
}

// ---------------------------------------------------------------------------
// Errors

// Error is a protocol error: it becomes Nats-Service-Error-Code / -Error
// headers on the wire. Because it has an Error() string method it satisfies
// Go's built-in `error` interface, so it can be returned anywhere an error is.
type Error struct {
	Code    int            // 400, 401, 403, 404, 409, 429 or 500
	Message string         // short description
	Body    map[string]any // optional JSON body with more detail
}

func (e *Error) Error() string { return fmt.Sprintf("%d: %s", e.Code, e.Message) }

// Errorf is a convenience constructor, like fmt.Errorf but with a status code.
func Errorf(code int, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// ---------------------------------------------------------------------------
// Request envelope (§5)

// Attachment is a file carried inside a request or response. The `json:"..."`
// struct tags tell encoding/json which key names to use.
type Attachment struct {
	Filename string `json:"filename"`
	Content  string `json:"content"` // standard, padded base64
}

// Envelope is a decoded request. Extra keeps any fields we don't know about,
// because the protocol says unknown fields must be tolerated and preserved.
type Envelope struct {
	Prompt      string
	Attachments []Attachment
	Extra       map[string]json.RawMessage
}

// DecodeEnvelope applies the discrimination rule (§5.3): skip leading
// whitespace; if the next byte is '{' the payload is a JSON envelope,
// otherwise the whole payload is the prompt text.
func DecodeEnvelope(payload []byte, attachmentsOK bool) (Envelope, error) {
	if len(payload) == 0 {
		return Envelope{}, Errorf(400, "empty payload")
	}
	trimmed := bytes.TrimLeft(payload, "\t\n\r ")
	if len(trimmed) == 0 || trimmed[0] != '{' {
		if strings.TrimSpace(string(payload)) == "" {
			return Envelope{}, Errorf(400, "prompt missing or empty")
		}
		return Envelope{Prompt: string(payload)}, nil
	}

	// Decode into a generic map first so unknown keys survive.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &raw); err != nil {
		return Envelope{}, Errorf(400, "invalid JSON envelope")
	}
	var env Envelope
	if err := json.Unmarshal(raw["prompt"], &env.Prompt); err != nil || strings.TrimSpace(env.Prompt) == "" {
		return Envelope{}, Errorf(400, "prompt missing or empty")
	}
	if a, ok := raw["attachments"]; ok {
		if err := json.Unmarshal(a, &env.Attachments); err != nil {
			return Envelope{}, Errorf(400, "attachments must be an array of {filename, content}")
		}
	}
	delete(raw, "prompt")
	delete(raw, "attachments")
	env.Extra = raw

	if len(env.Attachments) > 0 && !attachmentsOK {
		return Envelope{}, Errorf(400, "attachments not accepted by this agent")
	}
	for i, a := range env.Attachments {
		if a.Filename == "" || a.Filename == ".." || strings.ContainsAny(a.Filename, `/\`) {
			return Envelope{}, Errorf(400, "attachment[%d] has unsafe filename", i)
		}
		// StdEncoding is the standard, padded alphabet; it rejects URL-safe and unpadded input.
		if _, err := base64.StdEncoding.Strict().DecodeString(a.Content); err != nil {
			return Envelope{}, Errorf(400, "attachment[%d] has invalid base64 content", i)
		}
	}
	return env, nil
}

// EncodeEnvelope is the caller-side inverse: plain text when possible, JSON otherwise.
func EncodeEnvelope(prompt string, attachments []Attachment, extra map[string]any) ([]byte, error) {
	if len(attachments) == 0 && len(extra) == 0 {
		return []byte(prompt), nil
	}
	m := map[string]any{}
	for k, v := range extra {
		m[k] = v
	}
	m["prompt"] = prompt
	if len(attachments) > 0 {
		m["attachments"] = attachments
	}
	return json.Marshal(m)
}

// ---------------------------------------------------------------------------
// Response chunks (§6)

// Chunk is one streamed message. Data stays raw JSON until we know the type.
type Chunk struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// QueryData is the payload of a "query" chunk (§7).
type QueryData struct {
	ID           string       `json:"id"`
	ReplySubject string       `json:"reply_subject"`
	Prompt       string       `json:"prompt"`
	Attachments  []Attachment `json:"attachments,omitempty"`
}

// EncodeChunk marshals {"type": typ, "data": data}.
func EncodeChunk(typ string, data any) []byte {
	// json.Marshal only fails for values like channels or functions, which we never pass.
	b, _ := json.Marshal(map[string]any{"type": typ, "data": data})
	return b
}

// Text extracts the text of a "response" chunk, whose data is either a
// string or {"text": "...", "attachments": [...]}.
func (c Chunk) Text() (text string, attachments []Attachment) {
	if err := json.Unmarshal(c.Data, &text); err == nil {
		return text, nil
	}
	var obj struct {
		Text        string       `json:"text"`
		Attachments []Attachment `json:"attachments"`
	}
	_ = json.Unmarshal(c.Data, &obj)
	return obj.Text, obj.Attachments
}

// ---------------------------------------------------------------------------
// Heartbeat / status payload (§8.3)

// Heartbeat is published periodically and returned by the status endpoint.
// `omitempty` leaves a field out of the JSON when it has its zero value.
type Heartbeat struct {
	Agent      string  `json:"agent"`
	Owner      string  `json:"owner"`
	Session    string  `json:"session,omitempty"`
	InstanceID string  `json:"instance_id"`
	TS         string  `json:"ts"`
	IntervalS  float64 `json:"interval_s"`
	// Extra fields are allowed by the spec; callers that don't know them ignore them.
	Busy    int   `json:"busy"`
	Served  int64 `json:"served"`
	UptimeS int64 `json:"uptime_s"`
}

// ---------------------------------------------------------------------------
// Sizes

// ParseSize reads "512KB" / "1MB" style strings from endpoint metadata (§2.1).
func ParseSize(s string) (int, error) {
	s = strings.TrimSpace(s)
	units := []struct {
		suffix string
		mult   int
	}{{"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1}}
	for _, u := range units {
		if num, ok := strings.CutSuffix(s, u.suffix); ok {
			n, err := strconv.Atoi(num)
			if err != nil || n < 0 {
				return 0, fmt.Errorf("bad size %q", s)
			}
			return n * u.mult, nil
		}
	}
	return 0, fmt.Errorf("bad size %q", s)
}

// FormatSize is the inverse of ParseSize, using the largest exact unit.
func FormatSize(n int) string {
	switch {
	case n > 0 && n%(1<<30) == 0:
		return strconv.Itoa(n>>30) + "GB"
	case n > 0 && n%(1<<20) == 0:
		return strconv.Itoa(n>>20) + "MB"
	case n > 0 && n%(1<<10) == 0:
		return strconv.Itoa(n>>10) + "KB"
	case n > 0:
		return strconv.Itoa(n) + "B"
	}
	return FormatSize(defaultMaxBytes)
}
