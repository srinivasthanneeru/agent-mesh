// Package kvstore is a tiny key-value store interface with two
// implementations: NATS JetStream KV (durable, shared by every agent on the
// fabric) and an in-memory map (for tests, or when JetStream is off).
//
// Agents use it to retain sessions across restarts: the Claude agent stores
// its Claude session id, the Ollama agent stores conversation histories.
//
// Code that needs storage depends on the Store interface, not on NATS. That
// is "dependency injection": main() decides which implementation to pass in.
package kvstore

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DefaultBucket is the JetStream KV bucket every agent-mesh agent shares.
// Inspect it with: nats kv ls agent_mesh
const DefaultBucket = "agent_mesh"

// Store saves small values (up to about 1 MB) under string keys.
// Keys may contain letters, digits and - _ = / . ("." separates levels).
type Store interface {
	// Get returns the value, or (nil, nil) when the key doesn't exist.
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	Delete(ctx context.Context, key string) error
	// Kind describes where data lives, for log messages ("jetstream:agent_mesh").
	Kind() string
}

// ---------------------------------------------------------------------------
// JetStream KV

type jsStore struct {
	kv     jetstream.KeyValue
	bucket string
}

// OpenJetStream opens (creating if needed) a KV bucket on the connected
// server. It fails if the server has JetStream turned off (nats-server -js).
func OpenJetStream(ctx context.Context, nc *nats.Conn, bucket string) (Store, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:      bucket,
		Description: "agent-mesh sessions: Claude session ids and Ollama conversations",
		History:     1, // keep only the latest value of each key
		Storage:     jetstream.FileStorage,
	})
	if err != nil {
		return nil, err
	}
	return &jsStore{kv: kv, bucket: bucket}, nil
}

func (s *jsStore) Get(ctx context.Context, key string) ([]byte, error) {
	entry, err := s.kv.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) { // missing (or deleted) is not an error for us
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

func (s *jsStore) Put(ctx context.Context, key string, value []byte) error {
	_, err := s.kv.Put(ctx, key, value) // Put returns the revision number; we don't need it
	return err
}

func (s *jsStore) Delete(ctx context.Context, key string) error {
	// Purge removes the value and its history, unlike Delete which leaves a marker.
	err := s.kv.Purge(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return nil
	}
	return err
}

func (s *jsStore) Kind() string { return "jetstream:" + s.bucket }

// ---------------------------------------------------------------------------
// In memory

type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

// NewMemory returns a Store that lives only as long as the process.
func NewMemory() Store { return &memStore{data: map[string][]byte{}} }

func (m *memStore) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[key]
	if !ok {
		return nil, nil
	}
	return append([]byte(nil), v...), nil // return a copy so callers can't change our data
}

func (m *memStore) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = append([]byte(nil), value...)
	return nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *memStore) Kind() string { return "memory (lost on restart)" }

// ---------------------------------------------------------------------------

// Open tries JetStream and falls back to memory, returning a warning to log
// when it had to fall back. Agents keep working either way.
func Open(ctx context.Context, nc *nats.Conn) (Store, string) {
	s, err := OpenJetStream(ctx, nc, DefaultBucket)
	if err == nil {
		return s, ""
	}
	return NewMemory(), "sessions will not survive restarts: JetStream KV unavailable (" + err.Error() +
		"); start nats-server with -js"
}

// Key joins parts into a KV key, replacing characters keys can't contain.
// Key("ollama", "srini", "llama3-2", "default") == "ollama.srini.llama3-2.default"
func Key(parts ...string) string {
	clean := make([]string, len(parts))
	for i, p := range parts {
		clean[i] = strings.Map(func(r rune) rune {
			switch {
			case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '=':
				return r
			}
			return '_' // "." would add a level, and spaces etc. aren't allowed
		}, p)
		if clean[i] == "" {
			clean[i] = "_"
		}
	}
	return strings.Join(clean, ".")
}
