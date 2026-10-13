package main

import "encoding/json"

// Small wrappers so render code stays readable.
func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
