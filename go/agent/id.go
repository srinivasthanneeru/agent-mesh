package agent

import (
	"crypto/rand"
	"fmt"
)

// newID returns a random UUID (version 4) such as 1b4e28ba-2fa1-41d2-883f-0016d3cca427.
func newID() (string, error) {
	var b [16]byte // an array: fixed size, lives on the stack
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// NewID is exported for other packages (e.g. the controller's session ids).
func NewID() string {
	id, _ := newID()
	return id
}
