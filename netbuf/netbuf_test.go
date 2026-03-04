package netbuf

import (
	"bytes"
	"crypto/tls"
	"reflect"
	"testing"
)

// TestRawInputFieldExists guards against Go runtime changes that rename or
// remove tls.Conn.rawInput, which would cause GrowTLSReadBuf to panic.
func TestRawInputFieldExists(t *testing.T) {
	f := reflect.Indirect(reflect.ValueOf(&tls.Conn{})).FieldByName("rawInput")
	if !f.IsValid() {
		t.Fatal("tls.Conn.rawInput field not found: GrowTLSReadBuf will panic")
	}
	if f.Type() != reflect.TypeOf(bytes.Buffer{}) {
		t.Fatalf("tls.Conn.rawInput has type %v, want bytes.Buffer", f.Type())
	}
}
