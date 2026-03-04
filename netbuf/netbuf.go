// Package netbuf works around golang/go#47672: tls.Conn.rawInput starts with
// zero capacity and grows one TLS record (~16 KiB) at a time, so even a
// transport configured with a large ReadBufferSize still issues many small
// read(2) syscalls.  Pre-sizing rawInput before the handshake lets the kernel
// fill a large buffer in one shot, reducing syscall overhead significantly on
// high-throughput connections.
//
// Usage — wire into http.Transport at construction time:
//
//	transport := &http.Transport{
//	    ReadBufferSize:  netbuf.TLSBufSize,
//	    WriteBufferSize: netbuf.TLSBufSize,
//	    DialTLSContext:  netbuf.DialTLSContext,
//	}
//
// Remove this package once golang/go#47672 is resolved.
package netbuf

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"reflect"
	"unsafe"
)

// TLSBufSize is the pre-allocated capacity of tls.Conn.rawInput.
// 256 KiB lets the TLS layer batch ~16 records per read(2) syscall.
const TLSBufSize = 256 << 10

// DialTLSContext is a drop-in for http.Transport.DialTLSContext.
// It dials, pre-sizes rawInput, then completes the TLS handshake.
func DialTLSContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	tcpConn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(tcpConn, &tls.Config{ServerName: host})
	GrowTLSReadBuf(tlsConn)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		tcpConn.Close()
		return nil, err
	}
	return tlsConn, nil
}

// GrowTLSReadBuf pre-allocates tls.Conn.rawInput to TLSBufSize bytes.
// Call before Handshake for the buffer to be in effect from the first read.
//
// This reaches into an unexported field by name via reflect/unsafe.
// The companion test verifies the field exists with the correct type so
// any Go upgrade that removes or renames it fails at test time, not runtime.
func GrowTLSReadBuf(c *tls.Conn) {
	f := reflect.Indirect(reflect.ValueOf(c)).FieldByName("rawInput")
	*(*bytes.Buffer)(unsafe.Pointer(f.UnsafeAddr())) = *bytes.NewBuffer(make([]byte, 0, TLSBufSize))
}
