package ingress

import (
	"context"
	"net"
	"sync"
)

// memoryOrigins lets an embedding host back a --unix-socket origin with an
// in-memory listener instead of a real unix socket on disk. The host (see
// cmd/cloudflared/tunnel.Tunnel.Listen) registers a dialer under the socket
// "path" — a unique per-tunnel key — and newHTTPTransport dials the origin
// through it instead of dialing a unix socket. Keyed per tunnel, so it stays
// multi-instance safe.
var memoryOrigins sync.Map // key string -> func(context.Context) (net.Conn, error)

// RegisterMemoryOrigin routes the given --unix-socket key to an in-memory
// dialer (e.g. a bufconn listener's DialContext).
func RegisterMemoryOrigin(key string, dial func(context.Context) (net.Conn, error)) {
	memoryOrigins.Store(key, dial)
}

// UnregisterMemoryOrigin removes a key previously passed to RegisterMemoryOrigin.
func UnregisterMemoryOrigin(key string) {
	memoryOrigins.Delete(key)
}

func memoryOriginDialer(key string) (func(context.Context) (net.Conn, error), bool) {
	v, ok := memoryOrigins.Load(key)
	if !ok {
		return nil, false
	}
	return v.(func(context.Context) (net.Conn, error)), true
}
