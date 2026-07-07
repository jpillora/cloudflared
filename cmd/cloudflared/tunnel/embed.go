package tunnel

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"
	"google.golang.org/grpc/test/bufconn"

	"github.com/jpillora/cloudflared/cmd/cloudflared/cliutil"
	cfdflags "github.com/jpillora/cloudflared/cmd/cloudflared/flags"
	"github.com/jpillora/cloudflared/connection"
	"github.com/jpillora/cloudflared/ingress"
)

// defaultQuickService is the trycloudflare endpoint that allocates account-less
// quick tunnels.
const defaultQuickService = "https://api.trycloudflare.com"

// originBufferSize is the per-connection buffer of the in-memory origin listener.
const originBufferSize = 256 * 1024

// Config configures an in-process cloudflared Tunnel. It is the programmatic
// equivalent of the flags a `cloudflared tunnel run` invocation would parse.
type Config struct {
	// Token, when set, runs a named tunnel using the base64 Cloudflare tunnel
	// token. When empty, an account-less quick tunnel is requested instead.
	Token string
	// QuickServiceURL overrides the quick-tunnel allocation service. Defaults to
	// https://api.trycloudflare.com.
	QuickServiceURL string
	// Logger is the zerolog logger cloudflared writes to. Required.
	Logger *zerolog.Logger
	// Sink, when set, receives tunnel events (SetURL, Connected, Disconnected,
	// ...) so the host can observe status without scraping logs.
	Sink connection.EventSinkFunc
	// Version is reported in the User-Agent and build info.
	Version string
	// MetricsAddr, when non-empty, runs cloudflared's metrics/readiness/
	// diagnostic HTTP server on this address (e.g. "127.0.0.1:0"). Empty
	// (default) means no metrics server is started and no OS TCP listener is
	// opened — metrics are strictly opt-in when embedded.
	MetricsAddr string
	// LocalOrigin, for a named tunnel, keeps this host's own origin (the handler
	// served over Listen) in force instead of the dashboard-managed service the
	// edge would otherwise push. The connector ignores remote config, so every
	// request is served by the local handler regardless of the tunnel's
	// dashboard ingress. No effect on quick tunnels (they have no remote config).
	LocalOrigin bool
}

// Tunnel is a single in-process cloudflared tunnel. Create one with New, then
// start it with Listen. A Tunnel is one-shot: use a fresh instance per run.
//
//	t := tunnel.New(tunnel.Config{Logger: &log, Sink: sink})
//	ln, err := t.Listen(ctx) // serve an in-process handler over the tunnel
//	http.Serve(ln, tunnel.ClientIPMiddleware(handler))
//
// Shutdown is entirely ctx-driven: cancel the context passed to Listen (or close
// the listener) to stop the tunnel. No host process signals are ever trapped.
type Tunnel struct {
	cfg Config

	mu        sync.Mutex
	getConfig func() ([]byte, error) // live config getter, set once the connector is up
}

// New returns a Tunnel configured with c. Call Listen to start it.
func New(c Config) *Tunnel {
	return &Tunnel{cfg: c}
}

// captureConfig stores the running connector's config getter so GetMetadata can
// read the live configuration without opening a second connection.
func (t *Tunnel) captureConfig(fn func() ([]byte, error)) {
	t.mu.Lock()
	t.getConfig = fn
	t.mu.Unlock()
}

// Listen starts the tunnel and returns a net.Listener whose connections are
// proxied from the public Cloudflare edge — hand it straight to http.Serve. No
// OS TCP port is bound and nothing touches the filesystem: cloudflared bridges
// the tunnel to a private in-memory listener that backs the returned listener,
// so the listener *is* the tunnel. Wrap the handler with ClientIPMiddleware to
// see the real client IP.
//
// A quick (account-less) tunnel is used unless Config.Token is set. The public
// URL is delivered through Config.Sink (a connection.SetURL event) as soon as it
// is allocated. Listen returns as soon as the origin is ready; the tunnel
// connects in the background. Closing the returned listener (or canceling ctx)
// stops serving and tears the tunnel down.
func (t *Tunnel) Listen(ctx context.Context) (net.Listener, error) {
	if t.cfg.Logger == nil {
		return nil, fmt.Errorf("embed: Logger is required")
	}
	// The origin is an in-memory listener, not a unix socket on disk. cloudflared
	// is pointed at it via a --unix-socket "path" that is really a registry key
	// (originKey); newHTTPTransport dials the registered in-memory listener.
	ln := bufconn.Listen(originBufferSize)
	originKey := "cfembed-mem:" + uuid.NewString()
	ingress.RegisterMemoryOrigin(originKey, func(dctx context.Context) (net.Conn, error) {
		return ln.DialContext(dctx)
	})

	ctx, cancel := context.WithCancel(ctx)
	tl := &tunnelListener{Listener: ln, cancel: cancel, originKey: originKey}
	go func() {
		if err := t.run(ctx, originKey); err != nil && ctx.Err() == nil {
			t.cfg.Logger.Error().Err(err).Msg("embed: tunnel exited")
		}
		_ = tl.Close()
	}()
	return tl, nil
}

// HeaderClientIP is the header Cloudflare's edge sets to the originating client
// IP on every request it forwards to an origin — including requests arriving
// through a tunnel.
const HeaderClientIP = "Cf-Connecting-Ip"

// ClientIPMiddleware returns an http.Handler that rewrites r.RemoteAddr from
// Cloudflare's Cf-Connecting-Ip header (the real client IP) before invoking
// next. A handler served over a Tunnel.Listen listener otherwise sees the origin
// unix socket's meaningless RemoteAddr, so anything downstream that logs, rate-
// limits or authorizes by client IP would be wrong. This is the Cloudflare
// analog of tailscale's WhoIsMiddleware. If the header is absent or not a valid
// IP, RemoteAddr is left unchanged.
func ClientIPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ip := r.Header.Get(HeaderClientIP); ip != "" && net.ParseIP(ip) != nil {
			r.RemoteAddr = net.JoinHostPort(ip, "0")
		}
		next.ServeHTTP(w, r)
	})
}

// Route is one ingress rule the tunnel serves — a public hostname (and optional
// path) mapped to a local service.
type Route struct {
	Hostname string `json:"hostname,omitempty"`
	Path     string `json:"path,omitempty"`
	Service  string `json:"service,omitempty"`
}

// Metadata describes a named tunnel: the identity carried in its token plus the
// ingress routes it currently serves (pulled live from the connector). Routes is
// empty for a tunnel that has no dashboard configuration.
type Metadata struct {
	AccountID     string  `json:"accountID"`
	TunnelID      string  `json:"tunnelID"`
	ConfigVersion int     `json:"configVersion"`
	Routes        []Route `json:"routes"`
}

// GetMetadata reads a named tunnel's identity + ingress routes using its token,
// spinning up a short-lived connector to pull the config and tearing it down.
// It's the standalone form of Tunnel.GetMetadata.
func GetMetadata(ctx context.Context, token string) (*Metadata, error) {
	return New(Config{Token: token}).GetMetadata(ctx)
}

// GetMetadata returns this tunnel's identity (from its token) and the ingress
// routes it currently serves. If the Tunnel is already running (Listen was
// called and the connector is up), the routes are read live with no extra
// connection — ideal for polling while active. Otherwise it briefly connects
// with the token to pull them. Requires Config.Token (a named tunnel).
func (t *Tunnel) GetMetadata(ctx context.Context) (*Metadata, error) {
	if t.cfg.Token == "" {
		return nil, fmt.Errorf("embed: GetMetadata requires a named tunnel token")
	}
	tok, err := ParseToken(t.cfg.Token)
	if err != nil {
		return nil, fmt.Errorf("embed: invalid tunnel token: %w", err)
	}
	md := &Metadata{AccountID: tok.AccountTag, TunnelID: tok.TunnelID.String(), ConfigVersion: -1}

	t.mu.Lock()
	getConfig := t.getConfig
	t.mu.Unlock()

	if getConfig != nil {
		// Already running — read the live config directly, no extra connection.
		if b, err := getConfig(); err == nil {
			parseVersionedConfig(md, b)
		}
		return md, nil
	}

	// Not running — spin a short-lived connector to pull the config.
	probe := New(t.cfg)
	if probe.cfg.Logger == nil {
		lg := zerolog.New(io.Discard)
		probe.cfg.Logger = &lg
	}
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ln, err := probe.Listen(pctx)
	if err != nil {
		return md, err
	}
	defer func() { _ = ln.Close() }()
	if b, ok := probe.waitConfig(pctx, 15*time.Second); ok {
		parseVersionedConfig(md, b)
	}
	return md, nil
}

// waitConfig blocks until the probe connector has published a config the edge
// pushed (version >= 0) or the timeout elapses, returning the latest config seen.
func (t *Tunnel) waitConfig(ctx context.Context, timeout time.Duration) ([]byte, bool) {
	deadline := time.Now().Add(timeout)
	var last []byte
	for {
		t.mu.Lock()
		fn := t.getConfig
		t.mu.Unlock()
		if fn != nil {
			if b, err := fn(); err == nil {
				last = b
				var v struct {
					Version int `json:"version"`
				}
				if json.Unmarshal(b, &v) == nil && v.Version >= 0 {
					return b, true
				}
			}
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return last, last != nil
		}
		select {
		case <-ctx.Done():
			return last, last != nil
		case <-time.After(300 * time.Millisecond):
		}
	}
}

// parseVersionedConfig fills md.ConfigVersion and md.Routes from the JSON
// produced by orchestrator.GetVersionedConfigJSON, dropping the default
// catch-all rule.
func parseVersionedConfig(md *Metadata, b []byte) {
	var vc struct {
		Version int `json:"version"`
		Config  struct {
			Ingress []Route `json:"ingress"`
		} `json:"config"`
	}
	if err := json.Unmarshal(b, &vc); err != nil {
		return
	}
	md.ConfigVersion = vc.Version
	for _, r := range vc.Config.Ingress {
		// Only hostname-bound rules are real public routes; a hostname-less rule
		// is the catch-all default (the local origin the connector proxies to).
		if r.Hostname == "" {
			continue
		}
		md.Routes = append(md.Routes, r)
	}
}

// run bridges the tunnel to the unix-socket origin backing Listen's listener,
// blocking until ctx is canceled or a fatal error occurs. It operates on a copy
// of the config so per-run defaults (e.g. QuickServiceURL) never mutate the
// instance's stored Config.
func (t *Tunnel) run(ctx context.Context, originSocket string) error {
	o := t.cfg
	if o.QuickServiceURL == "" {
		o.QuickServiceURL = defaultQuickService
	}
	info := cliutil.GetBuildInfo("embedded", o.Version)
	// Prepare the process for embedded, multi-instance use (idempotent): populate
	// cloudflared's CLI globals and redirect its metric registration off the
	// host's default prometheus registry. See makeMultiInstanceSafe.
	makeMultiInstanceSafe(info)
	c, err := o.buildContext(ctx, info, originSocket)
	if err != nil {
		return err
	}
	if o.Token != "" {
		tok, err := ParseToken(o.Token)
		if err != nil {
			return fmt.Errorf("embed: invalid tunnel token: %w", err)
		}
		return StartServer(
			c,
			info,
			&connection.TunnelProperties{Credentials: tok.Credentials()},
			o.Logger,
			withEmbedded(o.Sink, o.MetricsAddr, t.captureConfig, o.LocalOrigin),
		)
	}
	return o.runQuick(ctx, c, info)
}

// EmbedServerOption tunes StartServer for in-process use.
type EmbedServerOption func(*embedServerOptions)

type embedServerOptions struct {
	embedded    bool
	sink        connection.EventSinkFunc
	metricsAddr string
	onConfig    func(func() ([]byte, error))
	localOrigin bool
}

// withEmbedded marks a StartServer call as host-embedded: it must not trap
// process signals, initialise sentry, run the autoupdater, or start the metrics
// server (unless metricsAddr is set), and it registers sink on the connection
// observer. onConfig, when set, receives the orchestrator's live config getter
// once it exists (so GetMetadata can read the tunnel's current routes).
// localOrigin keeps the local origin in force (ignore edge-pushed config).
func withEmbedded(sink connection.EventSinkFunc, metricsAddr string, onConfig func(func() ([]byte, error)), localOrigin bool) EmbedServerOption {
	return func(o *embedServerOptions) {
		o.embedded = true
		o.sink = sink
		o.metricsAddr = metricsAddr
		o.onConfig = onConfig
		o.localOrigin = localOrigin
	}
}

// embedOnce guards the one-time, process-wide setup embedded tunnels need.
var embedOnce sync.Once

// makeMultiInstanceSafe prepares the host process so that concurrent Tunnel
// runs are safe. It runs exactly once per process and only from the embedded
// path, so CLI behaviour is never affected. It addresses two cloudflared
// globals that are otherwise unsafe to share across tunnels:
//
//   - Init populates the buildInfo/graceShutdownC package globals normally set
//     by the CLI entrypoint (StartServer reads buildInfo for diagnostics;
//     graceShutdownC feeds the signal trap the embedded path skips). buildInfo
//     is effectively constant and graceShutdownC is unused when embedded, so
//     first-writer-wins is fine — the Once only removes the data race.
//
//   - Metrics are made strictly opt-in. cloudflared hardcodes metric
//     registration onto the process-global prometheus.DefaultRegisterer, once
//     per tunnel (e.g. supervisor's v3.NewMetrics). Left as-is that both
//     registers cloudflared's metrics on the host's default registry as an
//     automatic side effect of embedding AND panics on the second concurrent
//     tunnel ("duplicate metrics collector registration attempted"), because a
//     registry rejects duplicate collectors. We redirect DefaultRegisterer — and
//     its paired DefaultGatherer — to a private, duplicate-tolerant registry:
//     nothing lands on the host's real default registry, concurrent tunnels no
//     longer panic, and the opt-in metrics server (Config.MetricsAddr) still
//     exposes cloudflared's metrics because promhttp.Handler() serves
//     DefaultGatherer. With MetricsAddr empty (the default) no metrics server
//     runs and nothing is registered or exposed — metrics are never automatic.
func makeMultiInstanceSafe(info *cliutil.BuildInfo) {
	embedOnce.Do(func() {
		Init(info, make(chan struct{}))
		reg := prometheus.NewRegistry()
		prometheus.DefaultRegisterer = dedupeRegisterer{Registerer: reg}
		prometheus.DefaultGatherer = reg
	})
}

// dedupeRegisterer wraps a prometheus.Registerer and turns duplicate collector
// registrations into no-ops instead of errors. cloudflared registers the same
// collectors once per tunnel; without this the second concurrent embedded
// tunnel panics with "duplicate metrics collector registration attempted". The
// first registration wins and stays live; later duplicates are silently
// accepted.
type dedupeRegisterer struct {
	prometheus.Registerer
}

func (d dedupeRegisterer) Register(c prometheus.Collector) error {
	err := d.Registerer.Register(c)
	if err != nil {
		var already prometheus.AlreadyRegisteredError
		if errors.As(err, &already) {
			return nil
		}
	}
	return err
}

// MustRegister is overridden (not inherited) so it routes through this type's
// dup-swallowing Register; the promoted method would call the embedded
// registerer's Register and panic on duplicates.
func (d dedupeRegisterer) MustRegister(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := d.Register(c); err != nil {
			panic(err)
		}
	}
}

// buildContext constructs a *cli.Context populated with cloudflared's own
// tunnel/run flag defaults, overriding only the values the embed path needs.
// Unset flags read as their zero value via cli.Context, matching upstream.
func (o Config) buildContext(ctx context.Context, info *cliutil.BuildInfo, originSocket string) (*cli.Context, error) {
	fs := flag.NewFlagSet("tunnel", flag.ContinueOnError)
	seen := map[string]bool{}
	apply := func(list []cli.Flag) error {
		for _, f := range list {
			dup := false
			for _, n := range f.Names() {
				if seen[n] {
					dup = true
					break
				}
			}
			if dup {
				continue
			}
			if err := f.Apply(fs); err != nil {
				return fmt.Errorf("embed: apply flag %v: %w", f.Names(), err)
			}
			for _, n := range f.Names() {
				seen[n] = true
			}
		}
		return nil
	}
	if err := apply(tunnelFlags(false)); err != nil {
		return nil, err
	}
	if err := apply(buildRunCommand().Flags); err != nil {
		return nil, err
	}
	set := func(name, val string) {
		if err := fs.Set(name, val); err != nil {
			o.Logger.Warn().Str("flag", name).Err(err).Msg("embed: failed to set flag")
		}
	}
	// The origin is Listen's private in-memory listener, addressed via the
	// --unix-socket flag whose value is a registry key (not a real path) that
	// ingress dials in memory — this is what lets a handler be served with no OS
	// TCP listener bound and nothing on disk.
	set("unix-socket", originSocket)
	set(cfdflags.Protocol, "quic")
	set(cfdflags.HaConnections, "1")
	set(cfdflags.NoAutoUpdate, "true")
	set("quick-service", o.QuickServiceURL)
	if o.Token != "" {
		set(TunnelTokenFlag, o.Token)
	}
	app := &cli.App{Name: "cloudflared", Version: o.Version}
	c := cli.NewContext(app, fs, nil)
	c.Context = ctx
	return c, nil
}

// runQuick allocates an account-less quick tunnel (mirroring RunQuickTunnel's
// request) and hands the resulting credentials to StartServer. The hostname is
// known synchronously from the allocation response.
func (o Config) runQuick(ctx context.Context, c *cli.Context, info *cliutil.BuildInfo) error {
	client := http.Client{
		Transport: &http.Transport{
			TLSHandshakeTimeout:   httpTimeout,
			ResponseHeaderTimeout: httpTimeout,
		},
		Timeout: httpTimeout,
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/tunnel", o.QuickServiceURL), nil)
	if err != nil {
		return fmt.Errorf("embed: build quick tunnel request: %w", err)
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", info.UserAgent())
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("embed: request quick tunnel: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("embed: read quick tunnel response: %w", err)
	}
	var data QuickTunnelResponse
	if err := json.Unmarshal(body, &data); err != nil {
		return fmt.Errorf("embed: unmarshal quick tunnel response (%s): %w", resp.Status, err)
	}
	tunnelID, err := uuid.Parse(data.Result.ID)
	if err != nil {
		return fmt.Errorf("embed: parse quick tunnel id: %w", err)
	}
	credentials := connection.Credentials{
		AccountTag:   data.Result.AccountTag,
		TunnelSecret: data.Result.Secret,
		TunnelID:     tunnelID,
	}
	return StartServer(
		c,
		info,
		&connection.TunnelProperties{Credentials: credentials, QuickTunnelUrl: data.Result.Hostname},
		o.Logger,
		withEmbedded(o.Sink, o.MetricsAddr, nil, false), // quick tunnels have no routes/metadata or remote config
	)
}

// tunnelListener is the net.Listener returned by Tunnel.Listen. Its connections
// arrive from the Cloudflare edge via a private in-memory origin. Close stops
// the tunnel and unregisters the origin.
type tunnelListener struct {
	net.Listener
	cancel    context.CancelFunc
	originKey string
	closeOnce sync.Once
}

func (l *tunnelListener) Close() error {
	l.cancel()
	err := l.Listener.Close()
	l.closeOnce.Do(func() { ingress.UnregisterMemoryOrigin(l.originKey) })
	return err
}
