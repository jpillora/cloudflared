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
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"

	"github.com/jpillora/cloudflared/cmd/cloudflared/cliutil"
	cfdflags "github.com/jpillora/cloudflared/cmd/cloudflared/flags"
	"github.com/jpillora/cloudflared/connection"
)

// defaultQuickService is the trycloudflare endpoint that allocates account-less
// quick tunnels.
const defaultQuickService = "https://api.trycloudflare.com"

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
}

// New returns a Tunnel configured with c. Call Listen to start it.
func New(c Config) *Tunnel {
	return &Tunnel{cfg: c}
}

// Listen starts the tunnel and returns a net.Listener whose connections are
// proxied from the public Cloudflare edge — hand it straight to http.Serve. No
// OS TCP port is ever bound: cloudflared bridges the tunnel to a private
// unix-socket origin that backs the returned listener, so the listener *is* the
// tunnel. Wrap the handler with ClientIPMiddleware to see the real client IP.
//
// A quick (account-less) tunnel is used unless Config.Token is set. The public
// URL is delivered through Config.Sink (a connection.SetURL event) as soon as it
// is allocated. Listen returns as soon as the origin socket is ready; the tunnel
// connects in the background. Closing the returned listener (or canceling ctx)
// stops serving and tears the tunnel down.
func (t *Tunnel) Listen(ctx context.Context) (net.Listener, error) {
	if t.cfg.Logger == nil {
		return nil, fmt.Errorf("embed: Logger is required")
	}
	dir, err := os.MkdirTemp("", "cfembed")
	if err != nil {
		return nil, fmt.Errorf("embed: create origin dir: %w", err)
	}
	sock := filepath.Join(dir, "origin.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("embed: listen on origin socket: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	tl := &tunnelListener{Listener: ln, cancel: cancel, dir: dir}
	go func() {
		if err := t.run(ctx, sock); err != nil && ctx.Err() == nil {
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
			withEmbedded(o.Sink, o.MetricsAddr),
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
}

// withEmbedded marks a StartServer call as host-embedded: it must not trap
// process signals, initialise sentry, run the autoupdater, or start the metrics
// server (unless metricsAddr is set), and it registers sink on the connection
// observer.
func withEmbedded(sink connection.EventSinkFunc, metricsAddr string) EmbedServerOption {
	return func(o *embedServerOptions) {
		o.embedded = true
		o.sink = sink
		o.metricsAddr = metricsAddr
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
	// The origin is always Listen's private unix socket, routed through the
	// --unix-socket flag — this is what lets a handler be served with no OS TCP
	// listener bound.
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
		withEmbedded(o.Sink, o.MetricsAddr),
	)
}

// tunnelListener is the net.Listener returned by Tunnel.Listen. Its connections
// arrive from the Cloudflare edge via a private unix-socket origin. Close stops
// the tunnel and removes the socket's temp dir.
type tunnelListener struct {
	net.Listener
	cancel    context.CancelFunc
	dir       string
	closeOnce sync.Once
}

func (l *tunnelListener) Close() error {
	l.cancel()
	err := l.Listener.Close()
	l.closeOnce.Do(func() { _ = os.RemoveAll(l.dir) })
	return err
}
