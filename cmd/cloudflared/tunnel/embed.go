package tunnel

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/urfave/cli/v2"

	"github.com/cloudflare/cloudflared/cmd/cloudflared/cliutil"
	cfdflags "github.com/cloudflare/cloudflared/cmd/cloudflared/flags"
	"github.com/cloudflare/cloudflared/connection"
)

// defaultQuickService is the trycloudflare endpoint that allocates account-less
// quick tunnels.
const defaultQuickService = "https://api.trycloudflare.com"

// EmbedOptions configures an in-process cloudflared tunnel. It is the
// programmatic equivalent of the flags a `cloudflared tunnel run` invocation
// would parse.
type EmbedOptions struct {
	// OriginURL is the local service cloudflared proxies to, e.g.
	// "http://127.0.0.1:7247".
	OriginURL string
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
}

// EmbedServerOption tunes StartServer for in-process use.
type EmbedServerOption func(*embedServerOptions)

type embedServerOptions struct {
	embedded bool
	sink     connection.EventSinkFunc
}

// withEmbedded marks a StartServer call as host-embedded: it must not trap
// process signals, initialise sentry, or run the autoupdater, and it registers
// sink on the connection observer.
func withEmbedded(sink connection.EventSinkFunc) EmbedServerOption {
	return func(o *embedServerOptions) {
		o.embedded = true
		o.sink = sink
	}
}

// RunEmbedded runs a cloudflared tunnel in-process. It blocks until ctx is
// canceled or a fatal error occurs, and never traps host process signals —
// lifecycle is entirely ctx-driven. For quick tunnels the public URL is
// delivered via EmbedOptions.Sink (a SetURL event) as soon as it is allocated.
func RunEmbedded(ctx context.Context, o EmbedOptions) error {
	if o.Logger == nil {
		return fmt.Errorf("embed: Logger is required")
	}
	if o.QuickServiceURL == "" {
		o.QuickServiceURL = defaultQuickService
	}
	info := cliutil.GetBuildInfo("embedded", o.Version)
	// Populate cloudflared's package-level globals (buildInfo, graceShutdownC)
	// normally set by the CLI entrypoint. StartServer reads the buildInfo global
	// for diagnostics; graceShutdownC is only consumed by the signal trap, which
	// the embedded path skips.
	Init(info, make(chan struct{}))
	c, err := o.buildContext(ctx, info)
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
			withEmbedded(o.Sink),
		)
	}
	return o.runQuick(ctx, c, info)
}

// buildContext constructs a *cli.Context populated with cloudflared's own
// tunnel/run flag defaults, overriding only the values the embed path needs.
// Unset flags read as their zero value via cli.Context, matching upstream.
func (o EmbedOptions) buildContext(ctx context.Context, info *cliutil.BuildInfo) (*cli.Context, error) {
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
	if o.OriginURL != "" {
		set("url", o.OriginURL)
	}
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
func (o EmbedOptions) runQuick(ctx context.Context, c *cli.Context, info *cliutil.BuildInfo) error {
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
		withEmbedded(o.Sink),
	)
}
