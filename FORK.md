# FORK.md — jpillora/cloudflared (embed fork)

This is a fork of [`cloudflare/cloudflared`](https://github.com/cloudflare/cloudflared) that adds
a small, additive **in-process embedding API** so a host Go application can run a Cloudflare
Tunnel as a library — like Tailscale's `tsnet` — instead of shelling out to the `cloudflared`
binary.

It was created for [rais](https://github.com/jpillora/rais)'s "one-click public URL" feature
(the public sibling of rais's embedded Tailscale integration). No subprocess, no binary to
install/detect/supervise, and the quick-tunnel URL is known programmatically instead of by
scraping logs.

- **Upstream:** `cloudflare/cloudflared` (remote `upstream`)
- **Fork:** `jpillora/cloudflared` (remote `origin`)
- **Patch branch:** `embed`
- **Module path: UNCHANGED** — still `module github.com/cloudflare/cloudflared`. Renaming it would
  force rewriting every internal `github.com/cloudflare/cloudflared/...` import and would make
  upstream merges conflict on every file. Consumers use a `replace` directive instead (see
  "Consuming this fork" below).

## Design goals

1. **Tiny, additive diff.** The entire divergence from upstream is **2 files**, so
   `git merge upstream/master` stays cheap. One file is brand new; the other has a handful of
   small guarded hunks.
2. **Host owns process lifecycle.** Embedded, cloudflared must not trap OS signals, init sentry,
   notify systemd, or run its autoupdater. Shutdown is entirely `context.Context`-driven.
3. **Observable without log scraping.** Status (URL, connected, disconnected) is delivered through
   cloudflared's existing `connection.Observer` event sink.

## What changed vs upstream

### 1. `cmd/cloudflared/tunnel/embed.go` (new file)

The public entrypoint, in `package tunnel` so it can reach the package's unexported helpers
(`tunnelFlags`, `buildRunCommand`, `ParseToken`, `StartServer`, `Init`, `httpTimeout`,
`QuickTunnelResponse`).

```go
// EmbedOptions configures an in-process cloudflared tunnel — the programmatic
// equivalent of the flags a `cloudflared tunnel run` invocation would parse.
type EmbedOptions struct {
    OriginURL       string                   // local service to proxy to, e.g. "http://127.0.0.1:7247"
    Token           string                   // named tunnel token; empty => account-less quick tunnel
    QuickServiceURL string                   // default "https://api.trycloudflare.com"
    Logger          *zerolog.Logger          // required
    Sink            connection.EventSinkFunc // receives SetURL / Connected / Disconnected / ... events
    Version         string                   // reported in User-Agent + build info
}

// RunEmbedded runs a cloudflared tunnel in-process. It blocks until ctx is
// canceled or a fatal error occurs, and never traps host process signals.
func RunEmbedded(ctx context.Context, o EmbedOptions) error
```

What it does:
- Calls `Init(info, make(chan struct{}))` to populate cloudflared's package globals `buildInfo`
  and `graceShutdownC` (normally set by the CLI entrypoint). **Required** — without it
  `StartServer` nil-derefs at the diagnostic collector (`cmd.go`, `buildInfo.CloudflaredVersion`).
- Builds a `*cli.Context` by `.Apply`-ing cloudflared's own `tunnelFlags(false)` +
  `buildRunCommand().Flags` onto a `flag.FlagSet` (so **all upstream flag defaults are preserved**),
  then overrides only `url`, `protocol=quic`, `ha-connections=1`, `no-autoupdate=true`,
  `quick-service`, and (named) `token`. Sets `c.Context = ctx`.
- **Quick mode** (`Token == ""`): POSTs `"<QuickServiceURL>/tunnel"` (mirroring upstream
  `RunQuickTunnel`), reads `QuickTunnelResponse`; the public hostname is `Result.Hostname`
  (known synchronously) and the credentials come from the same response. Then calls
  `StartServer(..., &connection.TunnelProperties{Credentials, QuickTunnelUrl}, ..., withEmbedded(sink))`.
- **Named mode** (`Token != ""`): `ParseToken(Token)` → `StartServer(..., &connection.TunnelProperties{Credentials: tok.Credentials()}, ..., withEmbedded(sink))`.
  The public hostname is **not** in the token (it's whatever the user routed in the Cloudflare
  dashboard), so the host supplies it separately for display.

It also defines the option plumbing consumed by the `StartServer` guards:

```go
type EmbedServerOption func(*embedServerOptions)
type embedServerOptions struct { embedded bool; sink connection.EventSinkFunc }
func withEmbedded(sink connection.EventSinkFunc) EmbedServerOption
```

### 2. `cmd/cloudflared/tunnel/cmd.go` — `StartServer` guards (edited)

`StartServer` gained a trailing variadic parameter (existing CLI callers in `quick_tunnel.go` and
`subcommand_context.go` are unaffected — variadic is optional):

```go
func StartServer(
    c *cli.Context,
    info *cliutil.BuildInfo,
    namedTunnel *connection.TunnelProperties,
    log *zerolog.Logger,
    embedOpts ...EmbedServerOption, // NEW
) error {
    var emb embedServerOptions
    for _, opt := range embedOpts { opt(&emb) }
    ...
}
```

When `emb.embedded` is set, these are skipped (host owns the process):
- `sentry.Init(...)`
- `go waitForSignal(graceShutdownC, log)` — the OS SIGTERM/SIGINT trap
- `go notifySystemd(connectedSignal)`
- the `updater.NewAutoUpdater(...).Run(ctx)` goroutine (and its `wg.Add(1)`)

And when a sink is provided, it is registered right after the observer is created:
```go
observer := connection.NewObserver(log, logTransport)
if emb.sink != nil { observer.RegisterSink(emb.sink) }
```

Everything else in `StartServer` is unchanged.

## Status events

The sink is a `connection.EventSinkFunc` receiving `connection.Event`. Relevant `EventType`s:
- `connection.SetURL` — `event.URL` holds the tunnel hostname (quick mode; fired as soon as the
  tunnel is allocated, before the edge connection is up). Note: bare hostname, prepend `https://`.
- `connection.Connected` — an edge connection registered (`event.Index`, `event.Location`).
- `connection.Disconnected` — an edge connection dropped.

This is how a host surfaces live tunnel status without parsing logs.

## Lifecycle & shutdown

- `RunEmbedded` blocks until `ctx` is canceled or a fatal error occurs.
- Cancel the `ctx` you passed in to stop the tunnel. `StartServer` derives its server context from
  `c.Context`; canceling it stops the supervisor, which returns through the internal `errC`, and
  `waitToShutdown` unblocks on that. (The graceful `graceShutdownC` path is unused when embedded —
  we hand `Init` a fresh channel that is never closed.)
- No host signals are intercepted, so the host's own SIGTERM handling is unaffected.

## Consuming this fork (host `go.mod`)

The host imports `github.com/cloudflare/cloudflared/cmd/cloudflared/tunnel` and replaces the
module with this fork. **Two things are mandatory:**

```gomod
require github.com/cloudflare/cloudflared v0.0.0-00010101000000-000000000000

// Point the upstream module at this fork. For local dev use a filesystem path;
// for committed/CI builds use the fork with a pseudo-version of the `embed` HEAD.
replace github.com/cloudflare/cloudflared => ../cloudflared
// replace github.com/cloudflare/cloudflared => github.com/jpillora/cloudflared v0.0.0-<ts>-<sha>

// MIRROR cloudflared's own replace directives. Go does NOT apply a dependency's
// replaces to the main build, so without these the host pulls incompatible real
// quic-go / stock urfave-cli and fails to compile.
replace github.com/urfave/cli/v2           => github.com/ipostelnik/cli/v2 v2.3.1-0.20210324024421-b6ea8234fe3d
replace github.com/prometheus/golang_client => github.com/prometheus/golang_client v1.12.1
replace gopkg.in/yaml.v3                    => gopkg.in/yaml.v3 v3.0.1
replace github.com/quic-go/quic-go         => github.com/chungthuang/quic-go v0.45.1-0.20260529212404-a9fddf436fc4
```

(These four are copied verbatim from this fork's `go.mod`; if upstream changes them, re-copy.)

Requirements: **Go 1.26+** (this module is `go 1.26`). cloudflared and quic-go are pure Go — no
CGO is added by embedding.

Minimal host usage:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

logger := zerolog.New(io.Discard) // or bridge to your logger; status comes from the sink
sink := connection.EventSinkFunc(func(e connection.Event) {
    switch e.EventType {
    case connection.SetURL:       // e.URL -> "https://" + e.URL
    case connection.Connected:    // tunnel is live
    case connection.Disconnected: // dropped
    }
})

err := tunnel.RunEmbedded(ctx, tunnel.EmbedOptions{
    OriginURL: "http://127.0.0.1:7247", // the local service to expose
    // Token:  "<base64 CF tunnel token>", // omit for a quick tunnel
    Logger:  &logger,
    Sink:    sink,
    Version: "myapp",
})
```

## Validation

Proven end-to-end with a standalone module (`/tmp/cfembed-smoke` on the dev box: a `go.mod` with
the replace set above + a ~40-line `main.go`). It:
1. started a local HTTP origin,
2. called `RunEmbedded` in quick mode,
3. received `SetURL` (`<random>.trycloudflare.com`) then `Connected` (edge `cbr01`),
4. fetched `https://<random>.trycloudflare.com` → **200**, body proxied from the local origin,
5. canceled the context → clean shutdown, `RunEmbedded` returned `nil`, `Disconnected` fired.

The standalone binary was ~36 MB (a marker for the dependency weight embedding adds).

## Re-syncing with upstream

The patch is intentionally tiny. To pull a newer cloudflared:

```bash
git fetch upstream
git checkout master && git merge upstream/master   # keep master == upstream
git checkout embed && git rebase master             # or merge master into embed
```

Only `cmd/cloudflared/tunnel/cmd.go` can conflict, and only around the guarded blocks
(`sentry.Init`, `waitForSignal`, `notifySystemd`, the autoupdater goroutine, and the
`observer := connection.NewObserver(...)` line). `embed.go` is purely additive. If upstream
renames `StartServer`, `tunnelFlags`, `buildRunCommand`, `ParseToken`, `Init`, or the
`connection.Observer` sink API, update `embed.go` to match. Also re-copy the four `replace`
directives above if upstream changes them.

## Caveats

- **quick tunnels** (`trycloudflare.com`) have no uptime guarantee and rotate their hostname each
  run — fine for dev/experimentation, not production. Named tunnels (token) give a stable
  hostname.
- **prometheus:** cloudflared registers metrics on the default registry at runtime (via
  `NewObserver`/`StartServer`, not `init()`). If the host also uses the default prometheus
  registry with colliding metric names, expect a duplicate-registration panic. Audit if relevant.
- A harmless `failed to sufficiently increase receive buffer size` warning from quic-go may print
  on start; see the [quic-go UDP buffer sizes wiki](https://github.com/quic-go/quic-go/wiki/UDP-Buffer-Sizes).
