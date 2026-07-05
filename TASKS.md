# TASKS

a [meads](https://github.com/jpillora/meads) (`md`) managed task log

* created: 2026-07-05T10:54:02Z
* updated: 2026-07-05T11:36:30Z

## 1. embed: multi-instance safety (concurrent tunnels)

* status: closed
* priority: P2
* type: task
* status-reason: Fixed: private dedupe prometheus registry (metrics opt-in, no dup panic) + observer sink-dispatch race (reliable SetURL). Verified with 3 concurrent quick tunnels: no panic, 3/3 connected, 3/3 SetURL, 3/3 served, zero TCP listeners.
* created: 2026-07-05T10:54:02Z
* updated: 2026-07-05T11:36:30Z

Make embedded tunnels multi-instance safe (concurrent RunEmbedded/Listen)

This `embed` fork (github.com/jpillora/cloudflared) is NOT multi-instance safe. Running >1 concurrent RunEmbedded/Listen in ONE process panics:
    panic: duplicate metrics collector registration attempted
CONFIRMED empirically from the rais side (2 concurrent quick tunnels -> panic during the 2nd supervisor startup, ~7s in).

Root causes:
- supervisor/supervisor.go:79 `v3.NewMetrics(prometheus.DefaultRegisterer)` runs PER TUNNEL (NewSupervisor <- StartTunnelDaemon:103) and does `registerer.MustRegister(active_flows,total_flows,failed_flows,...)` -> 2nd tunnel dup-panics on the default registry. AUDIT other per-tunnel MustRegister sites too (supervisor/tunnelsforha.go NewTunnelsForHA "tunnel_ids"; supervisor/metrics.go + orchestration/metrics.go are init()-once = safe; connection/metrics.go newTunnelMetrics is sync.Once = safe; metrics.RegisterBuildInfo is CLI-main only = safe).
- cmd/cloudflared/tunnel/cmd.go Init() sets package globals buildInfo/graceShutdownC; concurrent RunEmbedded calls race them (benign — buildInfo constant, graceShutdownC unused embedded — but still a data race under -race).

Recommended fix (keep additive, in embed.go only, minimal hand-written fork diff):
- sync.Once-guard the Init() call inside RunEmbedded.
- Once-guard swapping `prometheus.DefaultRegisterer` with a dedupe registerer that delegates to a real registry but swallows prometheus.AlreadyRegisteredError (Register returns nil on dup; MustRegister never panics). Embedded metrics are opt-in/off when MetricsAddr=="" so routing per-tunnel duplicate registrations to a throwaway deduping registry is fine. Do this ONLY in the embedded path (don't change CLI behaviour). Alternative (threading a per-instance registry through supervisor/connection) is more invasive and drifts from upstream — prefer the DefaultRegisterer swap.

DONE-WHEN: a concurrent smoke test starting 2+ RunEmbedded/Listen in one process runs with NO panic and each tunnel connects; `go build ./...`; commit on the `embed` branch + push; then re-pin rais go.mod require to the new pseudo-version. Keep the hand-written diff to embed.go (see FORK.md re-sync notes).

Motivation: unblocks rais's "arbitrary Cloudflare tunnels like Tailscale listeners" feature (rais md#590 / md#604) — rais's main tunnel + N listener tunnels run concurrently in one process.
