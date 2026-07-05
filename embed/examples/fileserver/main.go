// Command fileserver serves a directory over a Cloudflare quick tunnel.
//
// It never binds a TCP port: its net.Listener comes from Tunnel.Listen, so the
// only public listener is the Cloudflare edge. Run it and open the printed
// https://<random>.trycloudflare.com URL.
//
//	go run ./embed/examples/fileserver [dir]
package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"

	"github.com/rs/zerolog"

	"github.com/jpillora/cloudflared/cmd/cloudflared/tunnel"
	"github.com/jpillora/cloudflared/connection"
)

func main() {
	dir := "."
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}

	// Host owns the process lifecycle: cancel ctx to stop the tunnel.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	// The public URL arrives here as soon as the quick tunnel is allocated.
	sink := connection.EventSinkFunc(func(e connection.Event) {
		switch e.EventType {
		case connection.SetURL:
			log.Info().Msgf("serving %q at https://%s", dir, e.URL)
		case connection.Connected:
			log.Info().Msg("edge connected")
		}
	})

	// new tunnel -> listen. The returned listener *is* the tunnel; ln is backed
	// by the Cloudflare edge, not an OS TCP socket.
	ln, err := tunnel.New(tunnel.Config{Logger: &log, Sink: sink, Version: "fileserver-example"}).Listen(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("start tunnel listener")
	}
	defer ln.Close()

	// ClientIPMiddleware restores the real client IP (r.RemoteAddr) from the
	// Cf-Connecting-Ip header — requests off the tunnel otherwise carry the
	// origin unix socket's meaningless address.
	handler := tunnel.ClientIPMiddleware(http.FileServer(http.Dir(dir)))
	if err := http.Serve(ln, handler); err != nil && ctx.Err() == nil {
		log.Info().Err(err).Msg("server stopped")
	}
}
