// Command fileserver serves a directory over a Cloudflare quick tunnel.
//
// It never binds a TCP port: its net.Listener comes from tunnel.Listen, so the
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

	// ln is backed by the Cloudflare tunnel, not an OS TCP socket.
	ln, err := tunnel.Listen(ctx, tunnel.EmbedOptions{
		Logger:  &log,
		Sink:    sink,
		Version: "fileserver-example",
	})
	if err != nil {
		log.Fatal().Err(err).Msg("start tunnel listener")
	}
	defer ln.Close()

	// http.Serve accepts connections straight off the tunnel.
	if err := http.Serve(ln, http.FileServer(http.Dir(dir))); err != nil {
		log.Info().Err(err).Msg("server stopped")
	}
}
