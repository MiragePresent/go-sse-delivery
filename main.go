package main

import (
	"flag"

	"github.com/miragepresent/go-sse-delivery/server"
)

func main() {
	debug := flag.Bool("debug", false, "Enable debug mode with test page at /debug/")
	flag.Parse()

	config := server.DefaultConfig()
	config.Debug = *debug

	srv := server.NewServer(config)
	srv.Start()
}
