package main

import "github.com/miragepresent/go-sse-delivery/server"

func main() {
	srv := server.NewServer()
	srv.Start()
}
