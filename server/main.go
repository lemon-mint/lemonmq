package main

import (
	"net"

	"github.com/lemon-mint/lemonmq"
)

func main() {
	server := lemonmq.NewServer(0)
	ln, err := net.Listen("tcp", ":5555")
	if err != nil {
		panic(err)
	}
	server.Ln = ln
	server.Serve()
}
