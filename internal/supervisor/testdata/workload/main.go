// Command workload is a test fixture: a process that runs until terminated.
// It optionally binds the port given in PORT so port-related behavior can be
// exercised, but stays alive either way.
package main

import (
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if port := os.Getenv("PORT"); port != "" {
		if ln, err := net.Listen("tcp", ":"+port); err == nil {
			defer ln.Close()
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					conn.Close()
				}
			}()
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
