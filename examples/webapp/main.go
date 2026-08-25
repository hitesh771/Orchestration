// Command webapp is a sample workload for mini-k8s.
//
// It serves HTTP on the port the agent assigns and burns a measurable amount of
// CPU per request, so load applied to it shows up in telemetry and drives the
// autoscaler. A workload that only slept would prove the request path works but
// leave the CPU-based scaling path untested.
package main

import (
	"crypto/sha256"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

// workPerRequest is how many hash rounds each request costs.
//
// Sized so a few hundred requests per second consume a visible fraction of a
// core: telemetry is sampled every few seconds, and work cheap enough to finish
// between samples registers as no load at all, which leaves the autoscaling
// path untested however much traffic is applied. Still small enough that one
// request returns promptly.
const workPerRequest = 120000

var served atomic.Uint64

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	podID := os.Getenv("MINIK8S_POD_ID")

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		burnCPU(workPerRequest)
		n := served.Add(1)
		fmt.Fprintf(w, "pod=%s served=%d\n", podID, n)
	})
	// A liveness path that does no work, so probing cannot itself create load.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	srv := &http.Server{
		Addr:              net.JoinHostPort("", port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Shut down on signal rather than dying instantly: the supervisor sends
	// SIGTERM first precisely so a workload can finish what it is holding.
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		_ = srv.Close()
	}()

	log.Printf("webapp pod=%s listening on :%s", podID, port)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve: %v", err)
	}
}

// burnCPU does a fixed amount of real work.
func burnCPU(rounds int) {
	buf := []byte(strconv.Itoa(rounds))
	for i := 0; i < rounds; i++ {
		sum := sha256.Sum256(buf)
		buf = sum[:]
	}
}
