// Command loadgen applies rate-ramped HTTP load to a target.
//
// It exists to drive the autoscaler: a fixed-rate generator can only show that
// a cluster copes or does not, whereas a ramp shows the replica count tracking
// demand and then settling, which is the behavior worth verifying.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// requestTimeout bounds each request. Without it a saturated backend would stall
// workers and quietly reduce the offered rate, understating the load applied.
const requestTimeout = 5 * time.Second

type stats struct {
	sent   atomic.Uint64
	ok     atomic.Uint64
	failed atomic.Uint64
}

func main() {
	target := flag.String("target", "", "URL to request (required)")
	start := flag.Int("start-rps", 10, "initial requests per second")
	peak := flag.Int("peak-rps", 200, "peak requests per second")
	step := flag.Int("step-rps", 10, "rate increase per step")
	stepEvery := flag.Duration("step-every", 5*time.Second, "how often the rate steps up")
	hold := flag.Duration("hold", 30*time.Second, "how long to hold peak before stopping")
	workers := flag.Int("workers", 64, "concurrent request workers")
	flag.Parse()

	if *target == "" {
		fmt.Fprintln(os.Stderr, "loadgen: -target is required")
		flag.Usage()
		os.Exit(2)
	}
	if *start <= 0 || *peak < *start || *step <= 0 || *workers <= 0 {
		fmt.Fprintln(os.Stderr, "loadgen: rates must be positive and peak must be at least start")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *target, *start, *peak, *step, *stepEvery, *hold, *workers); err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, target string, start, peak, step int,
	stepEvery, hold time.Duration, workers int) error {

	client := &http.Client{
		Timeout: requestTimeout,
		Transport: &http.Transport{
			// Connections are pooled per worker: without this the generator
			// spends its time in TCP setup rather than applying the rate asked
			// for, and exhausts local ports at high rates.
			MaxIdleConnsPerHost: workers,
		},
	}

	var s stats
	jobs := make(chan struct{}, workers)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
				if err != nil {
					s.failed.Add(1)
					continue
				}
				resp, err := client.Do(req)
				if err != nil {
					s.failed.Add(1)
					continue
				}
				// The body must be drained or the connection cannot be reused,
				// which silently collapses the pool back to one-shot dials.
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode < 400 {
					s.ok.Add(1)
				} else {
					s.failed.Add(1)
				}
			}
		}()
	}

	rate := start
	atPeakSince := time.Time{}
	report := time.NewTicker(time.Second)
	defer report.Stop()
	rampUp := time.NewTicker(stepEvery)
	defer rampUp.Stop()

	fmt.Printf("loadgen: %s, %d -> %d rps in steps of %d every %s\n",
		target, start, peak, step, stepEvery)

	// The dispatcher paces work rather than the workers themselves: a fixed
	// worker pool spinning as fast as it can would apply whatever rate the
	// backend allows, not the rate under test.
	tick := time.NewTicker(time.Second / time.Duration(rate))
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			printSummary(&s)
			return nil

		case <-tick.C:
			select {
			case jobs <- struct{}{}:
				s.sent.Add(1)
			default:
				// Every worker is busy. Dropping the slot rather than blocking
				// keeps the clock honest: a blocked dispatcher would report a
				// rate it never actually offered.
			}

		case <-rampUp.C:
			if rate < peak {
				rate += step
				if rate > peak {
					rate = peak
				}
				tick.Reset(time.Second / time.Duration(rate))
				if rate == peak {
					atPeakSince = time.Now()
				}
			}

		case <-report.C:
			fmt.Printf("  rate=%d/s sent=%d ok=%d failed=%d\n",
				rate, s.sent.Load(), s.ok.Load(), s.failed.Load())
			if !atPeakSince.IsZero() && time.Since(atPeakSince) >= hold {
				close(jobs)
				wg.Wait()
				printSummary(&s)
				return nil
			}
		}
	}
}

func printSummary(s *stats) {
	sent, ok, failed := s.sent.Load(), s.ok.Load(), s.failed.Load()
	fmt.Printf("loadgen done: sent=%d ok=%d failed=%d\n", sent, ok, failed)
	if sent > 0 {
		fmt.Printf("  success rate: %.1f%%\n", float64(ok)/float64(sent)*100)
	}
}
