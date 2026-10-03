package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Metric matches the ingestion API format.
type Metric struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"` // "counter" or "gauge"
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`
	Timestamp time.Time         `json:"timestamp"`
}

// SimulatedEndpoint represents an API route with realistic behaviors.
type SimulatedEndpoint struct {
	Path        string
	Method      string
	BaseLatency float64 // in milliseconds
}

var endpoints = []SimulatedEndpoint{
	{Path: "/api/products", Method: "GET", BaseLatency: 45.0},
	{Path: "/api/search", Method: "GET", BaseLatency: 80.0},
	{Path: "/api/login", Method: "POST", BaseLatency: 120.0},
	{Path: "/api/checkout", Method: "POST", BaseLatency: 250.0},
}

func main() {
	// 1. Command-line flags for easy configuration
	targetURL := flag.String("target", "http://localhost:8080/ingest", "Target ingestion API URL")
	concurrency := flag.Int("users", 3, "Number of concurrent simulated users (goroutines)")
	intervalMs := flag.Int("interval", 800, "Delay in milliseconds between requests per user")
	errorRate := flag.Float64("error-rate", 0.15, "Fraction of requests that should fail (0.0 to 1.0, e.g. 0.15 = 15%)")
	flag.Parse()

	fmt.Println("==================================================")
	fmt.Println("🚀 Watchtower Synthetic Load Generator")
	fmt.Printf("   Target:       %s\n", *targetURL)
	fmt.Printf("   Users:        %d goroutines\n", *concurrency)
	fmt.Printf("   Interval:     %d ms\n", *intervalMs)
	fmt.Printf("   Error Rate:   %.1f%%\n", *errorRate*100)
	fmt.Println("   Press Ctrl+C to stop gracefully")
	fmt.Println("==================================================")

	// 2. Set up context that cancels when the user presses Ctrl+C
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n🛑 Stopping load generator gracefully...")
		cancel()
	}()

	// 3. HTTP client reused across requests (best practice in Go)
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	// 4. WaitGroup to wait for all simulated users to finish
	var wg sync.WaitGroup

	// 5. Launch a goroutine for each simulated user
	for userID := 1; userID <= *concurrency; userID++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			runSimulatedUser(ctx, id, *targetURL, *intervalMs, *errorRate, client)
		}(userID)
	}

	// Wait until all user goroutines complete
	wg.Wait()
	fmt.Println("All simulated users stopped.")
}

func runSimulatedUser(ctx context.Context, userID int, targetURL string, intervalMs int, errorRate float64, client *http.Client) {
	ticker := time.NewTicker(time.Duration(intervalMs) * time.Millisecond)
	defer ticker.Stop()

	// Use local random generator to prevent lock contention
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(userID)))

	for {
		select {
		case <-ctx.Done():
			// Context canceled (Ctrl+C), exit goroutine
			return
		case <-ticker.C:
			// Pick a random endpoint
			endpoint := endpoints[r.Intn(len(endpoints))]

			// Determine status code based on error rate
			isError := r.Float64() < errorRate
			statusCode := "200"
			if isError {
				if r.Float64() < 0.7 {
					statusCode = "500"
				} else {
					statusCode = "503"
				}
			}

			// Calculate latency with jitter and occasional latency spikes
			latency := endpoint.BaseLatency + r.Float64()*30.0
			if r.Float64() < 0.10 { // 10% chance of a latency spike
				latency += 400.0 + r.Float64()*600.0
			}

			now := time.Now().UTC()

			// 1. Metric: Request counter
			counterMetric := Metric{
				Name:      "http_requests_total",
				Type:      "counter",
				Value:     1.0,
				Timestamp: now,
				Tags: map[string]string{
					"endpoint": endpoint.Path,
					"method":   endpoint.Method,
					"status":   statusCode,
				},
			}

			// 2. Metric: Latency gauge
			latencyMetric := Metric{
				Name:      "http_request_duration_ms",
				Type:      "gauge",
				Value:     latency,
				Timestamp: now,
				Tags: map[string]string{
					"endpoint": endpoint.Path,
					"status":   statusCode,
				},
			}

			// Send both metrics
			sendMetric(client, targetURL, counterMetric)
			sendMetric(client, targetURL, latencyMetric)

			fmt.Printf("[User %d] %s %-15s -> status=%s latency=%.1fms\n",
				userID, endpoint.Method, endpoint.Path, statusCode, latency)
		}
	}
}

func sendMetric(client *http.Client, url string, m Metric) {
	payload, err := json.Marshal(m)
	if err != nil {
		fmt.Printf("Error marshaling metric: %v\n", err)
		return
	}

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		fmt.Printf("Failed to send metric: %v\n", err)
		return
	}
	defer resp.Body.Close()
}
