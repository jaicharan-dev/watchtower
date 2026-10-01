package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Metric represents an observability event.
// In observability systems (like Prometheus or Datadog), metrics have:
// - Name: what we are measuring (e.g., "http_requests_total", "http_request_duration_ms")
// - Type: "counter" (monotonically increasing) or "gauge" (fluctuating value)
// - Value: the numerical measurement
// - Tags: key-value labels for filtering (e.g., endpoint="/pay", status="500")
// - Timestamp: when the event occurred
type Metric struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"`      // "counter" or "gauge"
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags"`      // Key-value metadata
	Timestamp time.Time         `json:"timestamp"` // Defaults to time.Now() if empty
}

func main() {
	http.HandleFunc("/ingest", handleIngest)

	fmt.Println("Watchtower Ingestion API listening on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("Server failed to start:", err)
	}
}

func handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
		return
	}

	var m Metric
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Basic validation
	if m.Name == "" || m.Type == "" {
		http.Error(w, "Metric 'name' and 'type' are required", http.StatusBadRequest)
		return
	}

	// Default timestamp to current UTC time if not supplied
	if m.Timestamp.IsZero() {
		m.Timestamp = time.Now().UTC()
	}

	// Log the incoming metric for visibility
	fmt.Printf("[METRIC] %s | Type: %-7s | Value: %8.2f | Tags: %v | Time: %s\n",
		m.Name, m.Type, m.Value, m.Tags, m.Timestamp.Format(time.RFC3339))

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintf(w, "Metric '%s' recorded\n", m.Name)
}
