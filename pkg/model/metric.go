package model

import "time"

// Metric represents an observability event.
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
	Timestamp time.Time         `json:"timestamp"` // Event timestamp (UTC)
}
