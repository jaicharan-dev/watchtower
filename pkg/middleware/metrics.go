package middleware

import (
	"net/http"
	"strconv"
	"time"
	"watchtower/pkg/batcher"
	"watchtower/pkg/model"
)

// responseWriterInterceptor wraps http.ResponseWriter to capture the HTTP status code.
type responseWriterInterceptor struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriterInterceptor) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// MetricsDecorator implements the Decorator pattern (SOLID).
// It wraps an http.Handler to automatically record Watchtower's own HTTP performance metrics (dogfooding).
func MetricsDecorator(b *batcher.Batcher, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		interceptor := &responseWriterInterceptor{
			ResponseWriter: w,
			statusCode:     http.StatusOK, // Default if WriteHeader is not explicitly called
		}

		// Execute wrapped handler
		next.ServeHTTP(interceptor, r)

		durationMs := float64(time.Since(start).Nanoseconds()) / 1e6
		now := time.Now().UTC()
		statusStr := strconv.Itoa(interceptor.statusCode)

		// Dogfooding Metric 1: Watchtower API request duration
		b.Submit(model.Metric{
			Name:      "watchtower_internal_latency_ms",
			Type:      "gauge",
			Value:     durationMs,
			Timestamp: now,
			Tags: map[string]string{
				"path":   r.URL.Path,
				"method": r.Method,
				"status": statusStr,
			},
		})

		// Dogfooding Metric 2: Watchtower API request count
		b.Submit(model.Metric{
			Name:      "watchtower_internal_requests_total",
			Type:      "counter",
			Value:     1.0,
			Timestamp: now,
			Tags: map[string]string{
				"path":   r.URL.Path,
				"method": r.Method,
				"status": statusStr,
			},
		})
	})
}
