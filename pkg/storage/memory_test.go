package storage

import (
	"context"
	"testing"
	"time"
	"watchtower/pkg/model"
)

// TestMemoryStorage demonstrates standard Go table-driven tests.
func TestMemoryStorage(t *testing.T) {
	store := NewMemoryStorage(100)
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	metrics := []model.Metric{
		{
			Name:      "http_requests_total",
			Type:      "counter",
			Value:     1.0,
			Tags:      map[string]string{"endpoint": "/api/login", "status": "200"},
			Timestamp: now.Add(-2 * time.Minute),
		},
		{
			Name:      "http_requests_total",
			Type:      "counter",
			Value:     1.0,
			Tags:      map[string]string{"endpoint": "/api/checkout", "status": "500"},
			Timestamp: now.Add(-1 * time.Minute),
		},
		{
			Name:      "http_request_duration_ms",
			Type:      "gauge",
			Value:     142.5,
			Tags:      map[string]string{"endpoint": "/api/login"},
			Timestamp: now,
		},
	}

	// 1. Test InsertBatch
	if err := store.InsertBatch(ctx, metrics); err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}

	if count := store.Count(); count != 3 {
		t.Errorf("expected 3 items in store, got %d", count)
	}

	// 2. Table-driven test cases for Query
	testCases := []struct {
		name          string
		query         QueryOptions
		expectedCount int
	}{
		{
			name:          "Query by metric name",
			query:         QueryOptions{MetricName: "http_requests_total"},
			expectedCount: 2,
		},
		{
			name:          "Query by metric name and tag",
			query:         QueryOptions{MetricName: "http_requests_total", Tags: map[string]string{"status": "500"}},
			expectedCount: 1,
		},
		{
			name:          "Query with limit",
			query:         QueryOptions{Limit: 1},
			expectedCount: 1,
		},
		{
			name:          "Query nonexistent metric",
			query:         QueryOptions{MetricName: "cpu_usage"},
			expectedCount: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			results, err := store.Query(ctx, tc.query)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if len(results) != tc.expectedCount {
				t.Errorf("expected %d results, got %d", tc.expectedCount, len(results))
			}
		})
	}
}
