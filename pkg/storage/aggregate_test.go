package storage

import (
	"context"
	"testing"
	"time"
	"watchtower/pkg/model"
)

func TestCalculateStats(t *testing.T) {
	// Test on known numbers 1 to 100
	values := make([]float64, 100)
	for i := 0; i < 100; i++ {
		values[i] = float64(i + 1)
	}

	stats := CalculateStats(values)

	if stats.Count != 100 {
		t.Errorf("expected count 100, got %f", stats.Count)
	}
	if stats.Min != 1 {
		t.Errorf("expected min 1, got %f", stats.Min)
	}
	if stats.Max != 100 {
		t.Errorf("expected max 100, got %f", stats.Max)
	}
	if stats.Avg != 50.5 {
		t.Errorf("expected avg 50.5, got %f", stats.Avg)
	}
	if stats.P50 != 50 {
		t.Errorf("expected p50 50, got %f", stats.P50)
	}
	if stats.P90 != 90 {
		t.Errorf("expected p90 90, got %f", stats.P90)
	}
	if stats.P95 != 95 {
		t.Errorf("expected p95 95, got %f", stats.P95)
	}
	if stats.P99 != 99 {
		t.Errorf("expected p99 99, got %f", stats.P99)
	}
}

func TestMemoryStorageAggregate(t *testing.T) {
	store := NewMemoryStorage(100)
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UTC()

	metrics := []model.Metric{
		{
			Name:      "http_request_duration_ms",
			Type:      "gauge",
			Value:     50.0,
			Tags:      map[string]string{"endpoint": "/api/login"},
			Timestamp: now.Add(-10 * time.Second),
		},
		{
			Name:      "http_request_duration_ms",
			Type:      "gauge",
			Value:     150.0,
			Tags:      map[string]string{"endpoint": "/api/login"},
			Timestamp: now.Add(-5 * time.Second),
		},
		{
			Name:      "http_request_duration_ms",
			Type:      "gauge",
			Value:     300.0,
			Tags:      map[string]string{"endpoint": "/api/checkout"},
			Timestamp: now,
		},
	}

	if err := store.InsertBatch(ctx, metrics); err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}

	// 1. Test Overall Aggregation
	res, err := store.Aggregate(ctx, AggregateOptions{
		MetricName: "http_request_duration_ms",
	})
	if err != nil {
		t.Fatalf("Aggregate failed: %v", err)
	}

	if res.Overall.Count != 3 {
		t.Errorf("expected overall count 3, got %f", res.Overall.Count)
	}
	if res.Overall.Min != 50 {
		t.Errorf("expected min 50, got %f", res.Overall.Min)
	}
	if res.Overall.Max != 300 {
		t.Errorf("expected max 300, got %f", res.Overall.Max)
	}
	if res.Overall.Avg != 166.67 {
		t.Errorf("expected avg 166.67, got %f", res.Overall.Avg)
	}

	// 2. Test Group By Tag ("endpoint")
	groupRes, err := store.Aggregate(ctx, AggregateOptions{
		MetricName: "http_request_duration_ms",
		GroupByTag: "endpoint",
	})
	if err != nil {
		t.Fatalf("Group by Aggregate failed: %v", err)
	}

	if len(groupRes.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groupRes.Groups))
	}

	for _, g := range groupRes.Groups {
		if g.GroupValue == "/api/login" {
			if g.Stats.Count != 2 {
				t.Errorf("expected login count 2, got %f", g.Stats.Count)
			}
			if g.Stats.Avg != 100 {
				t.Errorf("expected login avg 100, got %f", g.Stats.Avg)
			}
		} else if g.GroupValue == "/api/checkout" {
			if g.Stats.Count != 1 {
				t.Errorf("expected checkout count 1, got %f", g.Stats.Count)
			}
		}
	}
}
