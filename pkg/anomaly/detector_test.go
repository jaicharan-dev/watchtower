package anomaly

import (
	"context"
	"testing"
	"time"
	"watchtower/pkg/model"
	"watchtower/pkg/storage"
)

func TestCalculateMeanAndStdDev(t *testing.T) {
	// Sample data: [10, 12, 23, 23, 16, 23, 21, 16]
	// Mean = 18.0
	// Sample Variance = 27.428 -> StdDev = ~5.24
	values := []float64{10, 12, 23, 23, 16, 23, 21, 16}
	mean, stdDev := CalculateMeanAndStdDev(values)

	if mean != 18.0 {
		t.Errorf("expected mean 18.0, got %f", mean)
	}
	if stdDev < 5.2 || stdDev > 5.3 {
		t.Errorf("expected stdDev ~5.24, got %f", stdDev)
	}
}

func TestDetectorAnalyze(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStorage(100)
	defer store.Close()

	detector := NewDetector(store)

	// Create 20 baseline points around 50ms (normal noise: 45ms to 55ms)
	now := time.Now().UTC()
	var metrics []model.Metric

	for i := 0; i < 20; i++ {
		val := 50.0 + float64(i%5) // 50, 51, 52, 53, 54
		metrics = append(metrics, model.Metric{
			Name:      "http_request_duration_ms",
			Type:      "gauge",
			Value:     val,
			Tags:      map[string]string{"endpoint": "/api/products"},
			Timestamp: now.Add(-time.Duration(20-i) * time.Second),
		})
	}

	// Inject 1 severe spike at 500ms (10x higher than normal)
	metrics = append(metrics, model.Metric{
		Name:      "http_request_duration_ms",
		Type:      "gauge",
		Value:     500.0,
		Tags:      map[string]string{"endpoint": "/api/products"},
		Timestamp: now,
	})

	if err := store.InsertBatch(ctx, metrics); err != nil {
		t.Fatalf("InsertBatch failed: %v", err)
	}

	result, err := detector.Analyze(ctx, "http_request_duration_ms", "endpoint", 5*time.Minute, 2.5)
	if err != nil {
		t.Fatalf("Analyze failed: %v", err)
	}

	if result.TotalAnomalies == 0 {
		t.Fatalf("expected anomaly to be detected, got 0")
	}

	if len(result.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(result.Groups))
	}

	group := result.Groups[0]
	if len(group.Anomalies) != 1 {
		t.Fatalf("expected 1 anomaly in products group, got %d", len(group.Anomalies))
	}

	anomaly := group.Anomalies[0]
	if anomaly.Value != 500.0 {
		t.Errorf("expected anomaly value 500.0, got %f", anomaly.Value)
	}
	if anomaly.ZScore < 2.5 {
		t.Errorf("expected Z-score >= 2.5, got %f", anomaly.ZScore)
	}
}
