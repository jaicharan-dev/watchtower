package batcher

import (
	"testing"
	"time"
	"watchtower/pkg/model"
	"watchtower/pkg/storage"
)

func TestBatcherFlushOnSize(t *testing.T) {
	store := storage.NewMemoryStorage(100)
	defer store.Close()

	// BatchSize of 3, flush interval 10 seconds
	b := NewBatcher(Config{
		BatchSize:     3,
		FlushInterval: 10 * time.Second,
		QueueCapacity: 50,
	}, store)
	defer b.Stop()

	// Submit 2 metrics (should NOT flush yet)
	b.Submit(model.Metric{Name: "test_m1", Type: "counter", Value: 1})
	b.Submit(model.Metric{Name: "test_m2", Type: "counter", Value: 2})

	time.Sleep(50 * time.Millisecond)
	if count := store.Count(); count != 0 {
		t.Fatalf("expected 0 metrics in store before reaching batch size, got %d", count)
	}

	// Submit 3rd metric (should trigger immediate flush!)
	b.Submit(model.Metric{Name: "test_m3", Type: "counter", Value: 3})

	time.Sleep(50 * time.Millisecond)
	if count := store.Count(); count != 3 {
		t.Fatalf("expected 3 metrics flushed to store, got %d", count)
	}
}

func TestBatcherFlushOnInterval(t *testing.T) {
	store := storage.NewMemoryStorage(100)
	defer store.Close()

	// BatchSize of 100, flush interval 100ms
	b := NewBatcher(Config{
		BatchSize:     100,
		FlushInterval: 100 * time.Millisecond,
		QueueCapacity: 50,
	}, store)
	defer b.Stop()

	// Submit only 1 metric (less than BatchSize)
	b.Submit(model.Metric{Name: "test_ticker", Type: "gauge", Value: 99})

	// Wait for ticker interval to elapse
	time.Sleep(200 * time.Millisecond)

	if count := store.Count(); count != 1 {
		t.Fatalf("expected ticker to flush partial batch (1 metric), got %d", count)
	}
}
