package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"watchtower/pkg/batcher"
	"watchtower/pkg/storage"
)

func TestMetricsDecorator(t *testing.T) {
	store := storage.NewMemoryStorage(100)
	defer store.Close()

	b := batcher.NewBatcher(batcher.Config{
		BatchSize:     10,
		FlushInterval: 50 * time.Millisecond,
		QueueCapacity: 50,
	}, store)
	defer b.Stop()

	// Simple dummy handler that returns 201 Created
	dummyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("ok"))
	})

	decorated := MetricsDecorator(b, dummyHandler)

	req := httptest.NewRequest(http.MethodGet, "/test-route", nil)
	rec := httptest.NewRecorder()

	decorated.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d", rec.Code)
	}

	// Wait for batcher ticker to flush dogfooding metrics
	time.Sleep(100 * time.Millisecond)

	if count := store.Count(); count < 2 {
		t.Fatalf("expected at least 2 internal metrics emitted, got %d", count)
	}
}
