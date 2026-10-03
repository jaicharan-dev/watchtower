package storage

import (
	"context"
	"sync"
	"watchtower/pkg/model"
)

// MemoryStorage is a thread-safe, in-memory implementation of the Storage interface.
// It keeps metrics in a slice with a maximum capacity to prevent unbounded memory growth.
type MemoryStorage struct {
	mu       sync.RWMutex
	metrics  []model.Metric
	maxItems int
}

// NewMemoryStorage creates an in-memory storage engine with a given maximum capacity.
func NewMemoryStorage(maxItems int) *MemoryStorage {
	if maxItems <= 0 {
		maxItems = 100000
	}
	return &MemoryStorage{
		metrics:  make([]model.Metric, 0, 1024),
		maxItems: maxItems,
	}
}

// InsertBatch appends a batch of metrics to the in-memory store under a write lock.
func (m *MemoryStorage) InsertBatch(ctx context.Context, batch []model.Metric) error {
	if len(batch) == 0 {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.metrics = append(m.metrics, batch...)

	// Evict oldest metrics if we exceed maxItems (simple retention)
	if len(m.metrics) > m.maxItems {
		excess := len(m.metrics) - m.maxItems
		m.metrics = m.metrics[excess:]
	}

	return nil
}

// Query filters in-memory metrics based on QueryOptions under a read lock.
func (m *MemoryStorage) Query(ctx context.Context, opts QueryOptions) ([]model.Metric, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []model.Metric

	for _, metric := range m.metrics {
		// 1. Filter by Name (if specified)
		if opts.MetricName != "" && metric.Name != opts.MetricName {
			continue
		}

		// 2. Filter by Time range (if specified)
		if !opts.From.IsZero() && metric.Timestamp.Before(opts.From) {
			continue
		}
		if !opts.To.IsZero() && metric.Timestamp.After(opts.To) {
			continue
		}

		// 3. Filter by Tags (must match all provided tags)
		matchesTags := true
		for k, v := range opts.Tags {
			if metric.Tags[k] != v {
				matchesTags = false
				break
			}
		}
		if !matchesTags {
			continue
		}

		result = append(result, metric)

		// 4. Respect Limit
		if opts.Limit > 0 && len(result) >= opts.Limit {
			break
		}
	}

	return result, nil
}

// Close satisfies the Storage interface.
func (m *MemoryStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics = nil
	return nil
}

// Count returns the total number of metrics currently held in memory.
func (m *MemoryStorage) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.metrics)
}
