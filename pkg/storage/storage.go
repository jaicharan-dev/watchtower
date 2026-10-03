package storage

import (
	"context"
	"time"
	"watchtower/pkg/model"
)

// QueryOptions defines filtering criteria for fetching metrics.
type QueryOptions struct {
	MetricName string            // e.g. "http_requests_total"
	From       time.Time         // Start of time range
	To         time.Time         // End of time range
	Tags       map[string]string // Match tags (e.g. endpoint="/api/login")
	Limit      int               // Max records to return
}

// Storage is the abstraction for any persistence backend in Watchtower.
// (Adheres to the Dependency Inversion Principle - SOLID)
type Storage interface {
	// InsertBatch saves a collection of metrics in one atomic/efficient operation.
	InsertBatch(ctx context.Context, metrics []model.Metric) error

	// Query fetches metrics matching the given criteria.
	Query(ctx context.Context, opts QueryOptions) ([]model.Metric, error)

	// Aggregate calculates summary statistics (min, max, avg, percentiles) and group-by aggregations.
	Aggregate(ctx context.Context, opts AggregateOptions) (*AggregateResult, error)

	// Close cleans up connections or resources.
	Close() error
}
