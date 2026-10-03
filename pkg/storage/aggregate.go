package storage

import (
	"context"
	"fmt"
	"math"
	"slices"
	"time"
)

// Stats holds computed statistical indicators for a metric series.
type Stats struct {
	Count float64 `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Avg   float64 `json:"avg"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
}

// AggregateOptions defines criteria for calculating aggregated metrics.
type AggregateOptions struct {
	MetricName string
	From       time.Time
	To         time.Time
	GroupByTag string            // Optional tag to group by (e.g. "endpoint", "status")
	Tags       map[string]string // Optional filter tags
}

// AggregateGroup contains statistics for a specific tag group.
type AggregateGroup struct {
	GroupValue string `json:"group_value"` // e.g. "/api/login"
	Stats      Stats  `json:"stats"`
}

// AggregateResult represents the complete aggregation response.
type AggregateResult struct {
	MetricName string           `json:"metric_name"`
	From       time.Time        `json:"from"`
	To         time.Time        `json:"to"`
	GroupBy    string           `json:"group_by,omitempty"`
	Overall    Stats            `json:"overall"`
	Groups     []AggregateGroup `json:"groups,omitempty"`
}

// CalculateStats computes statistical summary (min, max, avg, percentiles) from raw numbers.
func CalculateStats(values []float64) Stats {
	if len(values) == 0 {
		return Stats{}
	}

	// Copy to avoid mutating original slice, then sort ascending
	sorted := make([]float64, len(values))
	copy(sorted, values)
	slices.Sort(sorted)

	var sum float64
	min := sorted[0]
	max := sorted[len(sorted)-1]

	for _, v := range sorted {
		sum += v
	}

	avg := sum / float64(len(sorted))

	return Stats{
		Count: float64(len(sorted)),
		Sum:   math.Round(sum*100) / 100,
		Min:   math.Round(min*100) / 100,
		Max:   math.Round(max*100) / 100,
		Avg:   math.Round(avg*100) / 100,
		P50:   percentile(sorted, 0.50),
		P90:   percentile(sorted, 0.90),
		P95:   percentile(sorted, 0.95),
		P99:   percentile(sorted, 0.99),
	}
}

// percentile calculates the nearest-rank percentile value from a sorted slice.
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	// Nearest-rank formula: ceil(p * N) - 1
	rank := int(math.Ceil(p*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return math.Round(sorted[rank]*100) / 100
}

// Aggregate performs filtering, grouping, and statistical calculation over in-memory metrics.
func (m *MemoryStorage) Aggregate(ctx context.Context, opts AggregateOptions) (*AggregateResult, error) {
	if opts.MetricName == "" {
		return nil, fmt.Errorf("metric_name is required")
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var allValues []float64
	groupValues := make(map[string][]float64)

	for _, metric := range m.metrics {
		if metric.Name != opts.MetricName {
			continue
		}

		if !opts.From.IsZero() && metric.Timestamp.Before(opts.From) {
			continue
		}
		if !opts.To.IsZero() && metric.Timestamp.After(opts.To) {
			continue
		}

		// Filter tags if provided
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

		allValues = append(allValues, metric.Value)

		// Group by tag if requested
		if opts.GroupByTag != "" {
			tagVal, ok := metric.Tags[opts.GroupByTag]
			if !ok {
				tagVal = "unknown"
			}
			groupValues[tagVal] = append(groupValues[tagVal], metric.Value)
		}
	}

	res := &AggregateResult{
		MetricName: opts.MetricName,
		From:       opts.From,
		To:         opts.To,
		GroupBy:    opts.GroupByTag,
		Overall:    CalculateStats(allValues),
	}

	if opts.GroupByTag != "" {
		for groupKey, vals := range groupValues {
			res.Groups = append(res.Groups, AggregateGroup{
				GroupValue: groupKey,
				Stats:      CalculateStats(vals),
			})
		}
	}

	return res, nil
}
