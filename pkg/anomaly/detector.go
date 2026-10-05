package anomaly

import (
	"context"
	"fmt"
	"math"
	"time"
	"watchtower/pkg/model"
	"watchtower/pkg/storage"
)

// AnomalyPoint represents a detected statistical anomaly.
type AnomalyPoint struct {
	MetricName string    `json:"metric_name"`
	TagKey     string    `json:"tag_key,omitempty"`
	TagValue   string    `json:"tag_value,omitempty"`
	Value      float64   `json:"value"`
	Mean       float64   `json:"mean"`
	StdDev     float64   `json:"std_dev"`
	ZScore     float64   `json:"z_score"`
	Severity   string    `json:"severity"` // "warning" (|Z| >= 2.0), "critical" (|Z| >= 3.0)
	Timestamp  time.Time `json:"timestamp"`
	Message    string    `json:"message"`
}

// GroupBaseline contains the baseline statistics for a particular group (e.g. endpoint).
type GroupBaseline struct {
	GroupValue string         `json:"group_value"`
	SampleSize int            `json:"sample_size"`
	Mean       float64        `json:"mean"`
	StdDev     float64        `json:"std_dev"`
	UpperLimit float64        `json:"upper_limit"` // Mean + (sigma * StdDev)
	Anomalies  []AnomalyPoint `json:"anomalies"`
}

// DetectionResult holds the full report of anomaly detection.
type DetectionResult struct {
	MetricName      string          `json:"metric_name"`
	Window          string          `json:"window"`
	SigmaMultiplier float64         `json:"sigma_multiplier"`
	TotalAnomalies  int             `json:"total_anomalies"`
	Groups          []GroupBaseline `json:"groups"`
}

// CalculateMeanAndStdDev calculates the arithmetic mean and sample standard deviation.
func CalculateMeanAndStdDev(values []float64) (mean float64, stdDev float64) {
	n := len(values)
	if n == 0 {
		return 0, 0
	}
	if n == 1 {
		return values[0], 0
	}

	var sum float64
	for _, v := range values {
		sum += v
	}
	mean = sum / float64(n)

	var varianceSum float64
	for _, v := range values {
		diff := v - mean
		varianceSum += diff * diff
	}

	// Sample variance (Bessel's correction: N - 1)
	variance := varianceSum / float64(n-1)
	stdDev = math.Sqrt(variance)

	return math.Round(mean*100) / 100, math.Round(stdDev*100) / 100
}

// Detector runs statistical anomaly detection over stored time-series data.
type Detector struct {
	store storage.Storage
}

func NewDetector(store storage.Storage) *Detector {
	return &Detector{store: store}
}

// Analyze evaluates recent metrics and identifies anomalies exceeding sigmaThreshold standard deviations.
func (d *Detector) Analyze(ctx context.Context, metricName string, groupByTag string, window time.Duration, sigmaThreshold float64) (*DetectionResult, error) {
	if metricName == "" {
		return nil, fmt.Errorf("metric_name is required")
	}
	if sigmaThreshold <= 0 {
		sigmaThreshold = 2.5 // Default to 2.5 sigma
	}
	if window <= 0 {
		window = 5 * time.Minute
	}

	from := time.Now().UTC().Add(-window)

	// Fetch raw points matching criteria
	rawPoints, err := d.store.Query(ctx, storage.QueryOptions{
		MetricName: metricName,
		From:       from,
		Limit:      5000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query storage for anomaly analysis: %w", err)
	}

	// Group points by tag value
	grouped := make(map[string][]model.Metric)
	for _, pt := range rawPoints {
		groupKey := "overall"
		if groupByTag != "" {
			if val, ok := pt.Tags[groupByTag]; ok {
				groupKey = val
			} else {
				groupKey = "unknown"
			}
		}
		grouped[groupKey] = append(grouped[groupKey], pt)
	}

	report := &DetectionResult{
		MetricName:      metricName,
		Window:          window.String(),
		SigmaMultiplier: sigmaThreshold,
		Groups:          make([]GroupBaseline, 0, len(grouped)),
	}

	for groupKey, points := range grouped {
		if len(points) < 5 {
			// Need at least 5 points to establish a meaningful statistical baseline
			continue
		}

		values := make([]float64, len(points))
		for i, pt := range points {
			values[i] = pt.Value
		}

		mean, stdDev := CalculateMeanAndStdDev(values)
		upperLimit := math.Round((mean+(sigmaThreshold*stdDev))*100) / 100

		baseline := GroupBaseline{
			GroupValue: groupKey,
			SampleSize: len(points),
			Mean:       mean,
			StdDev:     stdDev,
			UpperLimit: upperLimit,
			Anomalies:  make([]AnomalyPoint, 0),
		}

		// Check each point against the baseline (Z-Score calculation)
		if stdDev > 0 {
			for _, pt := range points {
				zScore := (pt.Value - mean) / stdDev
				if zScore >= sigmaThreshold {
					severity := "warning"
					if zScore >= 3.0 {
						severity = "critical"
					}

					baseline.Anomalies = append(baseline.Anomalies, AnomalyPoint{
						MetricName: metricName,
						TagKey:     groupByTag,
						TagValue:   groupKey,
						Value:      math.Round(pt.Value*100) / 100,
						Mean:       mean,
						StdDev:     stdDev,
						ZScore:     math.Round(zScore*100) / 100,
						Severity:   severity,
						Timestamp:  pt.Timestamp,
						Message: fmt.Sprintf("Value %.1fms is %.1f standard deviations above normal (mean: %.1fms, limit: %.1fms)",
							pt.Value, zScore, mean, upperLimit),
					})
				}
			}
		}

		report.TotalAnomalies += len(baseline.Anomalies)
		report.Groups = append(report.Groups, baseline)
	}

	return report, nil
}
