package alert

import (
	"time"
)

// State represents the current lifecycle of an alert rule.
type State string

const (
	StateOK      State = "OK"      // Metric is within healthy thresholds
	StatePending State = "PENDING" // Threshold breached, awaiting duration confirmation
	StateFiring  State = "FIRING"  // Sustained violation confirmed, alert actively triggered
)

// Rule defines an alerting condition.
type Rule struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	MetricName  string        `json:"metric_name"`
	GroupByTag  string        `json:"group_by_tag,omitempty"`
	TargetGroup string        `json:"target_group,omitempty"` // e.g. "/api/checkout"
	Stat        string        `json:"stat"`                   // "avg", "p50", "p95", "p99", "max", "count"
	Operator    string        `json:"operator"`               // ">", ">=", "<", "<="
	Threshold   float64       `json:"threshold"`
	Window      time.Duration `json:"window"`                 // Lookback aggregation window (e.g. 1m)
	ForDuration time.Duration `json:"for_duration"`           // Time violation must persist before firing
}

// RuleState tracks the evaluation status and state machine for a single rule.
type RuleState struct {
	Rule            Rule      `json:"rule"`
	CurrentState    State     `json:"current_state"`
	CurrentValue    float64   `json:"current_value"`
	FirstBreachedAt time.Time `json:"first_breached_at,omitempty"`
	LastEvaluatedAt time.Time `json:"last_evaluated_at"`
	LastFiredAt     time.Time `json:"last_fired_at,omitempty"`
}

// AlertEvent represents a state transition event delivered to Notifiers (Observer pattern).
type AlertEvent struct {
	Rule      Rule      `json:"rule"`
	Previous  State     `json:"previous_state"`
	Current   State     `json:"current_state"`
	Value     float64   `json:"value"`
	Threshold float64   `json:"threshold"`
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
}
