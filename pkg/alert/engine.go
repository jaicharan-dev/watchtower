package alert

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
	"watchtower/pkg/storage"
)

// Engine schedules and executes alert evaluations against the storage layer.
type Engine struct {
	store     storage.Storage
	mu        sync.RWMutex
	rules     map[string]*RuleState
	notifiers []Notifier
	interval  time.Duration
	stopChan  chan struct{}
	wg        sync.WaitGroup
}

// NewEngine initializes the Alert Engine.
func NewEngine(store storage.Storage, evalInterval time.Duration) *Engine {
	if evalInterval <= 0 {
		evalInterval = 5 * time.Second
	}
	return &Engine{
		store:     store,
		rules:     make(map[string]*RuleState),
		notifiers: make([]Notifier, 0),
		interval:  evalInterval,
		stopChan:  make(chan struct{}),
	}
}

// RegisterNotifier adds a notification handler (Observer Pattern).
func (e *Engine) RegisterNotifier(n Notifier) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.notifiers = append(e.notifiers, n)
}

// RegisterRule registers or updates an alert rule.
func (e *Engine) RegisterRule(r Rule) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.rules[r.ID] = &RuleState{
		Rule:         r,
		CurrentState: StateOK,
	}
}

// GetStates returns a snapshot of all alert rule statuses.
func (e *Engine) GetStates() []RuleState {
	e.mu.RLock()
	defer e.mu.RUnlock()

	states := make([]RuleState, 0, len(e.rules))
	for _, s := range e.rules {
		states = append(states, *s)
	}
	return states
}

// Start launches the background alert scheduler.
func (e *Engine) Start(ctx context.Context) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ticker := time.NewTicker(e.interval)
		defer ticker.Stop()

		for {
			select {
			case <-e.stopChan:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				e.EvaluateOnce(ctx)
			}
		}
	}()
}

// Stop cleanly terminates the scheduler.
func (e *Engine) Stop() {
	close(e.stopChan)
	e.wg.Wait()
}

// EvaluateOnce executes an evaluation pass across all registered rules.
// Exposed publicly for testing and ad-hoc execution.
func (e *Engine) EvaluateOnce(ctx context.Context) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now().UTC()

	for _, state := range e.rules {
		r := state.Rule
		from := now.Add(-r.Window)
		if r.Window <= 0 {
			from = now.Add(-1 * time.Minute)
		}

		aggResult, err := e.store.Aggregate(ctx, storage.AggregateOptions{
			MetricName: r.MetricName,
			From:       from,
			To:         now,
			GroupByTag: r.GroupByTag,
		})
		if err != nil {
			fmt.Printf("[ALERT ENGINE] Error evaluating rule '%s': %v\n", r.ID, err)
			continue
		}

		// Find relevant stats (either overall or matching TargetGroup)
		stats := aggResult.Overall
		if r.GroupByTag != "" && r.TargetGroup != "" {
			found := false
			for _, g := range aggResult.Groups {
				if g.GroupValue == r.TargetGroup {
					stats = g.Stats
					found = true
					break
				}
			}
			if !found {
				// No data for target group yet, treat as 0
				stats = storage.Stats{}
			}
		}

		currentVal := extractStat(stats, r.Stat)
		state.CurrentValue = currentVal
		state.LastEvaluatedAt = now

		breached := checkCondition(currentVal, r.Operator, r.Threshold)

		// State Machine Transitions
		switch state.CurrentState {
		case StateOK:
			if breached {
				if r.ForDuration <= 0 {
					// Immediately fires if no grace duration configured
					e.transition(ctx, state, StateFiring, currentVal, now)
				} else {
					state.CurrentState = StatePending
					state.FirstBreachedAt = now
				}
			}

		case StatePending:
			if !breached {
				// Blip recovered before duration was reached!
				state.CurrentState = StateOK
				state.FirstBreachedAt = time.Time{}
			} else if now.Sub(state.FirstBreachedAt) >= r.ForDuration {
				// Sustained breach confirmed!
				e.transition(ctx, state, StateFiring, currentVal, now)
			}

		case StateFiring:
			if !breached {
				// Alert recovered!
				e.transition(ctx, state, StateOK, currentVal, now)
				state.FirstBreachedAt = time.Time{}
			}
			// If still breached, do NOT send duplicate spam notifications
		}
	}
}

func (e *Engine) transition(ctx context.Context, state *RuleState, next State, val float64, now time.Time) {
	prev := state.CurrentState
	state.CurrentState = next

	if next == StateFiring {
		state.LastFiredAt = now
	}

	msg := fmt.Sprintf("Metric '%s' value %.2f %s threshold %.2f",
		state.Rule.MetricName, val, state.Rule.Operator, state.Rule.Threshold)
	if state.Rule.TargetGroup != "" {
		msg += fmt.Sprintf(" for %s=%s", state.Rule.GroupByTag, state.Rule.TargetGroup)
	}

	event := AlertEvent{
		Rule:      state.Rule,
		Previous:  prev,
		Current:   next,
		Value:     val,
		Threshold: state.Rule.Threshold,
		Timestamp: now,
		Message:   msg,
	}

	for _, notifier := range e.notifiers {
		if err := notifier.Notify(ctx, event); err != nil {
			fmt.Printf("[NOTIFIER ERROR] Failed to deliver alert: %v\n", err)
		}
	}
}

func extractStat(stats storage.Stats, stat string) float64 {
	switch strings.ToLower(stat) {
	case "p50":
		return stats.P50
	case "p90":
		return stats.P90
	case "p95":
		return stats.P95
	case "p99":
		return stats.P99
	case "avg":
		return stats.Avg
	case "max":
		return stats.Max
	case "min":
		return stats.Min
	case "count":
		return stats.Count
	case "sum":
		return stats.Sum
	default:
		return stats.Avg
	}
}

func checkCondition(val float64, op string, threshold float64) bool {
	switch op {
	case ">":
		return val > threshold
	case ">=":
		return val >= threshold
	case "<":
		return val < threshold
	case "<=":
		return val <= threshold
	case "==":
		return val == threshold
	case "!=":
		return val != threshold
	default:
		return val > threshold
	}
}
