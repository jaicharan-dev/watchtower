package alert

import (
	"context"
	"sync"
	"testing"
	"time"
	"watchtower/pkg/model"
	"watchtower/pkg/storage"
)

// MockNotifier captures events for unit test assertions.
type MockNotifier struct {
	mu     sync.Mutex
	events []AlertEvent
}

func (m *MockNotifier) Notify(ctx context.Context, event AlertEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

func (m *MockNotifier) GetEvents() []AlertEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]AlertEvent, len(m.events))
	copy(copied, m.events)
	return copied
}

func TestAlertStateMachine(t *testing.T) {
	ctx := context.Background()
	store := storage.NewMemoryStorage(100)
	defer store.Close()

	engine := NewEngine(store, 1*time.Second)
	notifier := &MockNotifier{}
	engine.RegisterNotifier(notifier)

	// Rule: P95 latency > 300ms for at least 100ms before firing
	rule := Rule{
		ID:          "rule-checkout-p95",
		Name:        "Checkout Latency",
		MetricName:  "http_request_duration_ms",
		Stat:        "p95",
		Operator:    ">",
		Threshold:   300.0,
		Window:      1 * time.Minute,
		ForDuration: 50 * time.Millisecond,
	}
	engine.RegisterRule(rule)

	// Pass 1: Normal metrics (P95 is 100ms) -> State should be OK
	store.InsertBatch(ctx, []model.Metric{
		{Name: "http_request_duration_ms", Value: 100.0, Timestamp: time.Now().UTC()},
	})
	engine.EvaluateOnce(ctx)

	states := engine.GetStates()
	if states[0].CurrentState != StateOK {
		t.Fatalf("expected state OK, got %s", states[0].CurrentState)
	}
	if len(notifier.GetEvents()) != 0 {
		t.Fatalf("expected 0 alert events, got %d", len(notifier.GetEvents()))
	}

	// Pass 2: Breach threshold (P95 is 500ms) -> State should become PENDING
	store.InsertBatch(ctx, []model.Metric{
		{Name: "http_request_duration_ms", Value: 500.0, Timestamp: time.Now().UTC()},
	})
	engine.EvaluateOnce(ctx)

	states = engine.GetStates()
	if states[0].CurrentState != StatePending {
		t.Fatalf("expected state PENDING, got %s", states[0].CurrentState)
	}
	if len(notifier.GetEvents()) != 0 {
		t.Fatalf("expected no notifications yet during PENDING, got %d", len(notifier.GetEvents()))
	}

	// Pass 3: Wait for ForDuration (60ms) and evaluate again -> State should become FIRING!
	time.Sleep(60 * time.Millisecond)
	engine.EvaluateOnce(ctx)

	states = engine.GetStates()
	if states[0].CurrentState != StateFiring {
		t.Fatalf("expected state FIRING, got %s", states[0].CurrentState)
	}

	events := notifier.GetEvents()
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 FIRING alert event, got %d", len(events))
	}
	if events[0].Current != StateFiring {
		t.Errorf("expected event state FIRING, got %s", events[0].Current)
	}

	// Pass 4: Evaluate again while still breached -> Should NOT send duplicate spam!
	engine.EvaluateOnce(ctx)
	events = notifier.GetEvents()
	if len(events) != 1 {
		t.Fatalf("expected still exactly 1 event (no spam), got %d", len(events))
	}

	// Pass 5: Metric recovers (wipe store / insert low values) -> State should transition to OK (RESOLVED)
	store.Close() // clears metrics
	store.InsertBatch(ctx, []model.Metric{
		{Name: "http_request_duration_ms", Value: 50.0, Timestamp: time.Now().UTC()},
	})
	engine.EvaluateOnce(ctx)

	states = engine.GetStates()
	if states[0].CurrentState != StateOK {
		t.Fatalf("expected state OK after recovery, got %s", states[0].CurrentState)
	}

	events = notifier.GetEvents()
	if len(events) != 2 {
		t.Fatalf("expected exactly 2 events (1 FIRING, 1 RESOLVED), got %d", len(events))
	}
	if events[1].Current != StateOK {
		t.Errorf("expected 2nd event to be RESOLVED (StateOK), got %s", events[1].Current)
	}
}
