package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Notifier defines the interface for delivering alert events (Observer Pattern - SOLID).
type Notifier interface {
	Notify(ctx context.Context, event AlertEvent) error
}

// LogNotifier prints alert state changes directly to the console.
type LogNotifier struct{}

func NewLogNotifier() *LogNotifier {
	return &LogNotifier{}
}

func (l *LogNotifier) Notify(ctx context.Context, event AlertEvent) error {
	badge := "🚨 [ALERT FIRING]"
	if event.Current == StateOK {
		badge = "✅ [ALERT RESOLVED]"
	}

	fmt.Printf("\n%s Rule: '%s' (%s)\n", badge, event.Rule.Name, event.Rule.ID)
	fmt.Printf("   Current Value: %.2f | Threshold: %.2f (Operator: %s)\n",
		event.Value, event.Threshold, event.Rule.Operator)
	fmt.Printf("   State Transition: %s -> %s\n", event.Previous, event.Current)
	fmt.Printf("   Details: %s\n\n", event.Message)
	return nil
}

// WebhookNotifier dispatches alert events as JSON payloads to a webhook URL (Slack, Discord, PagerDuty).
type WebhookNotifier struct {
	URL    string
	client *http.Client
}

func NewWebhookNotifier(url string) *WebhookNotifier {
	return &WebhookNotifier{
		URL: url,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

func (w *WebhookNotifier) Notify(ctx context.Context, event AlertEvent) error {
	if w.URL == "" {
		return nil
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal alert event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook POST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned non-2xx status code: %d", resp.StatusCode)
	}

	return nil
}
