package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"watchtower/pkg/alert"
	"watchtower/pkg/anomaly"
	"watchtower/pkg/batcher"
	"watchtower/pkg/middleware"
	"watchtower/pkg/model"
	"watchtower/pkg/storage"
	"watchtower/pkg/web"
)

func main() {
	// 1. Initialize Storage (In-memory engine, pluggable to ClickHouse)
	store := storage.NewMemoryStorage(100000)
	defer store.Close()

	// 2. Initialize Batcher (Flush every 50 metrics OR every 1 second)
	batchConfig := batcher.Config{
		BatchSize:     50,
		FlushInterval: 1 * time.Second,
		QueueCapacity: 5000,
	}
	b := batcher.NewBatcher(batchConfig, store)
	defer b.Stop()

	// 3. Initialize Alert Engine (Evaluate every 4 seconds)
	alertEngine := alert.NewEngine(store, 4*time.Second)
	alertEngine.RegisterNotifier(alert.NewLogNotifier())

	// Register sample alert rules:
	// Rule 1: High P95 Latency on Checkout endpoint (> 500ms for 10s)
	alertEngine.RegisterRule(alert.Rule{
		ID:          "alert-checkout-high-p95",
		Name:        "High P95 Latency on Checkout",
		MetricName:  "http_request_duration_ms",
		GroupByTag:  "endpoint",
		TargetGroup: "/api/checkout",
		Stat:        "p95",
		Operator:    ">",
		Threshold:   500.0,
		Window:      1 * time.Minute,
		ForDuration: 10 * time.Second,
	})

	// Rule 2: Overall High P99 Latency (> 1000ms for 10s)
	alertEngine.RegisterRule(alert.Rule{
		ID:          "alert-overall-high-p99",
		Name:        "Severe Tail Latency (P99 > 1s)",
		MetricName:  "http_request_duration_ms",
		Stat:        "p99",
		Operator:    ">",
		Threshold:   1000.0,
		Window:      1 * time.Minute,
		ForDuration: 10 * time.Second,
	})

	alertCtx, cancelAlerts := context.WithCancel(context.Background())
	defer cancelAlerts()
	alertEngine.Start(alertCtx)
	defer alertEngine.Stop()

	// 4. Set up HTTP router
	mux := http.NewServeMux()

	// Ingest endpoint (Fast, non-blocking Producer)
	mux.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST requests are allowed", http.StatusMethodNotAllowed)
			return
		}

		var m model.Metric
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
			return
		}

		if m.Name == "" || m.Type == "" {
			http.Error(w, "Metric 'name' and 'type' are required", http.StatusBadRequest)
			return
		}

		if m.Timestamp.IsZero() {
			m.Timestamp = time.Now().UTC()
		}

		// Submit to batcher (non-blocking)
		if ok := b.Submit(m); !ok {
			// Queue full - send backpressure HTTP 429 Too Many Requests
			http.Error(w, "Ingestion buffer full, please retry", http.StatusTooManyRequests)
			return
		}

		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintf(w, "Metric accepted\n")
	})

	// Query endpoint (Instant verification of stored data)
	mux.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET requests are allowed", http.StatusMethodNotAllowed)
			return
		}

		metricName := r.URL.Query().Get("name")
		limitStr := r.URL.Query().Get("limit")
		limit := 20
		if limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil {
				limit = l
			}
		}

		results, err := store.Query(r.Context(), storage.QueryOptions{
			MetricName: metricName,
			Limit:      limit,
		})
		if err != nil {
			http.Error(w, "Query error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"total_in_storage": store.Count(),
			"count":            len(results),
			"results":          results,
		})
	})

	// Aggregate endpoint (Calculates min, max, avg, percentiles, group-by)
	mux.HandleFunc("/aggregate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET requests are allowed", http.StatusMethodNotAllowed)
			return
		}

		metricName := r.URL.Query().Get("name")
		if metricName == "" {
			http.Error(w, "Query parameter 'name' is required", http.StatusBadRequest)
			return
		}

		groupBy := r.URL.Query().Get("group_by")
		windowStr := r.URL.Query().Get("window")

		var from time.Time
		if windowStr != "" {
			d, err := time.ParseDuration(windowStr)
			if err != nil {
				http.Error(w, "Invalid window duration (e.g. 1m, 5m, 1h): "+err.Error(), http.StatusBadRequest)
				return
			}
			from = time.Now().UTC().Add(-d)
		}

		aggResult, err := store.Aggregate(r.Context(), storage.AggregateOptions{
			MetricName: metricName,
			From:       from,
			GroupByTag: groupBy,
		})
		if err != nil {
			http.Error(w, "Aggregate error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(aggResult)
	})

	// Alerts endpoint (View live status of all alert rules)
	mux.HandleFunc("/alerts", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET requests are allowed", http.StatusMethodNotAllowed)
			return
		}

		states := alertEngine.GetStates()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"total_rules": len(states),
			"alerts":      states,
		})
	})

	anomalyDetector := anomaly.NewDetector(store)

	// Anomaly Detection endpoint (Statistical Z-Score analysis)
	mux.HandleFunc("/anomalies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET requests are allowed", http.StatusMethodNotAllowed)
			return
		}

		metricName := r.URL.Query().Get("name")
		if metricName == "" {
			metricName = "http_request_duration_ms"
		}
		groupBy := r.URL.Query().Get("group_by")
		if groupBy == "" {
			groupBy = "endpoint"
		}

		windowStr := r.URL.Query().Get("window")
		window := 5 * time.Minute
		if windowStr != "" {
			if d, err := time.ParseDuration(windowStr); err == nil {
				window = d
			}
		}

		sigma := 2.5
		if sStr := r.URL.Query().Get("sigma"); sStr != "" {
			if s, err := strconv.ParseFloat(sStr, 64); err == nil {
				sigma = s
			}
		}

		result, err := anomalyDetector.Analyze(r.Context(), metricName, groupBy, window, sigma)
		if err != nil {
			http.Error(w, "Anomaly analysis error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})

	startTime := time.Now()

	// Web Dashboard (Served from embedded assets at http://localhost:8080/)
	mux.Handle("/", web.Handler())

	// Internal Self-Monitoring endpoint (Dogfooding statistics)
	mux.HandleFunc("/internal/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Only GET requests are allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"uptime":            time.Since(startTime).String(),
			"goroutines":        runtime.NumGoroutine(),
			"storage_metrics":   store.Count(),
			"batcher":           b.Stats(),
		})
	})

	// 4. Configure HTTP Server with MetricsDecorator (Decorator Pattern)
	server := &http.Server{
		Addr:    ":8080",
		Handler: middleware.MetricsDecorator(b, mux),
	}

	// 5. Handle graceful shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Println("==================================================")
		fmt.Println("🚀 Watchtower Ingestion API & Dashboard online!")
		fmt.Println("   📊 Dashboard:       http://localhost:8080/")
		fmt.Println("   POST /ingest        -> Submit metrics (Buffered & Batched)")
		fmt.Println("   GET  /query         -> View stored metrics")
		fmt.Println("   GET  /aggregate     -> Aggregated statistics (p95, avg, etc.)")
		fmt.Println("   GET  /alerts        -> Live Alert Engine statuses")
		fmt.Println("   GET  /anomalies     -> Statistical Anomaly Detection (Z-score)")
		fmt.Println("   GET  /internal/stats-> Dogfooding & internal telemetry")
		fmt.Println("==================================================")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server listen failed: %v\n", err)
		}
	}()

	<-stopChan
	fmt.Println("\n🛑 Shutting down server gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Printf("HTTP shutdown error: %v\n", err)
	}

	fmt.Println("Server stopped cleanly.")
}
