package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"watchtower/pkg/batcher"
	"watchtower/pkg/model"
	"watchtower/pkg/storage"
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

	// 3. Set up HTTP router
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

	// 4. Configure HTTP Server
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// 5. Handle graceful shutdown
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		fmt.Println("==================================================")
		fmt.Println("🚀 Watchtower Ingestion API online at :8080")
		fmt.Println("   POST /ingest -> Submit metrics (Buffered & Batched)")
		fmt.Println("   GET  /query  -> View stored metrics")
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
