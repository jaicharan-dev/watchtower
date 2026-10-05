package batcher

import (
	"context"
	"fmt"
	"sync"
	"time"
	"watchtower/pkg/model"
	"watchtower/pkg/storage"
)

// Config holds batching parameters.
type Config struct {
	BatchSize     int           // Max metrics before an immediate flush (e.g. 100)
	FlushInterval time.Duration // Max time to wait before flushing a partial batch (e.g. 1 second)
	QueueCapacity int           // Size of the incoming buffer channel
}

// Batcher buffers incoming metrics in memory and writes them to storage in batches.
// This prevents overwhelming the storage layer with millions of individual writes.
type Batcher struct {
	cfg     Config
	storage storage.Storage
	queue   chan model.Metric
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewBatcher creates and starts a new Batcher worker.
func NewBatcher(cfg Config, store storage.Storage) *Batcher {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 1 * time.Second
	}
	if cfg.QueueCapacity <= 0 {
		cfg.QueueCapacity = 10000
	}

	ctx, cancel := context.WithCancel(context.Background())

	b := &Batcher{
		cfg:     cfg,
		storage: store,
		queue:   make(chan model.Metric, cfg.QueueCapacity),
		ctx:     ctx,
		cancel:  cancel,
	}

	// Start background worker goroutine
	b.wg.Add(1)
	go b.worker()

	return b
}

// Submit enqueues a metric without blocking the caller.
// Returns false if the queue is full (rate limit / backpressure signal).
func (b *Batcher) Submit(m model.Metric) bool {
	select {
	case b.queue <- m:
		return true
	default:
		// Queue full: drop or signal backpressure
		return false
	}
}

// worker is the background consumer loop.
func (b *Batcher) worker() {
	defer b.wg.Done()

	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]model.Metric, 0, b.cfg.BatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := b.storage.InsertBatch(b.ctx, batch); err != nil {
			fmt.Printf("[BATCHER ERROR] Failed to flush batch: %v\n", err)
		} else {
			fmt.Printf("[BATCHER] Flushed %d metrics to storage\n", len(batch))
		}
		// Reset batch slice while keeping allocated capacity
		batch = make([]model.Metric, 0, b.cfg.BatchSize)
	}

	for {
		select {
		case <-b.ctx.Done():
			// Drain any remaining items in queue before exiting
			for len(b.queue) > 0 {
				batch = append(batch, <-b.queue)
				if len(batch) >= b.cfg.BatchSize {
					flush()
				}
			}
			flush()
			return

		case m := <-b.queue:
			batch = append(batch, m)
			if len(batch) >= b.cfg.BatchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// Stop cleanly terminates the batcher, flushing all queued metrics.
func (b *Batcher) Stop() {
	b.cancel()
	b.wg.Wait()
}

// Stats returns internal health and saturation indicators (self-monitoring).
func (b *Batcher) Stats() map[string]any {
	qLen := len(b.queue)
	qCap := cap(b.queue)
	saturation := 0.0
	if qCap > 0 {
		saturation = (float64(qLen) / float64(qCap)) * 100.0
	}

	return map[string]any{
		"queue_length":     qLen,
		"queue_capacity":   qCap,
		"queue_saturation": saturation,
		"batch_size":       b.cfg.BatchSize,
		"flush_interval":   b.cfg.FlushInterval.String(),
	}
}

