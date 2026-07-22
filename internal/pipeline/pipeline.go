package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaop-labs/coral/internal/delivery"
)

// Config controls pipeline concurrency.
type Config struct {
	Workers    int
	QueueSize  int
	QueueBytes int64
}

func (c *Config) setDefaults() {
	if c.Workers <= 0 {
		c.Workers = runtime.NumCPU()
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 10000
	}
	if c.QueueBytes <= 0 {
		c.QueueBytes = 64 << 20
	}
}

// Pipeline moves batches of one signal from receivers through processors to
// exporters. A single worker-pool implementation serves every signal type
// (traces, metrics, logs); the element type T carries the signal-specific data.
//
// Durable OTLP batches carry journal ownership metadata. Required-destination
// failures are returned to the ingress retry/quarantine loop; batches without
// that metadata retain the pipeline's fail-loud, best-effort semantics.
type Pipeline[T Signal] struct {
	cfg        Config
	receivers  []Receiver[T]
	processors []Processor[T]
	exporters  []Exporter[T]
	logger     *slog.Logger

	in             chan T
	wg             sync.WaitGroup
	exporterWG     sync.WaitGroup
	exporterLanes  []chan exporterItem[T]
	exporterNeeded []bool
	exporterBytes  []atomic.Int64
	deliveryObs    func(delivery.Metadata)
	deliveryFail   func(delivery.Metadata, error)
	stateMu        sync.Mutex
	shutdownOnce   sync.Once
	shutdownDone   chan struct{}
	abortOnce      sync.Once
	abortDone      chan struct{}
	shutdownMu     sync.RWMutex
	shutdownErr    error
	abortErr       error
	drainStarted   time.Time
	drainFinished  time.Time
	drainOutcome   string
	drainForced    bool
	workCtx        context.Context
	workCancel     context.CancelFunc
	receiverCancel context.CancelFunc
	started        atomic.Bool
	stopped        atomic.Bool
	enqueueStopped atomic.Bool
	enqueueMu      sync.Mutex
	enqueueWG      sync.WaitGroup
	queuedBytes    atomic.Int64
	stopEnqueue    chan struct{}
	exportMu       sync.RWMutex

	batchesIn      atomic.Uint64
	batchesDropped atomic.Uint64
	exporterDrops  atomic.Uint64
	itemsOut       atomic.Uint64

	batchesDispatched atomic.Uint64
	itemsDispatched   atomic.Uint64
	batchesDelivered  atomic.Uint64
	itemsDelivered    atomic.Uint64
	processorFailures atomic.Uint64
	exporterFailures  atomic.Uint64
	terminalFailures  atomic.Uint64
}

type exporterItem[T Signal] struct {
	batch    T
	required bool
	attempt  *deliveryAttempt
}

type deliveryAttempt struct {
	remaining atomic.Int64
	meta      delivery.Metadata
	observer  func(delivery.Metadata)
	failure   func(delivery.Metadata, error)
	failOnce  sync.Once
}

func (d *deliveryAttempt) fail(err error) bool {
	if d == nil || d.failure == nil || err == nil {
		return false
	}
	d.failOnce.Do(func() { d.failure(d.meta, err) })
	return true
}

func (d *deliveryAttempt) confirm() {
	if d == nil || d.remaining.Add(-1) != 0 || d.observer == nil {
		return
	}
	d.observer(d.meta)
}

// New creates a pipeline for signal type T. T must be supplied explicitly, as
// it cannot be inferred from the arguments: pipeline.New[model.Batch](cfg, log).
func New[T Signal](cfg Config, logger *slog.Logger) *Pipeline[T] {
	cfg.setDefaults()
	return &Pipeline[T]{
		cfg:          cfg,
		logger:       logger,
		in:           make(chan T, cfg.QueueSize),
		stopEnqueue:  make(chan struct{}),
		shutdownDone: make(chan struct{}),
		abortDone:    make(chan struct{}),
	}
}

var errPipelineStopped = errors.New("pipeline stopped")
var errPipelineStarted = errors.New("pipeline already started")

func (p *Pipeline[T]) AddReceiver(r Receiver[T])    { p.receivers = append(p.receivers, r) }
func (p *Pipeline[T]) AddProcessor(pr Processor[T]) { p.processors = append(p.processors, pr) }

// AddExporter adds an isolated best-effort destination. Its outcome never
// acknowledges durable journal work.
func (p *Pipeline[T]) AddExporter(e Exporter[T]) {
	p.exporters = append(p.exporters, e)
	p.exporterNeeded = append(p.exporterNeeded, false)
}

// AddRequiredExporter adds a destination whose successful durable admission
// is required before a journal record may be acknowledged.
func (p *Pipeline[T]) AddRequiredExporter(e Exporter[T]) {
	p.exporters = append(p.exporters, e)
	p.exporterNeeded = append(p.exporterNeeded, true)
}

// SetDeliveryObserver installs the journal completion callback. It must be
// configured before Start.
func (p *Pipeline[T]) SetDeliveryObserver(fn func(delivery.Metadata)) {
	p.deliveryObs = fn
}

// SetDeliveryFailureObserver installs the durable retry/quarantine callback
// for processor failures and required-destination failures.
func (p *Pipeline[T]) SetDeliveryFailureObserver(fn func(delivery.Metadata, error)) {
	p.deliveryFail = fn
}

// Enqueue pushes b onto the worker queue, blocking on backpressure until a
// worker is free or ctx is canceled. Empty batches are dropped silently. It is
// the single entry point for every source — Receiver emit closures and the
// shared OTLP ingress alike. Enqueue must not be called after Shutdown.
func (p *Pipeline[T]) Enqueue(ctx context.Context, b T) error {
	if b.Len() == 0 {
		return nil
	}
	p.enqueueMu.Lock()
	if p.enqueueStopped.Load() {
		p.enqueueMu.Unlock()
		return errPipelineStopped
	}
	p.enqueueWG.Add(1)
	p.enqueueMu.Unlock()
	defer p.enqueueWG.Done()

	p.batchesIn.Add(1)
	bytes := signalBytes(b)
	if !p.reserveBytes(ctx, bytes) {
		p.batchesDropped.Add(1)
		return fmt.Errorf("pipeline queue byte limit exceeded")
	}
	select {
	case p.in <- b:
		return nil
	case <-p.stopEnqueue:
		p.queuedBytes.Add(-bytes)
		p.batchesDropped.Add(1)
		return errPipelineStopped
	case <-ctx.Done():
		p.queuedBytes.Add(-bytes)
		p.batchesDropped.Add(1)
		return ctx.Err()
	}
}

// Start launches the worker pool and all receivers.
func (p *Pipeline[T]) Start(ctx context.Context) error {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if p.stopped.Load() || p.enqueueStopped.Load() {
		return errPipelineStopped
	}
	if p.started.Load() {
		return errPipelineStarted
	}

	// Receiver lifetime follows the application run context. Processing and
	// exporter delivery deliberately do not: the run context is normally
	// cancelled before Shutdown receives its independent drain deadline.
	receiverCtx, receiverCancel := context.WithCancel(ctx)
	p.receiverCancel = receiverCancel
	p.workCtx, p.workCancel = context.WithCancel(context.WithoutCancel(ctx))

	for range p.exporters {
		p.exporterLanes = append(p.exporterLanes, make(chan exporterItem[T], p.cfg.QueueSize))
		p.exporterBytes = append(p.exporterBytes, atomic.Int64{})
	}
	for i, e := range p.exporters {
		lane := p.exporterLanes[i]
		p.exporterWG.Add(1)
		go func() {
			defer p.exporterWG.Done()
			for item := range lane {
				p.exporterBytes[i].Add(-signalBytes(item.batch))
				if err := e.Export(p.workCtx, item.batch); err != nil {
					p.exporterFailures.Add(1)
					p.logger.Error("exporter error", "err", err)
					if item.required {
						if !item.attempt.fail(err) {
							p.terminalFailures.Add(1)
						}
					} else if p.deliveryFail == nil {
						p.terminalFailures.Add(1)
					}
					continue
				}
				if item.required {
					item.attempt.confirm()
				}
				p.batchesDelivered.Add(1)
				p.itemsDelivered.Add(uint64(item.batch.Len()))
			}
		}()
	}
	p.started.Store(true)

	for i := 0; i < p.cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(p.workCtx)
	}

	for _, r := range p.receivers {
		go func() {
			if err := r.Start(receiverCtx, p.Enqueue); err != nil && receiverCtx.Err() == nil {
				p.logger.Error("receiver exited with error", "err", err)
			}
		}()
	}
	return nil
}

// Shutdown stops receivers, drains the queue, closes processors and exporters.
// It is safe to call multiple times. If ctx expires, Shutdown cancels in-flight
// processing/export calls, returns promptly, and cleanup continues in the
// background. Later calls return the same terminal drain failure.
func (p *Pipeline[T]) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		p.beginShutdown()
		go p.finishShutdown(ctx)
	})

	select {
	case <-p.shutdownDone:
		return p.shutdownResult()
	case <-p.abortDone:
		return p.abortResult()
	case <-ctx.Done():
		// Prefer a cleanup result that became available concurrently with the
		// deadline over falsely reporting a forced drain.
		select {
		case <-p.shutdownDone:
			return p.shutdownResult()
		default:
		}
		p.abortDrain(ctx.Err())
		return p.abortResult()
	}
}

func (p *Pipeline[T]) beginShutdown() {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()

	p.shutdownMu.Lock()
	p.drainStarted = time.Now()
	p.drainOutcome = "in_progress"
	p.shutdownMu.Unlock()

	p.enqueueMu.Lock()
	p.enqueueStopped.Store(true)
	close(p.stopEnqueue)
	p.enqueueMu.Unlock()
	if p.receiverCancel != nil {
		p.receiverCancel()
	}
}

func (p *Pipeline[T]) finishShutdown(ctx context.Context) {
	var errs []error
	p.enqueueWG.Wait()

	for i, r := range p.receivers {
		if stopErr := stopReceiver(ctx, r); stopErr != nil {
			p.logger.Error("receiver stop error", "receiver", i, "err", stopErr)
			errs = append(errs, fmt.Errorf("receiver %d: %w", i, stopErr))
		}
	}

	close(p.in)
	p.wg.Wait()

	for i := len(p.processors) - 1; i >= 0; i-- {
		if closeErr := p.processors[i].Close(); closeErr != nil {
			p.logger.Error("processor close error", "processor", i, "err", closeErr)
			errs = append(errs, fmt.Errorf("processor %d: %w", i, closeErr))
		}
	}
	p.exportMu.Lock()
	p.stopped.Store(true)
	p.started.Store(false)
	for _, lane := range p.exporterLanes {
		close(lane)
	}
	p.exportMu.Unlock()
	p.exporterWG.Wait()
	if p.workCancel != nil {
		p.workCancel()
	}
	for i := len(p.exporters) - 1; i >= 0; i-- {
		if closeErr := p.exporters[i].Close(); closeErr != nil {
			p.logger.Error("exporter close error", "exporter", i, "err", closeErr)
			errs = append(errs, fmt.Errorf("exporter %d: %w", i, closeErr))
		}
	}

	if failures := p.failureError(); failures != nil {
		errs = append(errs, failures)
	}
	p.shutdownMu.Lock()
	if p.abortErr != nil {
		errs = append(errs, p.abortErr)
	}
	p.shutdownErr = errors.Join(errs...)
	p.drainFinished = time.Now()
	if p.drainForced {
		p.drainOutcome = "deadline"
	} else if p.shutdownErr != nil {
		p.drainOutcome = "failed"
	} else {
		p.drainOutcome = "success"
	}
	close(p.shutdownDone)
	p.shutdownMu.Unlock()
}

func stopReceiver[T Signal](ctx context.Context, r Receiver[T]) error {
	stopped := make(chan error, 1)
	go func() {
		stopped <- r.Stop(ctx)
	}()
	select {
	case err := <-stopped:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Pipeline[T]) abortDrain(cause error) {
	p.abortOnce.Do(func() {
		if p.workCancel != nil {
			p.workCancel()
		}
		p.shutdownMu.Lock()
		p.drainForced = true
		p.drainOutcome = "deadline"
		p.abortErr = fmt.Errorf("pipeline drain: %w", cause)
		close(p.abortDone)
		p.shutdownMu.Unlock()
	})
}

func (p *Pipeline[T]) shutdownResult() error {
	p.shutdownMu.RLock()
	defer p.shutdownMu.RUnlock()
	return p.shutdownErr
}

func (p *Pipeline[T]) abortResult() error {
	p.shutdownMu.RLock()
	defer p.shutdownMu.RUnlock()
	return p.abortErr
}

func (p *Pipeline[T]) failureError() error {
	processorFailures := p.processorFailures.Load()
	exporterFailures := p.exporterFailures.Load()
	exporterDrops := p.exporterDrops.Load()
	if p.deliveryFail != nil {
		if terminal := p.terminalFailures.Load(); terminal > 0 {
			return fmt.Errorf("pipeline data loss: unrecoverable_failures=%d", terminal)
		}
		return nil
	}
	if processorFailures == 0 && exporterFailures == 0 && exporterDrops == 0 {
		return nil
	}
	return fmt.Errorf(
		"pipeline data loss: processor_failures=%d exporter_failures=%d exporter_queue_drops=%d",
		processorFailures,
		exporterFailures,
		exporterDrops,
	)
}

// CloseUnstarted releases processors and exporters materialized while building
// an application that subsequently failed validation. It must only be called
// by the single construction goroutine before Start.
func (p *Pipeline[T]) CloseUnstarted() error {
	if p.started.Load() {
		return errors.New("pipeline is already started")
	}
	var errs []error
	for i := len(p.processors) - 1; i >= 0; i-- {
		if err := p.processors[i].Close(); err != nil {
			errs = append(errs, fmt.Errorf("processor %d: %w", i, err))
		}
	}
	for i := len(p.exporters) - 1; i >= 0; i-- {
		if err := p.exporters[i].Close(); err != nil {
			errs = append(errs, fmt.Errorf("exporter %d: %w", i, err))
		}
	}
	p.stopped.Store(true)
	if p.workCancel != nil {
		p.workCancel()
	}
	if p.receiverCancel != nil {
		p.receiverCancel()
	}
	return errors.Join(errs...)
}

// Export sends b through the full processor chain and then to exporters.
func (p *Pipeline[T]) Export(ctx context.Context, b T) error {
	return p.processFrom(ctx, b, 0)
}

// ExportFrom sends b through processors starting at startIndex.
// It is used by stateful processors that flush batches downstream.
func (p *Pipeline[T]) ExportFrom(ctx context.Context, b T, startIndex int) error {
	return p.processFrom(ctx, b, startIndex)
}

func (p *Pipeline[T]) worker(ctx context.Context) {
	defer p.wg.Done()
	for b := range p.in {
		p.queuedBytes.Add(-signalBytes(b))
		if err := p.processFrom(ctx, b, 0); err != nil {
			p.logger.Error("pipeline processing error", "err", err)
		}
	}
}

const defaultItemBytes = 256

func signalBytes[T Signal](b T) int64 {
	if sized, ok := any(b).(SizedSignal); ok && sized.SizeBytes() > 0 {
		return int64(sized.SizeBytes())
	}
	return int64(max(b.Len(), 1)) * defaultItemBytes
}

func (p *Pipeline[T]) reserveBytes(ctx context.Context, n int64) bool {
	if n > p.cfg.QueueBytes {
		return false
	}
	for {
		cur := p.queuedBytes.Load()
		if cur+n > p.cfg.QueueBytes {
			select {
			case <-ctx.Done():
				return false
			case <-p.stopEnqueue:
				return false
			default:
				return false
			}
		}
		if p.queuedBytes.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

func (p *Pipeline[T]) processFrom(ctx context.Context, b T, startIndex int) error {
	var err error
	if startIndex < 0 {
		startIndex = 0
	}
	if startIndex > len(p.processors) {
		startIndex = len(p.processors)
	}
	for _, pr := range p.processors[startIndex:] {
		input := b
		b, err = pr.Process(ctx, b)
		if err != nil {
			p.processorFailures.Add(1)
			// A failed processor is allowed to return an empty/partial batch.
			// Delivery ownership belongs to its input until processing succeeds.
			if !p.notifyDeliveryFailure(input, err) {
				p.terminalFailures.Add(1)
			}
			return fmt.Errorf("processor: %w", err)
		}
		if b.Len() == 0 {
			return nil
		}
	}
	p.itemsOut.Add(uint64(b.Len()))
	p.exportMu.RLock()
	defer p.exportMu.RUnlock()
	if p.stopped.Load() {
		return errPipelineStopped
	}
	if p.started.Load() {
		// Each exporter owns a bounded delivery lane. A retrying or unavailable
		// destination can fill and drop its own lane, but cannot delay or block
		// delivery to the other fan-out destinations.
		attempt := p.newDeliveryAttempt(b)
		for i, lane := range p.exporterLanes {
			bytes := signalBytes(b)
			if !p.reserveLaneBytes(i, bytes) {
				p.exporterDrops.Add(1)
				if p.exporterNeeded[i] {
					if !attempt.fail(errors.New("required exporter lane byte limit exceeded")) {
						p.terminalFailures.Add(1)
					}
				} else if p.deliveryFail == nil {
					p.terminalFailures.Add(1)
				}
				continue
			}
			select {
			case lane <- exporterItem[T]{batch: b, required: p.exporterNeeded[i], attempt: attempt}:
				p.batchesDispatched.Add(1)
				p.itemsDispatched.Add(uint64(b.Len()))
			default:
				p.exporterBytes[i].Add(-bytes)
				p.exporterDrops.Add(1)
				if p.exporterNeeded[i] {
					if !attempt.fail(errors.New("required exporter lane is full")) {
						p.terminalFailures.Add(1)
					}
				} else if p.deliveryFail == nil {
					p.terminalFailures.Add(1)
				}
				p.logger.Error("exporter queue full; batch dropped", "exporter", i)
			}
		}
	} else {
		// Direct Export before Start remains synchronous for processors/tests that
		// use the pipeline as a simple composition primitive.
		var errs []error
		requiredSucceeded := true
		requiredCount := 0
		for i, e := range p.exporters {
			p.batchesDispatched.Add(1)
			p.itemsDispatched.Add(uint64(b.Len()))
			if err := e.Export(ctx, b); err != nil {
				if p.exporterNeeded[i] {
					requiredSucceeded = false
					if !p.notifyDeliveryFailure(b, err) {
						p.terminalFailures.Add(1)
					}
				} else if p.deliveryFail == nil {
					p.terminalFailures.Add(1)
				}
				p.exporterFailures.Add(1)
				p.logger.Error("exporter error", "exporter", i, "err", err)
				errs = append(errs, fmt.Errorf("exporter %d: %w", i, err))
				continue
			}
			if p.exporterNeeded[i] {
				requiredCount++
			}
			p.batchesDelivered.Add(1)
			p.itemsDelivered.Add(uint64(b.Len()))
		}
		if requiredCount > 0 && requiredSucceeded && p.deliveryObs != nil {
			if carrier, ok := any(b).(delivery.Carrier); ok {
				meta := carrier.DeliveryMetadata()
				if !meta.Empty() {
					p.deliveryObs(meta)
				}
			}
		}
		return errors.Join(errs...)
	}
	return nil
}

func (p *Pipeline[T]) newDeliveryAttempt(b T) *deliveryAttempt {
	required := 0
	for _, needed := range p.exporterNeeded {
		if needed {
			required++
		}
	}
	if required == 0 || p.deliveryObs == nil {
		return nil
	}
	carrier, ok := any(b).(delivery.Carrier)
	if !ok {
		return nil
	}
	meta := carrier.DeliveryMetadata()
	if meta.Empty() {
		return nil
	}
	attempt := &deliveryAttempt{meta: meta, observer: p.deliveryObs, failure: p.deliveryFail}
	attempt.remaining.Store(int64(required))
	return attempt
}

func (p *Pipeline[T]) notifyDeliveryFailure(b T, err error) bool {
	if p.deliveryFail == nil || err == nil {
		return false
	}
	carrier, ok := any(b).(delivery.Carrier)
	if !ok {
		return false
	}
	meta := carrier.DeliveryMetadata()
	if !meta.Empty() {
		p.deliveryFail(meta, err)
		return true
	}
	return false
}

func (p *Pipeline[T]) reserveLaneBytes(i int, n int64) bool {
	for {
		cur := p.exporterBytes[i].Load()
		if cur+n > p.cfg.QueueBytes {
			return false
		}
		if p.exporterBytes[i].CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

// Stats returns pipeline counters for observability: batches enqueued, batches
// dropped on backpressure, and items (spans/points/records) processed through
// the processor chain. The third value historically meant "out" but never
// proved downstream delivery; use DeliveryStats for truthful delivery counts.
func (p *Pipeline[T]) Stats() (batchesIn, batchesDropped, itemsOut uint64) {
	return p.batchesIn.Load(), p.batchesDropped.Load(), p.itemsOut.Load()
}

// DeliverySnapshot is a bounded-cardinality aggregate of the pipeline's
// processing, fan-out dispatch, and confirmed exporter outcomes.
type DeliverySnapshot struct {
	ItemsProcessed    uint64
	BatchesDispatched uint64
	ItemsDispatched   uint64
	BatchesDelivered  uint64
	ItemsDelivered    uint64
	ProcessorFailures uint64
	ExporterFailures  uint64
	ExporterDrops     uint64
}

// DeliveryStats returns counters whose names reflect the point they measure.
func (p *Pipeline[T]) DeliveryStats() DeliverySnapshot {
	return DeliverySnapshot{
		ItemsProcessed:    p.itemsOut.Load(),
		BatchesDispatched: p.batchesDispatched.Load(),
		ItemsDispatched:   p.itemsDispatched.Load(),
		BatchesDelivered:  p.batchesDelivered.Load(),
		ItemsDelivered:    p.itemsDelivered.Load(),
		ProcessorFailures: p.processorFailures.Load(),
		ExporterFailures:  p.exporterFailures.Load(),
		ExporterDrops:     p.exporterDrops.Load(),
	}
}

// DrainSnapshot describes the current or final bounded shutdown attempt.
type DrainSnapshot struct {
	Started    bool
	InProgress bool
	Forced     bool
	Outcome    string
	Duration   time.Duration
}

// DrainStats returns drain state with one of the bounded outcomes:
// not_started, in_progress, success, failed, or deadline.
func (p *Pipeline[T]) DrainStats() DrainSnapshot {
	p.shutdownMu.RLock()
	defer p.shutdownMu.RUnlock()
	if p.drainStarted.IsZero() {
		return DrainSnapshot{Outcome: "not_started"}
	}
	end := p.drainFinished
	if end.IsZero() {
		end = time.Now()
	}
	return DrainSnapshot{
		Started:    true,
		InProgress: p.drainFinished.IsZero(),
		Forced:     p.drainForced,
		Outcome:    p.drainOutcome,
		Duration:   end.Sub(p.drainStarted),
	}
}

// QueueDepth returns the current input queue depth and its configured capacity.
// Channel len/cap are safe concurrent snapshots and do not block producers.
func (p *Pipeline[T]) QueueDepth() (depth, capacity int) {
	return len(p.in), cap(p.in)
}

// ExporterLaneDepth returns bounded queue depth/capacity and bytes for a
// destination index. Destination indices are stable within one configuration.
func (p *Pipeline[T]) ExporterLaneDepth(index int) (depth, capacity int, bytes, byteCapacity int64) {
	p.exportMu.RLock()
	defer p.exportMu.RUnlock()
	if index < 0 || index >= len(p.exporterLanes) {
		return 0, 0, 0, p.cfg.QueueBytes
	}
	return len(p.exporterLanes[index]), cap(p.exporterLanes[index]), p.exporterBytes[index].Load(), p.cfg.QueueBytes
}

// ExporterDrops returns batches dropped from an individual exporter lane
// because that destination remained slower than the pipeline.
func (p *Pipeline[T]) ExporterDrops() uint64 { return p.exporterDrops.Load() }
