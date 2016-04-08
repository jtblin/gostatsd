package statsd

import (
	"hash/adler32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

type Dispatcher interface {
	Run(context.Context) error
	DispatchMetric(context.Context, *types.Metric) error
}

type Aggregator interface {
	ReceiveMetric(*types.Metric, time.Time)
	Flush(func() time.Time) *types.MetricMap
	Reset(time.Time)
}

// AggregatorFactory creates Aggregator objects.
type AggregatorFactory interface {
	Create() Aggregator
}

// The HandlerFunc type is an adapter to allow the use of ordinary functions as metric handlers.
type AggregatorFactoryFunc func() Aggregator

// HandleMetric calls f(m).
func (f AggregatorFactoryFunc) Create() Aggregator {
	return f()
}

type flushCommand struct {
	context.Context
	result chan<- *types.MetricMap
}

type worker struct {
	metrics   chan *types.Metric
	flushChan chan *flushCommand
	aggr      Aggregator
}

type stats struct {
	lastFlush      atomic.Value // time.Time; Last time the metrics where aggregated
	lastFlushError atomic.Value // time.Time; Time of the last flush error
}

type dispatcher struct {
	flushInterval    time.Duration          // How often to flush metrics to the sender
	lastFlushTimeout time.Duration          // Timeout of the flush operation, performed before termination
	senders          []backend.MetricSender // Senders to which metrics are flushed
	workers          map[uint16]*worker
	stats
}

func NewDispatcher(numWorkers int, perWorkerBufferSize int, flushInterval, lastFlushTimeout time.Duration, af AggregatorFactory, senders []backend.MetricSender) Dispatcher {
	workers := make(map[uint16]*worker)

	n := uint16(numWorkers)

	for i := uint16(0); i < n; i++ {
		workers[i] = &worker{
			make(chan *types.Metric, perWorkerBufferSize),
			make(chan *flushCommand),
			af.Create(),
		}
	}
	return &dispatcher{
		flushInterval,
		lastFlushTimeout,
		senders,
		workers,
		stats{},
	}
}

func (d *dispatcher) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(len(d.workers) + 1)
	for _, worker := range d.workers {
		go worker.work(&wg)
	}
	go d.flushPeriodically(ctx, &wg)
	defer func() {
		for _, worker := range d.workers {
			close(worker.metrics) // Close channel to terminate worker
		}
		wg.Wait() // Wait for all workers and flusher to finish
	}()

	// Work until asked to stop
	<-ctx.Done()
	return ctx.Err()
}

func (d *dispatcher) DispatchMetric(ctx context.Context, m *types.Metric) error {
	hash := adler32.Checksum([]byte(m.Name))
	worker := d.workers[uint16(hash%uint32(len(d.workers)))]
	select {
	case <-ctx.Done():
		return ctx.Err()
	case worker.metrics <- m:
		return nil
	}
}

func (d *dispatcher) flushPeriodically(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	flushFinished := make(chan struct{}, 1)
	flushTimer := time.NewTimer(d.flushInterval)
	var flushCancel context.CancelFunc
	for {
		select {
		case <-ctx.Done():
			flushTimer.Stop()
			d.doLastFlush(flushCancel, flushFinished)
			return
		case <-flushTimer.C: // Time to flush to the backends
			var flushCtx context.Context
			flushCtx, flushCancel = context.WithCancel(context.Background())
			go func(cancel context.CancelFunc) {
				defer func() {
					flushFinished <- struct{}{}
				}()
				defer cancel()
				d.flushData(flushCtx)
			}(flushCancel)
		case <-flushFinished:
			flushCancel = nil
			flushTimer = time.NewTimer(d.flushInterval)
		}
	}
}

func (d *dispatcher) doLastFlush(flushCancel context.CancelFunc, flushFinished chan struct{}) {
	if flushCancel == nil {
		// Flush is not in flight
		lastCtx, cancelFunc := context.WithTimeout(context.Background(), d.lastFlushTimeout)
		defer cancelFunc()
		d.flushData(lastCtx)
	} else {
		// Flush is in flight
		flushTimeout := time.NewTimer(d.lastFlushTimeout)
		select {
		case <-flushFinished: // Flush finished before timeout
		case <-flushTimeout.C: // Flush has not finished in time
			flushCancel()   // Cancelling flush
			<-flushFinished // Waiting for flush to finish
		}
	}
}

func (d *dispatcher) flushData(ctx context.Context) {
	results := make(chan *types.MetricMap, len(d.workers)) // Enough capacity not to block workers
	cmd := &flushCommand{ctx, results}
	for _, worker := range d.workers {
		select {
		case <-ctx.Done():
			return
		case worker.flushChan <- cmd:
		}
	}
	for r := len(d.workers); r > 0; r-- {
		select {
		case <-ctx.Done():
			return
		case result := <-results:
			d.sendFlushedData(ctx, result)
		}
	}
}

func (d *dispatcher) sendFlushedData(ctx context.Context, metrics *types.MetricMap) {
	var wg sync.WaitGroup
	wg.Add(len(d.senders))
	for _, sender := range d.senders {
		go func(s backend.MetricSender) {
			defer wg.Done()
			log.Debugf("Sending metrics to backend %s", s.BackendName())
			//TODO pass ctx
			d.handleSendResult(s.SendMetrics(*metrics))
		}(sender)
	}
	wg.Wait()
}

func (d *dispatcher) handleSendResult(flushResult error) {
	if flushResult != nil {
		log.Errorf("Sending metrics to backend failed: %v", flushResult)
		d.stats.lastFlushError.Store(time.Now())
	} else {
		d.stats.lastFlush.Store(time.Now())
	}
}

func (d *worker) work(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case metric, ok := <-d.metrics:
			if ok {
				d.aggr.ReceiveMetric(metric, time.Now())
			} else {
				log.Debug("Worker metrics queue was closed, exiting")
				// TODO flush metrics
				return
			}
		case cmd := <-d.flushChan:
			flushed := d.aggr.Flush(time.Now) // pass func for stubbing
			d.aggr.Reset(time.Now())
			select {
			case <-cmd.Context.Done():
			case cmd.result <- flushed:
			}
		}
	}
}
