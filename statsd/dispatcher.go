package statsd

import (
	"time"
	"sync"
	"hash/adler32"

	"github.com/jtblin/gostatsd/types"
	"github.com/jtblin/gostatsd/backend"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

type Dispatcher interface {
	Run() error
	DispatchMetric(ctx context.Context, m *types.Metric)
}

type Aggregator interface {
	ReceiveMetric(*types.Metric, time.Time)
	Flush(func() time.Time) *types.MetricMap
	Reset(time.Time)
}

type AggregatorFactory interface {
	Create() Aggregator
}

type worker struct {
	metrics chan *types.Metric
	commands chan struct{}
	aggr Aggregator
}

type stats struct {
	LastFlush         time.Time          // Last time the metrics where aggregated
	LastFlushError time.Time
}

type dispatcher struct {
	flushInterval time.Duration      // How often to flush metrics to the sender
	//af AggregatorFactory		// Factory of Aggregator objects
	senders []backend.MetricSender // Senders to which metrics are flushed
	workers map[uint16]*worker
}

func NewDispatcher(numWorkers uint16, perWorkerBufferSize uint32, flushInterval time.Duration, af AggregatorFactory, senders []backend.MetricSender) Dispatcher {
	workers := make(map[uint16]*worker)

	for i := 0; i < numWorkers; i++ {
		workers[i] = &worker{
			make(chan *types.Metric, perWorkerBufferSize),
			make(chan struct{}),
			af.Create(),
		}
	}
	return &dispatcher{
		flushInterval,
		af,
		senders,
		workers,
	}
}

func (d *dispatcher) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(len(d.workers))
	for _, worker := range d.workers {
		go worker.work(wg)
	}
	defer func() {
		for _, channel := range d.workers {
			close(channel) // Close channel to terminate worker
		}
		wg.Wait() // Wait for all workers to finish
	}()

	// Work until asked to stop
	<-ctx.Done()
	return ctx.Err()
}

func (d *dispatcher) DispatchMetric(ctx context.Context, m *types.Metric) {
	hash := adler32.Checksum([]byte(m.Name))
	channel := d.workers[hash % len(d.workers)]
	select {
	case channel <- m:
		return nil
	case <- ctx.Done():
		return ctx.Err()
	}
}

func (d *worker) work(wg *sync.WaitGroup) {
	defer wg.Done()
	flushTimer := time.NewTimer(d.flushInterval)

	for {
		select {
		case metric, ok := <-channel:
			if ok {
				aggr.ReceiveMetric(metric, time.Now())
			} else {
				log.Debug("Aggregator metrics queue was closed, exiting")
				// TODO flush metrics
				return
			}
		case <-flushTimer.C: // Time to flush to the backends
			flushed := aggr.Flush(time.Now) // pass func for stubbing
			aggr.Reset(time.Now())
			for _, sender := range a.Senders {
				go func(s backend.MetricSender) {
					log.Debugf("Sending metrics to backend %s", s.BackendName())
					a.handleFlushResult(s.SendMetrics(flushed))
				}(sender)
			}
			flushTimer = time.NewTimer(a.FlushInterval)
		}
	}
}

func handleFlushResult(a *MetricAggregator, flushResult error) {
	//a.Lock()
	//defer a.Unlock()
	if flushResult != nil {
		log.Errorf("Sending metrics to backend failed: %v", flushResult)
		a.Stats.LastFlushError = time.Now()
	} else {
		a.Stats.LastFlush = time.Now()
	}
}
