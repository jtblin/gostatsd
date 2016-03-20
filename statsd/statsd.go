package statsd

import (
	"fmt"
	"runtime"
	"strconv"
	"time"

	"github.com/jtblin/gostatsd/backend"
	_ "github.com/jtblin/gostatsd/backend/backends" // import backends for initialisation
	"github.com/jtblin/gostatsd/cloudprovider"
	_ "github.com/jtblin/gostatsd/cloudprovider/providers" // import cloud providers for initialisation
	"github.com/jtblin/gostatsd/types"

	"golang.org/x/net/context"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server.
type Server struct {
	aggregator       *MetricAggregator
	Backends         []string
	ConsoleAddr      string
	CloudProvider    string
	DefaultTags      []string
	ExpiryInterval   time.Duration
	FlushInterval    time.Duration
	MaxReaders       int
	MaxWorkers       int
	MaxMessengers    int
	MetricsAddr      string
	Namespace        string
	PercentThreshold []string
	WebConsoleAddr   string
}

// NewServer will create a new Server with the default configuration.
func NewServer() *Server {
	return &Server{
		Backends:         []string{"graphite"},
		ConsoleAddr:      ":8126",
		ExpiryInterval:   5 * time.Minute,
		FlushInterval:    1 * time.Second,
		MaxReaders:       runtime.NumCPU(),
		MaxMessengers:    runtime.NumCPU() * 8,
		MaxWorkers:       runtime.NumCPU() * 8 * 8,
		MetricsAddr:      ":8125",
		PercentThreshold: []string{"90"},
		WebConsoleAddr:   ":8181",
	}
}

// Run runs the server until context signals done.
func (s *Server) Run(ctx context.Context) error {
	// Start the metric aggregator
	backends := make([]backend.MetricSender, 0, len(s.Backends))
	for _, backendName := range s.Backends {
		b, err := backend.InitBackend(backendName)
		if err != nil {
			return err
		}
		backends = append(backends, b)
	}

	var percentThresholds []float64
	for _, sPercentThreshold := range s.PercentThreshold {
		pt, err := strconv.ParseFloat(sPercentThreshold, 64)
		if err != nil {
			return err
		}
		percentThresholds = append(percentThresholds, pt)
	}

	aggregator := NewMetricAggregator(backends, percentThresholds, s.FlushInterval, s.ExpiryInterval, s.MaxWorkers, s.DefaultTags)
	go aggregator.Aggregate()
	s.aggregator = aggregator

	// Start the metric receiver
	f := func(metric *types.Metric) {
		aggregator.MetricQueue <- metric
	}
	cloud, err := cloudprovider.InitCloudProvider(s.CloudProvider)
	if err != nil {
		return err
	}
	receiver := NewMetricReceiver(s.MetricsAddr, s.Namespace, s.MaxReaders, s.MaxMessengers, s.DefaultTags, cloud, HandlerFunc(f))
	go receiver.ListenAndReceive()

	// Start the console(s)
	if s.ConsoleAddr != "" {
		console := ConsoleServer{s.ConsoleAddr, aggregator}
		go console.ListenAndServe()
	}
	if s.WebConsoleAddr != "" {
		console := WebConsoleServer{s.WebConsoleAddr, aggregator}
		go console.ListenAndServe()
	}

	// Listen until done
	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func internalStatName(name string) string {
	return fmt.Sprintf("statsd.%s", name)
}
