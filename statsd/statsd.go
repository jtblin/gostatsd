package statsd

import (
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jtblin/gostatsd/backend"
	_ "github.com/jtblin/gostatsd/backend/backends" // import backends for initialisation
	"github.com/jtblin/gostatsd/cloudprovider"
	_ "github.com/jtblin/gostatsd/cloudprovider/providers" // import cloud providers for initialisation

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

// DefaultBackends is the list of default backends.
var DefaultBackends = []string{"graphite"}

// DefaultMaxReaders is the default number of socket reading goroutines.
var DefaultMaxReaders = runtime.NumCPU()

// DefaultMaxWorkers is the default number of goroutines that aggregate metrics.
var DefaultMaxWorkers = runtime.NumCPU()

// DefaultPercentThreshold is the default list of applied percentiles.
var DefaultPercentThreshold = []string{"90"}

// DefaultTags is the default list of additional tags.
var DefaultTags = []string{""}

const (
	// DefaultExpiryInterval is the default expiry interval for metrics.
	DefaultExpiryInterval = 5 * time.Minute
	// DefaultFlushInterval is the default metrics flush interval.
	DefaultFlushInterval = 1 * time.Second
	// DefaultMetricsAddr is the default address on which to listen for metrics.
	DefaultMetricsAddr = ":8125"
	// DefaultMaxQueueSize is the default maximum number of buffered metrics per worker.
	DefaultMaxQueueSize     = 10000 // arbitrary
	defaultLastFlushTimeout = 2 * time.Second
)

const (
	// ParamBackends is the name of parameter with backends.
	ParamBackends = "backends"
	// ParamConsoleAddr is the name of parameter with console address.
	ParamConsoleAddr = "console-addr"
	// ParamCloudProvider is the name of parameter with the name of cloud provider.
	ParamCloudProvider = "cloud-provider"
	// ParamDefaultTags is the name of parameter with the list of additional tags.
	ParamDefaultTags = "default-tags"
	// ParamExpiryInterval is the name of parameter with expiry interval for metrics.
	ParamExpiryInterval = "expiry-interval"
	// ParamFlushInterval is the name of parameter with metrics flush interval.
	ParamFlushInterval = "flush-interval"
	// ParamMaxReaders is the name of parameter with number of socket readers.
	ParamMaxReaders = "max-readers"
	// ParamMaxWorkers is the name of parameter with number of goroutines that aggregate metrics.
	ParamMaxWorkers = "max-workers"
	// ParamMaxQueueSize is the name of parameter with maximum number of buffered metrics per worker.
	ParamMaxQueueSize = "max-queue-size"
	// ParamMetricsAddr is the name of parameter with address on which to listen for metrics.
	ParamMetricsAddr = "metrics-addr"
	// ParamNamespace is the name of parameter with namespace for all metrics.
	ParamNamespace = "namespace"
	// ParamPercentThreshold is the name of parameter with list of applied percentiles.
	ParamPercentThreshold = "percent-threshold"
	// ParamWebAddr is the name of parameter with the address of the web-based console.
	ParamWebAddr = "web-addr"
)

// Server encapsulates all of the parameters necessary for starting up
// the statsd server. These can either be set via command line or directly.
type Server struct {
	Backends         []string
	ConsoleAddr      string
	CloudProvider    string
	DefaultTags      []string
	ExpiryInterval   time.Duration
	FlushInterval    time.Duration
	MaxReaders       int
	MaxWorkers       int
	MaxQueueSize     int
	MaxMessengers    int
	MetricsAddr      string
	Namespace        string
	PercentThreshold []string
	WebConsoleAddr   string
	Viper            *viper.Viper
}

// NewServer will create a new Server with the default configuration.
func NewServer() *Server {
	return &Server{
		Backends:         DefaultBackends,
		ConsoleAddr:      DefaultConsoleAddr,
		DefaultTags:      DefaultTags,
		ExpiryInterval:   DefaultExpiryInterval,
		FlushInterval:    DefaultFlushInterval,
		MaxReaders:       DefaultMaxReaders,
		MaxWorkers:       DefaultMaxWorkers,
		MaxQueueSize:     DefaultMaxQueueSize,
		MetricsAddr:      DefaultMetricsAddr,
		PercentThreshold: DefaultPercentThreshold,
		WebConsoleAddr:   DefaultWebConsoleAddr,
		Viper:            viper.New(),
	}
}

// AddFlags adds flags to the specified FlagSet.
func AddFlags(fs *pflag.FlagSet) {
	fs.String(ParamConsoleAddr, DefaultConsoleAddr, "If set, use as the address of the telnet-based console")
	fs.String(ParamCloudProvider, "", "If set, use the cloud provider to retrieve metadata about the sender")
	fs.Duration(ParamExpiryInterval, DefaultExpiryInterval, "After how long do we expire metrics (0 to disable)")
	fs.Duration(ParamFlushInterval, DefaultFlushInterval, "How often to flush metrics to the backends")
	fs.Int(ParamMaxReaders, DefaultMaxReaders, "Maximum number of socket readers")
	fs.Int(ParamMaxWorkers, DefaultMaxWorkers, "Maximum number of workers to process metrics")
	fs.Int(ParamMaxQueueSize, DefaultMaxQueueSize, "Maximum number of buffered metrics per worker")
	fs.String(ParamMetricsAddr, DefaultMetricsAddr, "Address on which to listen for metrics")
	fs.String(ParamNamespace, "", "Namespace all metrics")
	fs.String(ParamWebAddr, DefaultWebConsoleAddr, "If set, use as the address of the web-based console")
	//TODO Remove workaround when https://github.com/spf13/viper/issues/112 is fixed
	fs.String(ParamBackends, strings.Join(DefaultBackends, ","), "Comma-separated list of backends")
	fs.String(ParamDefaultTags, strings.Join(DefaultTags, ","), "Comma-separated list of tags to add to all metrics")
	fs.String(ParamPercentThreshold, strings.Join(DefaultPercentThreshold, ","), "Comma-separated list of percentiles")
}

// Run runs the server until context signals done.
func (s *Server) Run(ctx context.Context) error {
	// Start the metric aggregator
	backends := make([]backend.MetricSender, 0, len(s.Backends))
	for _, backendName := range s.Backends {
		b, err := backend.InitBackend(backendName, s.Viper)
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

	cloud, err := cloudprovider.InitCloudProvider(s.CloudProvider, s.Viper)
	if err != nil {
		return err
	}

	factory := func() Aggregator {
		return NewMetricAggregator(percentThresholds, s.FlushInterval, s.ExpiryInterval, s.DefaultTags)
	}
	dispatcher := NewDispatcher(s.MaxWorkers, s.MaxQueueSize, defaultLastFlushTimeout, s.FlushInterval, AggregatorFactoryFunc(factory), backends)

	var wgDispatcher sync.WaitGroup
	defer wgDispatcher.Wait()                                       // Wait for dispatcher to shutdown
	ctxDisp, cancelDisp := context.WithCancel(context.Background()) // Separate context!
	defer cancelDisp()                                              // Tell the dispatcher to shutdown
	wgDispatcher.Add(1)
	go func() {
		defer wgDispatcher.Done()
		if err := dispatcher.Run(ctxDisp); err != nil && err != context.Canceled {
			log.Panicf("Dispatcher quit unexpectedly: %v", err)
		}
	}()
	var wgReceiver sync.WaitGroup
	defer wgReceiver.Wait() // Wait for all receivers to finish

	// Start the metric receiver
	c, err := net.ListenPacket("udp", s.MetricsAddr)
	if err != nil {
		return err
	}
	defer func() {
		// This makes receivers error out and stop
		if err := c.Close(); err != nil {
			log.Warnf("Error closing socket: %v", err)
		}
	}()

	receiver := NewMetricReceiver(s.Namespace, s.DefaultTags, cloud, HandlerFunc(dispatcher.DispatchMetric))
	wgReceiver.Add(s.MaxReaders)
	for r := 0; r < s.MaxReaders; r++ {
		go func() {
			defer wgReceiver.Done()
			if err := receiver.Receive(ctx, c); err != nil && err != context.Canceled {
				log.Errorf("Received exited with error: %v", err)
			}
		}()
	}

	// Start the console(s)
	//if s.ConsoleAddr != "" {
	//	console := ConsoleServer{s.ConsoleAddr, aggregator}
	//	go console.ListenAndServe()
	//}
	//if s.WebConsoleAddr != "" {
	//	console := WebConsoleServer{s.WebConsoleAddr, aggregator}
	//	go console.ListenAndServe()
	//}

	// Listen until done
	<-ctx.Done()
	return ctx.Err()
}

func internalStatName(name string) string {
	return fmt.Sprintf("statsd.%s", name)
}
