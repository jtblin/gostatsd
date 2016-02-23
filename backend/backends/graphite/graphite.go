package graphite

import (
	"bytes"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	"github.com/spf13/viper"
)

const backendName = "graphite"

func init() {
	viper.SetDefault("graphite.address", "localhost:2003")
	backend.RegisterBackend(backendName, func() (backend.MetricSender, error) {
		return NewGraphiteClient()
	})
}

const sampleConfig = `
[graphite]
	# graphite host or ip address
	address = "ip:2003"
`

// Regular expressions used for bucket name normalization
var regSemiColon = regexp.MustCompile(":")

// normalizeBucketName cleans up a bucket name by replacing or translating invalid characters
func normalizeBucketName(bucket string, tagsKey string) string {
	tags := strings.Split(tagsKey, ",")
	for _, tag := range tags {
		bucket += "." + regSemiColon.ReplaceAllString(tag, "_")
	}
	return bucket
}

// GraphiteClient is an object that is used to send messages to a Graphite server's TCP interface
type GraphiteClient struct {
	address string
}

// SendMetrics sends the metrics in a MetricsMap to the Graphite server
func (client *GraphiteClient) SendMetrics(metrics types.MetricMap) error {
	if metrics.NumStats == 0 {
		return nil
	}
	buf := new(bytes.Buffer)
	now := time.Now().Unix()
	types.EachCounter(metrics.Counters, func(key, tagsKey string, counter types.Counter) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats_count.%s %d %d\n", nk, counter.Value, now)
		fmt.Fprintf(buf, "stats.%s %f %d\n", nk, counter.PerSecond, now)
	})
	types.EachTimer(metrics.Timers, func(key, tagsKey string, timer types.Timer) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.timers.%s.lower %f %d\n", nk, timer.Min, now)
		fmt.Fprintf(buf, "stats.timers.%s.upper %f %d\n", nk, timer.Max, now)
		fmt.Fprintf(buf, "stats.timers.%s.count %f %d\n", nk, timer.Count, now)
		fmt.Fprintf(buf, "stats.timers.%s.count_ps %f %d\n", nk, timer.PerSecond, now)
		fmt.Fprintf(buf, "stats.timers.%s.mean %f %d\n", nk, timer.Mean, now)
		fmt.Fprintf(buf, "stats.timers.%s.median %f %d\n", nk, timer.Median, now)
		fmt.Fprintf(buf, "stats.timers.%s.sum %f %d\n", nk, timer.Sum, now)
		fmt.Fprintf(buf, "stats.timers.%s.sum %f %d\n", nk, timer.SumSquares, now)
		fmt.Fprintf(buf, "stats.timers.%s.sum_squares %f %d\n", nk, timer.StdDev, now)
		for _, pct := range timer.Percentiles {
			fmt.Fprintf(buf, "stats.timers.%s.%s %f %d\n", nk, pct.String(), pct.Float(), now)
		}
	})
	types.EachGauge(metrics.Gauges, func(key, tagsKey string, gauge types.Gauge) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.gauge.%s %f %d\n", nk, gauge.Value, now)
	})

	types.EachSet(metrics.Sets, func(key, tagsKey string, set types.Set) {
		nk := normalizeBucketName(key, tagsKey)
		fmt.Fprintf(buf, "stats.sets.%s %d %d\n", nk, len(set.Values), now)
	})

	fmt.Fprintf(buf, "statsd.numStats %d %d\n", metrics.NumStats, now)
	fmt.Fprintf(buf, "statsd.processingTime %f %d\n", float64(metrics.ProcessingTime)/float64(time.Millisecond), now)

	conn, err := net.Dial("tcp", client.address)
	if err != nil {
		return fmt.Errorf("error connecting to graphite backend: %s", err)
	}
	defer conn.Close()

	_, err = buf.WriteTo(conn)
	if err != nil {
		return fmt.Errorf("error sending to graphite: %s", err)
	}
	return nil
}

// SampleConfig returns the sample config for the graphite backend
func (g *GraphiteClient) SampleConfig() string {
	return sampleConfig
}

// NewGraphiteClient constructs a GraphiteClient object by connecting to an address
func NewGraphiteClient() (backend.MetricSender, error) {
	return &GraphiteClient{viper.GetString("graphite.address")}, nil
}

func (client *GraphiteClient) Name() string {
	return backendName
}
