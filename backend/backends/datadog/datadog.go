package datadog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jtblin/gostatsd/backend"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	apiURL      = "https://app.datadoghq.com/api/v1/series"
	backendName = "datadog"
	GAUGE       = "gauge"
)

// Datadog represents a Datadog client
type Datadog struct {
	ApiKey   string
	ApiURL   string
	Hostname string
	Client   *http.Client
}

const sampleConfig = `
[datadog]
	## Datadog API key
	api_key = "my-secret-key" # required.

	## Connection timeout.
	# timeout = "5s"
`

type TimeSeries struct {
	Series    []*Metric `json:"series"`
	Timestamp int64     `json:"-"`
	Hostname  string    `json:"-"`
}

type Metric struct {
	Metric string   `json:"metric"`
	Points [1]Point `json:"points"`
	Host   string   `json:"host"`
	Tags   []string `json:"tags,omitempty"`
	Type   string   `json:"type,omitempty"`
}

type Point [2]float64

// AddMetric adds a metric to the series
func (ts *TimeSeries) AddMetric(name, tags, metricType string, value float64) {
	metric := &Metric{
		Host:   ts.Hostname, // TODO: retrieve from tags or remove?
		Metric: name,
		Points: [1]Point{{float64(ts.Timestamp), value}},
		Tags:   strings.Split(tags, ","),
		Type:   metricType,
	}
	ts.Series = append(ts.Series, metric)
}

func (d *Datadog) SendMetrics(metrics types.MetricMap) error {
	if metrics.NumStats == 0 {
		return nil
	}
	ts := TimeSeries{Timestamp: time.Now().Unix(), Hostname: d.Hostname}

	types.EachCounter(metrics.Counters, func(key, tagsKey string, counter types.Counter) {
		ts.AddMetric(fmt.Sprintf("%s.counter", key), tagsKey, GAUGE, counter.PerSecond)
		ts.AddMetric(fmt.Sprintf("%s.counter.count", key), tagsKey, GAUGE, float64(counter.Value))
	})

	types.EachTimer(metrics.Timers, func(key, tagsKey string, timer types.Timer) {
		ts.AddMetric(fmt.Sprintf("%s.timer.lower", key), tagsKey, GAUGE, timer.Min)
		ts.AddMetric(fmt.Sprintf("%s.timer.upper", key), tagsKey, GAUGE, timer.Max)
		ts.AddMetric(fmt.Sprintf("%s.timer.count", key), tagsKey, GAUGE, float64(timer.Count))
		ts.AddMetric(fmt.Sprintf("%s.timer.count_ps", key), tagsKey, GAUGE, float64(timer.PerSecond))
		ts.AddMetric(fmt.Sprintf("%s.timer.mean", key), tagsKey, GAUGE, float64(timer.Mean))
		ts.AddMetric(fmt.Sprintf("%s.timer.median", key), tagsKey, GAUGE, float64(timer.Median))
		ts.AddMetric(fmt.Sprintf("%s.timer.std", key), tagsKey, GAUGE, float64(timer.StdDev))
		ts.AddMetric(fmt.Sprintf("%s.timer.sum", key), tagsKey, GAUGE, float64(timer.Sum))
		ts.AddMetric(fmt.Sprintf("%s.timer.sum_squares", key), tagsKey, GAUGE, float64(timer.SumSquares))
		for _, pct := range timer.Percentiles {
			ts.AddMetric(fmt.Sprintf("%s.timer.%s", key, pct.String()), tagsKey, GAUGE, pct.Float())
		}
	})

	types.EachGauge(metrics.Gauges, func(key, tagsKey string, gauge types.Gauge) {
		ts.AddMetric(fmt.Sprintf("%s.gauge", key), tagsKey, GAUGE, gauge.Value)
	})

	types.EachSet(metrics.Sets, func(key, tagsKey string, set types.Set) {
		ts.AddMetric(fmt.Sprintf("%s.set", key), tagsKey, GAUGE, float64(len(set.Values)))
	})

	ts.AddMetric("statsd.numStats", "", GAUGE, float64(metrics.NumStats))
	ts.AddMetric("statsd.processingTime", "", GAUGE, float64(metrics.ProcessingTime)/float64(time.Millisecond))

	tsBytes, err := json.Marshal(ts)
	log.Debugf("json: %s", string(tsBytes))
	if err != nil {
		return fmt.Errorf("unable to marshal TimeSeries, %s\n", err.Error())
	}
	req, err := http.NewRequest("POST", d.authenticatedUrl(), bytes.NewBuffer(tsBytes))
	if err != nil {
		return fmt.Errorf("unable to create http.Request, %s\n", err.Error())
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := d.Client.Do(req)
	if err != nil {
		return fmt.Errorf("error POSTing metrics, %s\n", err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 209 {
		return fmt.Errorf("received bad status code, %d\n", resp.StatusCode)
	}

	return nil
}

// SampleConfig returns the sample config for the datadog backend
func (d *Datadog) SampleConfig() string {
	return sampleConfig
}

// Name returns the name of the backend
func (d *Datadog) Name() string {
	return backendName
}

func (d *Datadog) authenticatedUrl() string {
	q := url.Values{
		"api_key": []string{d.ApiKey},
	}
	return fmt.Sprintf("%s?%s", d.ApiURL, q.Encode())
}

// NewDatadog returns a new Datadog API client
func NewDatadog() (*Datadog, error) {
	if viper.GetString("datadog.api_key") == "" {
		return nil, fmt.Errorf("api_key is a required field for datadog backend")
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	return &Datadog{
		ApiKey:   viper.GetString("datadog.api_key"),
		ApiURL:   apiURL,
		Hostname: hostname,
		Client: &http.Client{
			Timeout: viper.GetDuration("datadog.timeout"),
		},
	}, nil
}

func init() {
	viper.SetDefault("datadog.timeout", time.Duration(5)*time.Second)
	backend.RegisterBackend(backendName, func() (backend.MetricSender, error) {
		return NewDatadog()
	})
}
