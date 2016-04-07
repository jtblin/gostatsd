package statsd

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/jtblin/gostatsd/cloudprovider"
	"github.com/jtblin/gostatsd/types"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

const packetSizeUDP = 1500

// Handler interface can be used to handle metrics for a MetricReceiver.
type Handler interface {
	HandleMetric(ctx context.Context, m *types.Metric)
}

// The HandlerFunc type is an adapter to allow the use of ordinary functions as metric handlers.
type HandlerFunc func(context.Context, *types.Metric)

// HandleMetric calls f(m).
func (f HandlerFunc) HandleMetric(ctx context.Context, m *types.Metric) {
	f(ctx, m)
}

// MetricReceiver receives data on its listening port and converts lines in to Metrics.
// For each types.Metric it calls r.Handler.HandleMetric()
type MetricReceiver struct {
	Cloud     cloudprovider.Interface // Cloud provider interface
	Handler   Handler                 // handler to invoke
	Namespace string                  // Namespace to prefix all metrics
	Tags      types.Tags              // Tags to add to all metrics
}

// NewMetricReceiver initialises a new MetricReceiver.
func NewMetricReceiver(ns string, tags []string, cloud cloudprovider.Interface, handler Handler) *MetricReceiver {
	return &MetricReceiver{
		Cloud:     cloud,
		Handler:   handler,
		Namespace: ns,
		Tags:      tags,
	}
}

// increment allows counting server stats using default tags.
func (mr *MetricReceiver) increment(ctx context.Context, name string, value int) {
	mr.Handler.HandleMetric(ctx, types.NewMetric(internalStatName(name), float64(value), types.COUNTER, mr.Tags))
}

// Receive accepts incoming datagrams on c and calls mr.handleMessage() for each message.
func (mr *MetricReceiver) Receive(ctx context.Context, c net.PacketConn) {
	buf := make([]byte, packetSizeUDP)
	for {
		// This will error out when the socket is closed.
		nbytes, addr, err := c.ReadFrom(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && !netErr.Temporary() {
				select {
				case <-ctx.Done():
				default:
					log.Errorf("Non-temporary error reading from socket: %v", err)
				}
				return
			}
			log.Warnf("Error reading from socket: %v", err)
			continue
		}
		mr.increment(ctx, "packets_received", 1)
		mr.handleMessage(ctx, addr, buf[:nbytes])
	}
}

// handleMessage handles the contents of a datagram and call r.Handler.HandleMetric()
// for each line that successfully parses in to a types.Metric.
func (mr *MetricReceiver) handleMessage(ctx context.Context, addr net.Addr, msg []byte) {
	numMetrics := 0
	var triedToGetTags bool
	var additionalTags types.Tags
	buf := bytes.NewBuffer(msg)
	for {
		line, readerr := buf.ReadBytes('\n')

		// protocol does not require line to end in \n, if EOF use received line if valid
		if readerr != nil && readerr != io.EOF {
			log.Warnf("Error reading message from %s: %v", addr, readerr)
			return
		} else if readerr != io.EOF {
			// remove newline, only if not EOF
			if len(line) > 0 {
				line = line[:len(line)-1]
			}
		}

		if len(line) > 1 {
			metric, err := mr.parseLine(line)
			if err != nil {
				// logging as debug to avoid spamming logs when a bad actor sends
				// badly formatted messages
				log.Debugf("Error parsing line %q from %s: %v", line, addr, err)
				mr.increment(ctx, "bad_lines_seen", 1)
				continue
			}
			if !triedToGetTags {
				triedToGetTags = true
				additionalTags = mr.getAdditionalTags(addr.String())
			}
			if len(additionalTags) > 0 {
				metric.Tags = append(metric.Tags, additionalTags...)
				log.Debugf("Metric tags: %v", metric.Tags)
			}
			mr.Handler.HandleMetric(ctx, metric)
			numMetrics++
		}

		if readerr == io.EOF {
			// if was EOF, finished handling
			mr.increment(ctx, "metrics_received", numMetrics)
			return
		}
	}
}

func (mr *MetricReceiver) getAdditionalTags(addr string) types.Tags {
	n := strings.IndexByte(addr, ':')
	if n <= 1 {
		return nil
	}
	hostname := addr[0:n]
	if net.ParseIP(hostname) != nil {
		tags := make(types.Tags, 0, 16)
		if mr.Cloud != nil {
			instance, err := cloudprovider.GetInstance(mr.Cloud, hostname)
			if err != nil {
				log.Warnf("Error retrieving instance details from cloud provider %s: %v", mr.Cloud.ProviderName(), err)
			} else {
				hostname = instance.ID
				tags = append(tags, fmt.Sprintf("region:%s", instance.Region))
				tags = append(tags, instance.Tags...)
			}
		}
		tags = append(tags, fmt.Sprintf("%s:%s", types.StatsdSourceID, hostname))
		return tags
	}
	return nil
}

// ParseLine with lexer impl.
func (mr *MetricReceiver) parseLine(line []byte) (*types.Metric, error) {
	llen := len(line)
	if llen == 0 {
		return nil, nil
	}
	metric := &types.Metric{}
	metric.Tags = append(metric.Tags, mr.Tags...)
	l := &lexer{input: line, len: llen, m: metric, namespace: mr.Namespace}
	return l.run()
}
