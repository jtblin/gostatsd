package main

import (
	"log"
	"net"

	"github.com/jtblin/gostatsd/statsd"
	"github.com/jtblin/gostatsd/types"

	"golang.org/x/net/context"
)

func main() {
	f := func(ctx context.Context, m *types.Metric) {
		log.Printf("%s", m)
	}
	r := statsd.MetricReceiver{
		Namespace: "stats",
		Handler:   statsd.HandlerFunc(f),
	}
	c, err := net.ListenPacket("udp", ":8125")
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	r.Receive(context.TODO(), c)
}
