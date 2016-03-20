package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/jtblin/gostatsd/statsd"

	log "github.com/Sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

var (
	// BuildDate is the date when the binary was built
	BuildDate string
	// GitCommit is the commit hash when the binary was built
	GitCommit string
	// Version is the version of the binary
	Version string
)

func main() {
	s := statsd.NewServer()
	err, version, CPUProfile := setConfiguration(s)
	if err != nil {
		log.Fatalf("Error while parsing configuration: %v\n", err)
	}
	if version {
		fmt.Printf("Version: %s - Commit: %s - Date: %s\n", Version, GitCommit, BuildDate)
		return
	}
	exitCode := 0
	defer func(e *int) {
		//pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		if *e != 0 {
			os.Exit(*e)
		}
	}(&exitCode)
	if CPUProfile != "" {
		f, err := os.Create(CPUProfile)
		if err != nil {
			log.Fatalf("Failed to open profile file: %v", err)
		}
		defer func() {
			if err := f.Close(); err != nil {
				log.Errorf("Failed to close profile file: %v", err)
			}
		}()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	cancelOnInterrupt(cancelFunc)

	log.Info("Starting server")

	if err := s.Run(ctx); err != nil {
		if err != context.Canceled {
			exitCode = 1
			log.Errorf("%v", err)
		}
	}
}

// cancelOnInterrupt calls f when os.Interrupt or SIGTERM is received
func cancelOnInterrupt(f context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			f()
		}
	}()
}

// addConfigFlags adds flags supported by Config to the specified FlagSet
func addConfigFlags(s *statsd.Server, fs *pflag.FlagSet) {
	fs.StringSliceVar(&s.Backends, "backends", s.Backends, "Comma-separated list of backends")
	fs.StringVar(&s.ConsoleAddr, "console-addr", s.ConsoleAddr, "If set, use as the address of the telnet-based console")
	fs.StringVar(&s.CloudProvider, "cloud-provider", s.CloudProvider, "If set, use the cloud provider to retrieve metadata about the sender")
	fs.StringSliceVar(&s.DefaultTags, "default-tags", s.DefaultTags, "Default tags to add to the metrics")
	fs.DurationVar(&s.ExpiryInterval, "expiry-interval", s.ExpiryInterval, "After how long do we expire metrics (0 to disable)")
	fs.DurationVar(&s.FlushInterval, "flush-interval", s.FlushInterval, "How often to flush metrics to the backends")
	fs.IntVar(&s.MaxReaders, "max-readers", s.MaxReaders, "Maximum number of socket readers")
	fs.IntVar(&s.MaxMessengers, "max-messengers", s.MaxMessengers, "Maximum number of workers to process messages")
	fs.IntVar(&s.MaxWorkers, "max-workers", s.MaxWorkers, "Maximum number of workers to process metrics")
	fs.StringVar(&s.MetricsAddr, "metrics-addr", s.MetricsAddr, "Address on which to listen for metrics")
	fs.StringVar(&s.Namespace, "namespace", s.Namespace, "Namespace all metrics")
	fs.StringSliceVar(&s.PercentThreshold, "percent-threshold", s.PercentThreshold, "Comma-separated list of percentiles")
	fs.StringVar(&s.WebConsoleAddr, "web-addr", s.WebConsoleAddr, "If set, use as the address of the web-based console")
}

func setConfiguration(s *statsd.Server) (error, bool, string) {
	var configPath, profile string
	var verbose, version, json bool

	addConfigFlags(s, pflag.CommandLine)
	pflag.CommandLine.StringVar(&configPath, "config-path", "", "Path to the configuration file")
	pflag.CommandLine.BoolVar(&verbose, "verbose", false, "Verbose")
	pflag.CommandLine.BoolVar(&json, "json", false, "Log in JSON format")
	pflag.CommandLine.BoolVar(&version, "version", false, "Print the version and exit")
	pflag.CommandLine.StringVar(&profile, "cpu-profile", "", "Use profiler and write results to this file")
	pflag.Parse()

	if verbose {
		log.SetLevel(log.DebugLevel)
	}
	if json {
		log.SetFormatter(&log.JSONFormatter{})
	}

	if configPath != "" {
		viper.SetConfigFile(configPath)
		if err := viper.ReadInConfig(); err != nil {
			return err, false, ""
		}
	}

	return nil, version, profile
}
