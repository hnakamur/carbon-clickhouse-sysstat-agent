package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	agent "github.com/hnakamur/carbon-clickhouse-sysstat-agent"
	"github.com/hnakamur/ltsvlog"
	yaml "gopkg.in/yaml.v2"
)

func main() {
	configFilename := flag.String("config", "", "config filename")
	flag.Parse()

	var config agent.Config
	err := readConfig(*configFilename, &config)
	if err != nil {
		log.Fatalf("failed to read config file; %v", err)
	}

	logFile, err := os.OpenFile(config.LogFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0x600)
	if err != nil {
		log.Fatalf("failed to create log file; %v", err)
	}
	defer logFile.Close()
	ltsvlog.Logger = ltsvlog.NewLTSVLogger(logFile, config.EnableDebugLog)

	a, err := agent.New(&config)
	if err != nil {
		ltsvlog.Logger.Err(ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to create sysstat agent; %v", err)
		}))
	}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := a.Run(ctx)
		if err != nil {
			ltsvlog.Logger.Err(ltsvlog.WrapErr(err, func(err error) error {
				return fmt.Errorf("failed to run sysstat agent; %v", err)
			}))
		}
	}()

	ltsvlog.Logger.Info().String("msg", "started").Fmt("config", "%+v", config).Log()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	s := <-signals
	ltsvlog.Logger.Info().String("msg", "got signal").Stringer("signal", s).Log()
	cancel()
	wg.Wait()

	ltsvlog.Logger.Info().String("msg", "exiting").Log()
}

func readConfig(filename string, c *agent.Config) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	buf, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(buf, c)
}
