package agent

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"

	pb "github.com/hnakamur/carbon-clickhouse-sysstat-agent/carbon"
	"github.com/hnakamur/ltsvlog"
	"github.com/hnakamur/sysstat"
)

// Config is a configuration for Agent.
type Config struct {
	CarbonClickHouseAddress string        `yaml:"carbon_click_house_address"`
	DiskDeviceNames         []string      `yaml:"disk_device_names"`
	NetworkDeviceNames      []string      `yaml:"network_device_names"`
	ServerID                string        `yaml:"server_id"`
	Interval                time.Duration `yaml:"interval"`
	LogFilename             string        `yaml:"log_filename"`
	EnableDebugLog          bool          `yaml:"enable_debug_log"`
}

// Agent is an agent which sends system statistics to carbon-clickhouse.
type Agent struct {
	config *Config

	cpuStatReader  *sysstat.CPUStatReader
	memStatReader  *sysstat.MemoryStatReader
	diskStatReader *sysstat.DiskStatReader
	netStatReader  *sysstat.NetworkStatReader
	loadAvgReader  *sysstat.LoadAvgReader
	uptimeReader   *sysstat.UptimeReader

	cpuStat   sysstat.CPUStat
	memStat   sysstat.MemoryStat
	diskStats []sysstat.DiskStat
	netStats  []sysstat.NetworkStat
	loadAvg   sysstat.LoadAvg
	uptime    sysstat.Uptime

	payload pb.Payload
}

// New creates and returns an agent.
func New(config *Config) (*Agent, error) {
	cpuStatReader, err := sysstat.NewCPUStatReader()
	if err != nil {
		return nil, ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read cpu stats; %v", err)
		}).Stack("")
	}
	memStatReader := sysstat.NewMemoryStatReader()
	diskStatReader, err := sysstat.NewDiskStatReader(config.DiskDeviceNames)
	if err != nil {
		return nil, ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read disk stats; %v", err)
		}).Stack("")
	}
	netStatReader, err := sysstat.NewNetworkStatReader(config.NetworkDeviceNames)
	if err != nil {
		return nil, ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read network stats; %v", err)
		}).Stack("")
	}
	loadAvgReader := sysstat.NewLoadAvgReader()
	uptimeReader := sysstat.NewUptimeReader()

	diskStats := make([]sysstat.DiskStat, len(config.DiskDeviceNames))
	for i, devName := range config.DiskDeviceNames {
		diskStats[i].DevName = devName
	}
	netStats := make([]sysstat.NetworkStat, len(config.NetworkDeviceNames))
	for i, devName := range config.NetworkDeviceNames {
		netStats[i].DevName = devName
	}

	a := &Agent{
		config:         config,
		cpuStatReader:  cpuStatReader,
		memStatReader:  memStatReader,
		diskStatReader: diskStatReader,
		netStatReader:  netStatReader,
		loadAvgReader:  loadAvgReader,
		uptimeReader:   uptimeReader,
		diskStats:      diskStats,
		netStats:       netStats,
	}
	return a, nil
}

// Run runs loop for reading statistics and send them to carbon-clickhouse.
func (a *Agent) Run(ctx context.Context) error {
	ticker := time.NewTicker(a.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			err := a.readAndSendStats(ctx, t)
			if err != nil {
				ltsvlog.Logger.Err(err)
			}
			ltsvlog.Logger.Info().String("msg", "sent stats").UTCTime("t", t).Log()
		case <-ctx.Done():
			return nil
		}
	}
}

func (a *Agent) readAndSendStats(ctx context.Context, t time.Time) error {
	err := a.readStats()
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read stats; %v", err)
		})
	}
	return a.sendStats(ctx, t)
}

func (a *Agent) readStats() error {
	err := a.cpuStatReader.Read(&a.cpuStat)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read cpu stats; %v", err)
		}).Stack("")
	}
	err = a.memStatReader.Read(&a.memStat)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read memory stats; %v", err)
		}).Stack("")
	}
	err = a.diskStatReader.Read(a.diskStats)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read disk stats; %v", err)
		}).Stack("")
	}
	err = a.netStatReader.Read(a.netStats)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read network stats; %v", err)
		}).Stack("")
	}
	err = a.loadAvgReader.Read(&a.loadAvg)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read loadavg; %v", err)
		}).Stack("")
	}
	err = a.uptimeReader.Read(&a.uptime)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read uptime; %v", err)
		}).Stack("")
	}
	return nil
}

func (a *Agent) allocMetrics() {
	a.allocMetric(fmt.Sprintf("sysstat.%s.cpu.user", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.cpu.nice", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.cpu.sys", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.cpu.iowait", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.mem.total", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.mem.free", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.mem.avail", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.mem.buffers", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.mem.cached", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.mem.swap_cached", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.mem.swap_total", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.mem.swap_free", a.config.ServerID))
	for _, devName := range a.config.DiskDeviceNames {
		a.allocMetric(fmt.Sprintf("sysstat.%s.disk.%s.read_count", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.disk.%s.read_bytes", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.disk.%s.written_count", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.disk.%s.written_bytes", a.config.ServerID, devName))
	}
	for _, devName := range a.config.NetworkDeviceNames {
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.recv_bytes", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.recv_packets", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.recv_errs", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.recv_drops", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.trans_bytes", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.trans_packets", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.trans_errs", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.trans_drops", a.config.ServerID, devName))
		a.allocMetric(fmt.Sprintf("sysstat.%s.network.%s.trans_colls", a.config.ServerID, devName))
	}
	a.allocMetric(fmt.Sprintf("sysstat.%s.loadavg.load1", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.loadavg.load5", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.loadavg.load15", a.config.ServerID))
	a.allocMetric(fmt.Sprintf("sysstat.%s.uptime.uptime", a.config.ServerID))
}

func (a *Agent) allocMetric(metricName string) {
	metric := &pb.Metric{
		Metric: metricName,
		Points: []*pb.Point{new(pb.Point)},
	}
	a.payload.Metrics = append(a.payload.Metrics, metric)
}

func (a *Agent) updateMetrics(t time.Time) {
	ts := uint32(t.Unix())
	i := 0
	a.updateMetric(&i, ts, a.cpuStat.UserPercent)
	a.updateMetric(&i, ts, a.cpuStat.NicePercent)
	a.updateMetric(&i, ts, a.cpuStat.SysPercent)
	a.updateMetric(&i, ts, a.cpuStat.IOWaitPercent)
	a.updateMetric(&i, ts, float64(a.memStat.MemTotal))
	a.updateMetric(&i, ts, float64(a.memStat.MemFree))
	a.updateMetric(&i, ts, float64(a.memStat.MemAvailable))
	a.updateMetric(&i, ts, float64(a.memStat.Buffers))
	a.updateMetric(&i, ts, float64(a.memStat.Cached))
	a.updateMetric(&i, ts, float64(a.memStat.SwapCached))
	a.updateMetric(&i, ts, float64(a.memStat.SwapTotal))
	a.updateMetric(&i, ts, float64(a.memStat.SwapFree))
	for i := range a.diskStats {
		a.updateMetric(&i, ts, a.diskStats[i].ReadCountPerSec)
		a.updateMetric(&i, ts, a.diskStats[i].ReadBytesPerSec)
		a.updateMetric(&i, ts, a.diskStats[i].WrittenCountPerSec)
		a.updateMetric(&i, ts, a.diskStats[i].WrittenBytesPerSec)
	}
	for i := range a.netStats {
		a.updateMetric(&i, ts, a.netStats[i].RecvBytesPerSec)
		a.updateMetric(&i, ts, a.netStats[i].RecvPacketsPerSec)
		a.updateMetric(&i, ts, a.netStats[i].RecvErrsPerSec)
		a.updateMetric(&i, ts, a.netStats[i].RecvDropsPerSec)
		a.updateMetric(&i, ts, a.netStats[i].TransBytesPerSec)
		a.updateMetric(&i, ts, a.netStats[i].TransPacketsPerSec)
		a.updateMetric(&i, ts, a.netStats[i].TransErrsPerSec)
		a.updateMetric(&i, ts, a.netStats[i].TransDropsPerSec)
		a.updateMetric(&i, ts, a.netStats[i].TransCollsPerSec)
	}
	a.updateMetric(&i, ts, a.loadAvg.Load1)
	a.updateMetric(&i, ts, a.loadAvg.Load5)
	a.updateMetric(&i, ts, a.loadAvg.Load15)
	a.updateMetric(&i, ts, a.uptime.Uptime)
}

func (a *Agent) updateMetric(i *int, timestamp uint32, value float64) {
	metric := a.payload.Metrics[*i]
	p := metric.Points[0]
	p.Timestamp = timestamp
	p.Value = value
	*i = *i + 1
}

func (a *Agent) sendStats(ctx context.Context, t time.Time) error {
	conn, err := grpc.Dial(a.config.CarbonClickHouseAddress, grpc.WithInsecure())
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to dial to carbon-clickhouse server; %v", err)
		}).Stack("")
	}
	defer conn.Close()

	client := pb.NewCarbonClient(conn)
	a.updateMetrics(t)
	_, err = client.Store(ctx, &a.payload)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to send metrics to carbon-clickhouse server; %v", err)
		}).Stack("")
	}
	return nil
}
