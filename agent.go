package agent

import (
	"context"
	"fmt"
	"time"

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

	sysMetrics        *Metrics
	sysMetricsUpdater *MetricsUpdater

	metricsCollectTime  time.Duration
	metricsSendTime     time.Duration
	agentMetrics        *Metrics
	agentMetricsUpdater *MetricsUpdater
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
	a.initSysMetrics()
	a.initAgentMetrics()
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
	start := time.Now()
	err := a.readStats()
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to read sys stats; %v", err)
		})
	}
	a.metricsCollectTime = time.Since(start)

	a.updateSysMetrics(t)

	start = time.Now()
	err = a.sysMetrics.Send(ctx, a.config.CarbonClickHouseAddress)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to send sys stats; %v", err)
		})
	}
	a.metricsSendTime = time.Since(start)

	a.updateAgentMetrics(t)
	err = a.agentMetrics.Send(ctx, a.config.CarbonClickHouseAddress)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to send agent stats; %v", err)
		})
	}

	return nil
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

func (a *Agent) initSysMetrics() {
	var names []string
	names = append(names, fmt.Sprintf("sysstat.%s.cpu.user", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.cpu.nice", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.cpu.sys", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.cpu.iowait", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.mem.total", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.mem.free", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.mem.avail", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.mem.buffers", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.mem.cached", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.mem.swap_cached", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.mem.swap_total", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.mem.swap_free", a.config.ServerID))
	for _, devName := range a.config.DiskDeviceNames {
		names = append(names, fmt.Sprintf("sysstat.%s.disk.%s.read_count", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.disk.%s.read_bytes", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.disk.%s.written_count", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.disk.%s.written_bytes", a.config.ServerID, devName))
	}
	for _, devName := range a.config.NetworkDeviceNames {
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.recv_bytes", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.recv_packets", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.recv_errs", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.recv_drops", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.trans_bytes", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.trans_packets", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.trans_errs", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.trans_drops", a.config.ServerID, devName))
		names = append(names, fmt.Sprintf("sysstat.%s.network.%s.trans_colls", a.config.ServerID, devName))
	}
	names = append(names, fmt.Sprintf("sysstat.%s.loadavg.load1", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.loadavg.load5", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.loadavg.load15", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.uptime.uptime", a.config.ServerID))
	a.sysMetrics = NewMetrics(names)
	a.sysMetricsUpdater = a.sysMetrics.Updater()
}

func (a *Agent) updateSysMetrics(t time.Time) {
	ts := uint32(t.Unix())
	a.sysMetricsUpdater.Reset()
	a.sysMetricsUpdater.UpdateNextMetric(ts, a.cpuStat.UserPercent)
	a.sysMetricsUpdater.UpdateNextMetric(ts, a.cpuStat.NicePercent)
	a.sysMetricsUpdater.UpdateNextMetric(ts, a.cpuStat.SysPercent)
	a.sysMetricsUpdater.UpdateNextMetric(ts, a.cpuStat.IOWaitPercent)
	a.sysMetricsUpdater.UpdateNextMetric(ts, float64(a.memStat.MemTotal))
	a.sysMetricsUpdater.UpdateNextMetric(ts, float64(a.memStat.MemFree))
	a.sysMetricsUpdater.UpdateNextMetric(ts, float64(a.memStat.MemAvailable))
	a.sysMetricsUpdater.UpdateNextMetric(ts, float64(a.memStat.Buffers))
	a.sysMetricsUpdater.UpdateNextMetric(ts, float64(a.memStat.Cached))
	a.sysMetricsUpdater.UpdateNextMetric(ts, float64(a.memStat.SwapCached))
	a.sysMetricsUpdater.UpdateNextMetric(ts, float64(a.memStat.SwapTotal))
	a.sysMetricsUpdater.UpdateNextMetric(ts, float64(a.memStat.SwapFree))
	for j := range a.diskStats {
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.diskStats[j].ReadCountPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.diskStats[j].ReadBytesPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.diskStats[j].WrittenCountPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.diskStats[j].WrittenBytesPerSec)
	}
	for j := range a.netStats {
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].RecvBytesPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].RecvPacketsPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].RecvErrsPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].RecvDropsPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].TransBytesPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].TransPacketsPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].TransErrsPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].TransDropsPerSec)
		a.sysMetricsUpdater.UpdateNextMetric(ts, a.netStats[j].TransCollsPerSec)
	}
	a.sysMetricsUpdater.UpdateNextMetric(ts, a.loadAvg.Load1)
	a.sysMetricsUpdater.UpdateNextMetric(ts, a.loadAvg.Load5)
	a.sysMetricsUpdater.UpdateNextMetric(ts, a.loadAvg.Load15)
	a.sysMetricsUpdater.UpdateNextMetric(ts, a.uptime.Uptime)
}

func (a *Agent) initAgentMetrics() {
	var names []string
	names = append(names, fmt.Sprintf("sysstat.%s.agent.collect_time", a.config.ServerID))
	names = append(names, fmt.Sprintf("sysstat.%s.agent.send_time", a.config.ServerID))
	a.agentMetrics = NewMetrics(names)
	a.agentMetricsUpdater = a.agentMetrics.Updater()
}

func (a *Agent) updateAgentMetrics(t time.Time) {
	ts := uint32(t.Unix())
	a.agentMetricsUpdater.Reset()
	a.agentMetricsUpdater.UpdateNextMetric(ts, a.metricsCollectTime.Seconds())
	a.agentMetricsUpdater.UpdateNextMetric(ts, a.metricsSendTime.Seconds())
}
