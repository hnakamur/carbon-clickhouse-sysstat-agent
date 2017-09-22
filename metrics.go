package agent

import (
	"context"
	"fmt"

	pb "github.com/hnakamur/carbon-clickhouse-sysstat-agent/carbon"
	"github.com/hnakamur/ltsvlog"
	"google.golang.org/grpc"
)

// Metrics is a struct type for holding a set of metrics.
type Metrics struct {
	metrics []*pb.Metric
}

// MetricsUpdator is a type for updating timestamp and value of metrics.
type MetricsUpdater struct {
	m *Metrics
	i int
}

// NewMetrics returns a Metrics for specified metrics names.
func NewMetrics(names []string) *Metrics {
	m := new(Metrics)
	for _, name := range names {
		metric := &pb.Metric{
			Metric: name,
			Points: []*pb.Point{new(pb.Point)},
		}
		m.metrics = append(m.metrics, metric)
	}
	return m
}

// Updater returns a updateor for this metrics.
func (m *Metrics) Updater() *MetricsUpdater {
	return &MetricsUpdater{m: m}
}

// Send sends metrics to the specified carbon-clickhouse server via gRPC.
func (m *Metrics) Send(ctx context.Context, address string) error {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to dial to carbon-clickhouse server; %v", err)
		}).Stack("")
	}
	defer conn.Close()

	client := pb.NewCarbonClient(conn)
	payload := pb.Payload{Metrics: m.metrics}
	_, err = client.Store(ctx, &payload)
	if err != nil {
		return ltsvlog.WrapErr(err, func(err error) error {
			return fmt.Errorf("failed to send metrics to carbon-clickhouse server; %v", err)
		}).Stack("")
	}
	return nil
}

// Reset resets the internal index for updating a metric value to zero.
func (u *MetricsUpdater) Reset() {
	u.i = 0
}

// UpdateNextMetric updates the timestamp and the value of the next metric
// and increment the internal index.
func (u *MetricsUpdater) UpdateNextMetric(timestamp uint32, value float64) {
	metric := u.m.metrics[u.i]
	p := metric.Points[0]
	p.Timestamp = timestamp
	p.Value = value
	u.i++
}
