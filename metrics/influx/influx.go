package metrics

import (
	"fmt"
	"time"

	"github.com/prebid/prebid-cache/config"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
	influxdb "github.com/vrischmann/go-metrics-influxdb"
)

var TenSeconds time.Duration = time.Second * 10

const MetricsInfluxDB = "InfluxDB"

type InfluxMetrics struct {
	Registry    metrics.Registry
	Puts        *InfluxMetricsEntry
	Gets        *InfluxMetricsEntry
	PutsBackend *InfluxMetricsEntryByFormat
	GetsBackend *InfluxMetricsEntry
	GetsErr     *InfluxMetricsGetErrors
	Connections *InfluxConnectionMetrics
	MetricsName string
}

type InfluxMetricsEntry struct {
	Duration   metrics.Timer
	Errors     metrics.Meter
	BadRequest metrics.Meter
	Request    metrics.Meter
}

type InfluxMetricsEntryByFormat struct {
	Duration       metrics.Timer
	Request        metrics.Meter
	Errors         metrics.Meter
	BadRequest     metrics.Meter
	JsonRequest    metrics.Meter
	XmlRequest     metrics.Meter
	InvalidRequest metrics.Meter
	RequestLength  metrics.Histogram
	RequestTTL     metrics.Timer
}

type InfluxConnectionMetrics struct {
	ActiveConnections      metrics.Counter
	ConnectionCloseErrors  metrics.Meter
	ConnectionAcceptErrors metrics.Meter
}

type InfluxMetricsGetErrors struct {
	KeyNotFoundErrors metrics.Meter
	MissingKeyErrors  metrics.Meter
}

func NewInfluxGetErrorMetrics(name string, r metrics.Registry) *InfluxMetricsGetErrors {
	return &InfluxMetricsGetErrors{
		KeyNotFoundErrors: metrics.GetOrRegisterMeter(fmt.Sprintf("%s.key_not_found", name), r),
		MissingKeyErrors:  metrics.GetOrRegisterMeter(fmt.Sprintf("%s.missing_key", name), r),
	}
}

func NewInfluxMetricsEntry(name string, r metrics.Registry) *InfluxMetricsEntry {
	return &InfluxMetricsEntry{
		Duration:   metrics.GetOrRegisterTimer(fmt.Sprintf("%s.request_duration", name), r),
		Errors:     metrics.GetOrRegisterMeter(fmt.Sprintf("%s.error_count", name), r),
		BadRequest: metrics.GetOrRegisterMeter(fmt.Sprintf("%s.bad_request_count", name), r),
		Request:    metrics.GetOrRegisterMeter(fmt.Sprintf("%s.request_count", name), r),
	}
}

func NewInfluxMetricsEntryBackendPuts(name string, r metrics.Registry) *InfluxMetricsEntryByFormat {
	return &InfluxMetricsEntryByFormat{
		Duration:       metrics.GetOrRegisterTimer(fmt.Sprintf("%s.request_duration", name), r),
		Errors:         metrics.GetOrRegisterMeter(fmt.Sprintf("%s.error_count", name), r),
		BadRequest:     metrics.GetOrRegisterMeter(fmt.Sprintf("%s.bad_request_count", name), r),
		JsonRequest:    metrics.GetOrRegisterMeter(fmt.Sprintf("%s.json_request_count", name), r),
		XmlRequest:     metrics.GetOrRegisterMeter(fmt.Sprintf("%s.xml_request_count", name), r),
		InvalidRequest: metrics.GetOrRegisterMeter(fmt.Sprintf("%s.unknown_request_count", name), r),
		RequestLength:  metrics.GetOrRegisterHistogram(name+".request_size_bytes", r, metrics.NewExpDecaySample(1028, 0.015)),
		RequestTTL:     metrics.GetOrRegisterTimer(fmt.Sprintf("%s.request_ttl_seconds", name), r),
	}
}

func NewInfluxConnectionMetrics(r metrics.Registry) *InfluxConnectionMetrics {
	return &InfluxConnectionMetrics{
		ActiveConnections:      metrics.GetOrRegisterCounter("connections.active_incoming", r),
		ConnectionAcceptErrors: metrics.GetOrRegisterMeter("connections.accept_errors", r),
		ConnectionCloseErrors:  metrics.GetOrRegisterMeter("connections.close_errors", r),
	}
}

func CreateInfluxMetrics() *InfluxMetrics {
	flushTime := TenSeconds
	r := metrics.NewPrefixedRegistry("prebidcache.")
	m := &InfluxMetrics{
		Registry:    r,
		Puts:        NewInfluxMetricsEntry("puts.current_url", r),
		Gets:        NewInfluxMetricsEntry("gets.current_url", r),
		PutsBackend: NewInfluxMetricsEntryBackendPuts("puts.backend", r),
		GetsBackend: NewInfluxMetricsEntry("gets.backend", r),
		GetsErr:     NewInfluxGetErrorMetrics("gets.backend_error", r),
		Connections: NewInfluxConnectionMetrics(r),
		MetricsName: MetricsInfluxDB,
	}

	metrics.RegisterDebugGCStats(m.Registry)
	metrics.RegisterRuntimeMemStats(m.Registry)

	go metrics.CaptureRuntimeMemStats(m.Registry, flushTime)
	go metrics.CaptureDebugGCStats(m.Registry, flushTime)

	return m
}

// Export begins sending metrics to the configured database.
// This method blocks indefinitely, so it should probably be run in a goroutine.
func (m InfluxMetrics) Export(cfg config.Metrics) {

	logrus.Infof("Metrics will be exported to Influx with host=%s, db=%s, username=%s", cfg.Influx.Host, cfg.Influx.Database, cfg.Influx.Username)
	influxdb.InfluxDB(
		m.Registry,
		TenSeconds,
		cfg.Influx.Host,
		cfg.Influx.Database,
		cfg.Influx.Username,
		cfg.Influx.Password,
	)
	return
}

func (m *InfluxMetrics) GetEngineRegistry() interface{} {
	return &m.Registry
}

func (m *InfluxMetrics) GetMetricsEngineName() string {
	return m.MetricsName
}

func (m *InfluxMetrics) RecordPutError() {
	m.Puts.Errors.Mark(1)
}

func (m *InfluxMetrics) RecordPutBadRequest() {
	m.Puts.BadRequest.Mark(1)
}

func (m *InfluxMetrics) RecordPutTotal() {
	m.Puts.Request.Mark(1)
}

func (m *InfluxMetrics) RecordPutDuration(duration time.Duration) {
	m.Puts.Duration.Update(duration)
}

func (m *InfluxMetrics) RecordGetError() {
	m.Gets.Errors.Mark(1)
}

func (m *InfluxMetrics) RecordGetBadRequest() {
	m.Gets.BadRequest.Mark(1)
}

func (m *InfluxMetrics) RecordGetTotal() {
	m.Gets.Request.Mark(1)
}

func (m *InfluxMetrics) RecordGetDuration(duration time.Duration) {
	m.Gets.Duration.Update(duration)
}

func (m *InfluxMetrics) RecordPutBackendXml(source string) {
	m.PutsBackend.XmlRequest.Mark(1)
}

func (m *InfluxMetrics) RecordPutBackendJson(source string) {
	m.PutsBackend.JsonRequest.Mark(1)
}

func (m *InfluxMetrics) RecordPutBackendInvalid(source string) {
	m.PutsBackend.InvalidRequest.Mark(1)
}

func (m *InfluxMetrics) RecordPutBackendDuration(duration time.Duration) {
	m.PutsBackend.Duration.Update(duration)
}

func (m *InfluxMetrics) RecordPutBackendTTLSeconds(duration time.Duration) {
	m.PutsBackend.RequestTTL.Update(duration)
}

func (m *InfluxMetrics) RecordPutBackendError(source string) {
	m.PutsBackend.Errors.Mark(1)
}

func (m *InfluxMetrics) RecordGetBackendTotal(source string) {
	m.GetsBackend.Request.Mark(1)
}

func (m *InfluxMetrics) RecordPutBackendSize(sizeInBytes float64) {
	m.PutsBackend.RequestLength.Update(int64(sizeInBytes))
}

func (m *InfluxMetrics) RecordGetBackendDuration(duration time.Duration) {
	m.GetsBackend.Duration.Update(duration)
}

func (m *InfluxMetrics) RecordGetBackendError(source string) {
	m.GetsBackend.Errors.Mark(1)
}

func (m *InfluxMetrics) RecordKeyNotFoundError(source string) {
	m.GetsErr.KeyNotFoundErrors.Mark(1)
}

func (m *InfluxMetrics) RecordMissingKeyError(source string) {
	m.GetsErr.MissingKeyErrors.Mark(1)
}

func (m *InfluxMetrics) RecordConnectionOpen() {
	m.Connections.ActiveConnections.Inc(1)
}

func (m *InfluxMetrics) RecordConnectionClosed() {
	m.Connections.ActiveConnections.Dec(1)
}

func (m *InfluxMetrics) RecordCloseConnectionErrors() {
	m.Connections.ConnectionCloseErrors.Mark(1)
}

func (m *InfluxMetrics) RecordAcceptConnectionErrors() {
	m.Connections.ConnectionAcceptErrors.Mark(1)
}
