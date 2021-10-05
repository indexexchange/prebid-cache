package metrics

import (
	"time"

	"github.com/prebid/prebid-cache/config"
	influx "github.com/prebid/prebid-cache/metrics/influx"
	prometheus "github.com/prebid/prebid-cache/metrics/prometheus"
)

// Metrics provides access to metric engines.
type Metrics struct {
	MetricEngines []CacheMetrics
}

// Methods so the metrics object executes the methods of the `CacheMetrics` interface
func (m Metrics) RecordPutError() {
	for _, me := range m.MetricEngines {
		me.RecordPutError()
	}
}

func (m Metrics) RecordPutBadRequest() {
	for _, me := range m.MetricEngines {
		me.RecordPutBadRequest()
	}
}

func (m Metrics) RecordPutTotal() {
	for _, me := range m.MetricEngines {
		me.RecordPutTotal()
	}
}

func (m Metrics) RecordPutDuration(duration time.Duration) {
	for _, me := range m.MetricEngines {
		me.RecordPutDuration(duration)
	}
}

func (m Metrics) RecordGetError() {
	for _, me := range m.MetricEngines {
		me.RecordGetError()
	}
}

func (m Metrics) RecordGetBadRequest() {
	for _, me := range m.MetricEngines {
		me.RecordGetBadRequest()
	}
}

func (m Metrics) RecordGetTotal() {
	for _, me := range m.MetricEngines {
		me.RecordGetTotal()
	}
}

func (m Metrics) RecordGetDuration(duration time.Duration) {
	for _, me := range m.MetricEngines {
		me.RecordGetDuration(duration)
	}
}

func (m Metrics) RecordPutBackendXml(source string) {
	for _, me := range m.MetricEngines {
		me.RecordPutBackendXml(source)
	}
}

func (m Metrics) RecordPutBackendJson(source string) {
	for _, me := range m.MetricEngines {
		me.RecordPutBackendJson(source)
	}
}

func (m Metrics) RecordPutBackendInvalid(source string) {
	for _, me := range m.MetricEngines {
		me.RecordPutBackendInvalid(source)
	}
}

func (m Metrics) RecordPutBackendDuration(duration time.Duration) {
	for _, me := range m.MetricEngines {
		me.RecordPutBackendDuration(duration)
	}
}

func (m Metrics) RecordPutBackendTTLSeconds(duration time.Duration) {
	for _, me := range m.MetricEngines {
		me.RecordPutBackendTTLSeconds(duration)
	}
}

func (m Metrics) RecordPutBackendError(source string) {
	for _, me := range m.MetricEngines {
		me.RecordPutBackendError(source)
	}
}

func (m Metrics) RecordPutBackendSize(sizeInBytes float64) {
	for _, me := range m.MetricEngines {
		me.RecordPutBackendSize(sizeInBytes)
	}
}

func (m Metrics) RecordGetBackendDuration(duration time.Duration) {
	for _, me := range m.MetricEngines {
		me.RecordGetBackendDuration(duration)
	}
}

func (m Metrics) RecordGetBackendTotal(source string) {
	for _, me := range m.MetricEngines {
		me.RecordGetBackendTotal(source)
	}
}

func (m Metrics) RecordGetBackendError(source string) {
	for _, me := range m.MetricEngines {
		me.RecordGetBackendError(source)
	}
}

func (m Metrics) RecordKeyNotFoundError(source string) {
	for _, me := range m.MetricEngines {
		me.RecordKeyNotFoundError(source)
	}
}

func (m Metrics) RecordMissingKeyError(source string) {
	for _, me := range m.MetricEngines {
		me.RecordMissingKeyError(source)
	}
}

func (m Metrics) RecordConnectionOpen() {
	for _, me := range m.MetricEngines {
		me.RecordConnectionOpen()
	}
}

func (m Metrics) RecordConnectionClosed() {
	for _, me := range m.MetricEngines {
		me.RecordConnectionClosed()
	}
}

func (m Metrics) RecordCloseConnectionErrors() {
	for _, me := range m.MetricEngines {
		me.RecordCloseConnectionErrors()
	}
}

func (m Metrics) RecordAcceptConnectionErrors() {
	for _, me := range m.MetricEngines {
		me.RecordAcceptConnectionErrors()
	}
}

func (m Metrics) Export(cfg config.Configuration) {
	for _, me := range m.MetricEngines {
		me.Export(cfg.Metrics)
	}
}

func (m Metrics) GetEngineRegistry(name string) interface{} {
	for _, me := range m.MetricEngines {
		if name == me.GetMetricsEngineName() {
			return me.GetEngineRegistry()
		}
	}
	return nil
}

type CacheMetrics interface {
	// Auxiliary functions
	Export(cfg config.Metrics)
	GetMetricsEngineName() string
	GetEngineRegistry() interface{}

	// Record, update and log metrics functions
	RecordPutError()
	RecordPutBadRequest()
	RecordPutTotal()
	RecordPutDuration(duration time.Duration)
	RecordGetError()
	RecordGetBadRequest()
	RecordGetTotal()
	RecordGetDuration(duration time.Duration)
	RecordPutBackendXml(source string)
	RecordPutBackendJson(source string)
	RecordPutBackendInvalid(source string)
	RecordPutBackendDuration(duration time.Duration)
	RecordPutBackendTTLSeconds(duration time.Duration)
	RecordPutBackendError(source string)
	RecordPutBackendSize(sizeInBytes float64)
	RecordGetBackendTotal(source string)
	RecordGetBackendDuration(duration time.Duration)
	RecordGetBackendError(source string)
	RecordKeyNotFoundError(source string)
	RecordMissingKeyError(source string)
	RecordConnectionOpen()
	RecordConnectionClosed()
	RecordCloseConnectionErrors()
	RecordAcceptConnectionErrors()
}

func CreateMetrics(cfg config.Configuration) *Metrics {
	engineList := make([]CacheMetrics, 0, 2)

	if cfg.Metrics.Influx.Enabled {
		engineList = append(engineList, influx.CreateInfluxMetrics())
	}
	if cfg.Metrics.Prometheus.Enabled {
		engineList = append(engineList, prometheus.CreatePrometheusMetrics(cfg.Metrics.Prometheus))
	}
	return &Metrics{MetricEngines: engineList}
}
