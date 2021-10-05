package metrics

import (
	"testing"
	"time"

	"github.com/prebid/prebid-cache/config"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

const TenSeconds time.Duration = time.Second * 10

func createPrometheusMetricsForTesting() *PrometheusMetrics {
	return CreatePrometheusMetrics(config.PrometheusMetrics{
		Port:      8080,
		Namespace: "prebid",
		Subsystem: "cache",
	})
}

func assertCounterVecValue(t *testing.T, description string, counterVec *prometheus.CounterVec, expected float64, labels prometheus.Labels) {
	counter := counterVec.With(labels)
	assertCounterValue(t, description, counter, expected)
}

func assertCounterValue(t *testing.T, description string, counter prometheus.Counter, expected float64) {
	m := dto.Metric{}
	counter.Write(&m)
	actual := *m.GetCounter().Value

	assert.Equal(t, expected, actual, description)
}

func assertGaugeValue(t *testing.T, description string, gauge prometheus.Gauge, expected float64) {
	m := dto.Metric{}
	gauge.Write(&m)
	actual := *m.GetGauge().Value

	assert.Equal(t, expected, actual, description)
}

func assertHistogram(t *testing.T, name string, histogram prometheus.Histogram, expectedCount uint64, expectedSum float64) {
	m := dto.Metric{}
	histogram.Write(&m)
	actual := *m.GetHistogram()

	assert.Equal(t, expectedCount, actual.GetSampleCount(), name+":count")
	assert.Equal(t, expectedSum, actual.GetSampleSum(), name+":sum")
}

func TestPrometheusGetMetricsEngineName(t *testing.T) {
	m := createPrometheusMetricsForTesting()
	engineName := m.GetMetricsEngineName()

	assert.Equal(t, "Prometheus", engineName, "Prometheus engine name should be 'Prometheus', actual: %s \n", engineName)
}

func TestPrometheusGetEngineRegistry(t *testing.T) {
	m := createPrometheusMetricsForTesting()

	registry := m.GetEngineRegistry()

	_, ok := registry.(*prometheus.Registry)

	assert.True(t, ok, "Prometheus engine registry should be of type *prometheus.Registry")
}

func TestPrometheusRequestStatusMetric(t *testing.T) {
	m := createPrometheusMetricsForTesting()

	type testCaseObject struct {
		description           string
		expDuration           float64
		expRequestTotals      float64
		expRequestErrors      float64
		expBadRequests        float64
		testCase              func(pm *PrometheusMetrics)
		expLabels             map[string]string
		expRequestTotalLabels prometheus.Labels
		expRequestErrorLabels prometheus.Labels
		expBadRequestLabels   prometheus.Labels
	}

	testGroups := map[*PrometheusRequestStatusMetric][]testCaseObject{
		m.Puts: {
			{
				description: "Log put request duration",
				testCase: func(pm *PrometheusMetrics) {
					pm.RecordPutDuration(TenSeconds)
				},
				expDuration:      10,
				expRequestTotals: 0, expRequestErrors: 0, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal},
			},
			{
				description:      "Count put request total",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordPutTotal() },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 0, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal},
			},
			{
				description:      "Count put request error",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordPutError() },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 1, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal},
			},
			{
				description:      "Count put request bad request",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordPutBadRequest() },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 1, expBadRequests: 1,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal},
			},
		},
		m.Gets: {
			{
				description: "Log get request duration",
				testCase: func(pm *PrometheusMetrics) {
					pm.RecordGetDuration(TenSeconds)
				},
				expDuration:      10,
				expRequestTotals: 0, expRequestErrors: 0, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal},
			},
			{
				description:      "Count get request total",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordGetTotal() },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 0, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal},
			},
			{
				description:      "Count get request error",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordGetError() },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 1, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal},
			},
			{
				description:      "Count get request bad request",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordGetBadRequest() },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 1, expBadRequests: 1,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal},
			},
		},
		m.GetsBackend: {
			{
				description: "Log get backend request duration",
				testCase: func(pm *PrometheusMetrics) {
					pm.RecordGetBackendDuration(TenSeconds)
				},
				expDuration:      10,
				expRequestTotals: 0, expRequestErrors: 0, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal, SetKey: EmptyVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal, SetKey: EmptyVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal, SetKey: EmptyVal},
			},
			{
				description:      "Count get backend request total",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordGetBackendTotal("") },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 0, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal, SetKey: EmptyVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal, SetKey: EmptyVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal, SetKey: EmptyVal},
			},
			{
				description:      "Count get backend request error",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordGetBackendError("") },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 1, expBadRequests: 0,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal, SetKey: EmptyVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal, SetKey: EmptyVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal, SetKey: EmptyVal},
			},
			{
				description:      "Count get backend request bad request",
				testCase:         func(pm *PrometheusMetrics) { pm.RecordGetBackendBadRequest("") },
				expDuration:      10,
				expRequestTotals: 1, expRequestErrors: 1, expBadRequests: 1,
				expRequestTotalLabels: prometheus.Labels{StatusKey: TotalsVal, SetKey: EmptyVal},
				expRequestErrorLabels: prometheus.Labels{StatusKey: ErrorVal, SetKey: EmptyVal},
				expBadRequestLabels:   prometheus.Labels{StatusKey: BadRequestVal, SetKey: EmptyVal},
			},
		},
	}

	for prometheusMetric, testCaseArray := range testGroups {
		for _, test := range testCaseArray {
			test.testCase(m)

			assertHistogram(t, test.description, prometheusMetric.Duration, 1, test.expDuration)
			assertCounterVecValue(t, test.description, prometheusMetric.RequestStatus, test.expRequestTotals, test.expRequestTotalLabels)
			assertCounterVecValue(t, test.description, prometheusMetric.RequestStatus, test.expRequestErrors, test.expRequestErrorLabels)
			assertCounterVecValue(t, test.description, prometheusMetric.RequestStatus, test.expBadRequests, test.expBadRequestLabels)
		}
	}
}

func TestGetsBackendErrorsByType(t *testing.T) {

	m := createPrometheusMetricsForTesting()

	testCaseArray := []struct {
		description          string
		expKeyNotFoundErrors float64
		expMissingKeyErrors  float64
		recordMetric         func(pm *PrometheusMetrics)
		expectedSetKey       string
	}{
		{
			description:          "Add to the get backend key not found error counter",
			expKeyNotFoundErrors: 1,
			expMissingKeyErrors:  0,
			recordMetric:         func(pm *PrometheusMetrics) { pm.RecordKeyNotFoundError("") },
			expectedSetKey:       "",
		},
		{
			description:          "Add to the get backend missing key error",
			expKeyNotFoundErrors: 1,
			expMissingKeyErrors:  1,
			recordMetric:         func(pm *PrometheusMetrics) { pm.RecordMissingKeyError("") },
			expectedSetKey:       "",
		},
		{
			description:          "Add to the get backend missing key error with set",
			expKeyNotFoundErrors: 0,
			expMissingKeyErrors:  1,
			recordMetric:         func(pm *PrometheusMetrics) { pm.RecordMissingKeyError("ixl") },
			expectedSetKey:       "ixl",
		},
	}

	for _, test := range testCaseArray {

		test.recordMetric(m)

		assertCounterVecValue(t, test.description, m.GetsBackend.ErrorsByType, test.expKeyNotFoundErrors, prometheus.Labels{TypeKey: KeyNotFoundVal, SetKey: test.expectedSetKey})
		assertCounterVecValue(t, test.description, m.GetsBackend.ErrorsByType, test.expMissingKeyErrors, prometheus.Labels{TypeKey: MissingKeyVal, SetKey: test.expectedSetKey})
	}
}

func TestPutBackendMetrics(t *testing.T) {
	m := createPrometheusMetricsForTesting()

	type testCaseObject struct {
		description string
		testCase    func(pm *PrometheusMetrics)

		//counters
		expXmlCount     float64
		expJsonCount    float64
		expInvalidCount float64
		expDefTTLCount  float64
		expErrorCount   float64

		//Duration and sixe in bytes
		expDuration      float64
		expDefTTLSeconds float64
		expSizeHistSum   float64
		expSizeHistCount uint64
		expXmlLabels     prometheus.Labels
		expJsonLabels    prometheus.Labels
		expInvalidLabels prometheus.Labels
		expErrorLabels   prometheus.Labels
	}

	testCases := []testCaseObject{
		{
			description: "Log put backend request duration",
			testCase: func(pm *PrometheusMetrics) {
				pm.RecordPutBackendDuration(TenSeconds)
			},
			expDuration:      10,
			expXmlLabels:     prometheus.Labels{FormatKey: XmlVal, SetKey: EmptyVal},
			expJsonLabels:    prometheus.Labels{FormatKey: JsonVal, SetKey: EmptyVal},
			expInvalidLabels: prometheus.Labels{FormatKey: InvFormatVal, SetKey: EmptyVal},
			expErrorLabels:   prometheus.Labels{FormatKey: ErrorVal, SetKey: EmptyVal},
		},
		{
			description:      "Count put backend xml request",
			testCase:         func(pm *PrometheusMetrics) { pm.RecordPutBackendXml("") },
			expDuration:      10,
			expXmlCount:      1,
			expXmlLabels:     prometheus.Labels{FormatKey: XmlVal, SetKey: EmptyVal},
			expJsonLabels:    prometheus.Labels{FormatKey: JsonVal, SetKey: EmptyVal},
			expInvalidLabels: prometheus.Labels{FormatKey: InvFormatVal, SetKey: EmptyVal},
			expErrorLabels:   prometheus.Labels{FormatKey: ErrorVal, SetKey: EmptyVal},
		},
		{
			description:      "Count put backend json request",
			testCase:         func(pm *PrometheusMetrics) { pm.RecordPutBackendJson("") },
			expDuration:      10,
			expXmlCount:      1,
			expJsonCount:     1,
			expXmlLabels:     prometheus.Labels{FormatKey: XmlVal, SetKey: EmptyVal},
			expJsonLabels:    prometheus.Labels{FormatKey: JsonVal, SetKey: EmptyVal},
			expInvalidLabels: prometheus.Labels{FormatKey: InvFormatVal, SetKey: EmptyVal},
			expErrorLabels:   prometheus.Labels{FormatKey: ErrorVal, SetKey: EmptyVal},
		},
		{
			description:      "Count put backend invalid request",
			testCase:         func(pm *PrometheusMetrics) { pm.RecordPutBackendInvalid("") },
			expDuration:      10,
			expXmlCount:      1,
			expJsonCount:     1,
			expInvalidCount:  1,
			expXmlLabels:     prometheus.Labels{FormatKey: XmlVal, SetKey: EmptyVal},
			expJsonLabels:    prometheus.Labels{FormatKey: JsonVal, SetKey: EmptyVal},
			expInvalidLabels: prometheus.Labels{FormatKey: InvFormatVal, SetKey: EmptyVal},
			expErrorLabels:   prometheus.Labels{FormatKey: ErrorVal, SetKey: EmptyVal},
		},
		{
			description:      "Count put backend request errors",
			testCase:         func(pm *PrometheusMetrics) { pm.RecordPutBackendError("") },
			expDuration:      10,
			expXmlCount:      1,
			expJsonCount:     1,
			expInvalidCount:  1,
			expDefTTLCount:   1,
			expErrorCount:    1,
			expXmlLabels:     prometheus.Labels{FormatKey: XmlVal, SetKey: EmptyVal},
			expJsonLabels:    prometheus.Labels{FormatKey: JsonVal, SetKey: EmptyVal},
			expInvalidLabels: prometheus.Labels{FormatKey: InvFormatVal, SetKey: EmptyVal},
			expErrorLabels:   prometheus.Labels{FormatKey: ErrorVal, SetKey: EmptyVal},
		},
		{
			description: "Log put backend request duration",
			testCase: func(pm *PrometheusMetrics) {
				pm.RecordPutBackendSize(16)
			},
			expDuration:      10,
			expXmlCount:      1,
			expJsonCount:     1,
			expInvalidCount:  1,
			expDefTTLCount:   1,
			expErrorCount:    1,
			expSizeHistSum:   16,
			expSizeHistCount: 1,
			expXmlLabels:     prometheus.Labels{FormatKey: XmlVal, SetKey: EmptyVal},
			expJsonLabels:    prometheus.Labels{FormatKey: JsonVal, SetKey: EmptyVal},
			expInvalidLabels: prometheus.Labels{FormatKey: InvFormatVal, SetKey: EmptyVal},
			expErrorLabels:   prometheus.Labels{FormatKey: ErrorVal, SetKey: EmptyVal},
		},
		{
			description: "Out of those requests that define a TTL, log the number of TTL seconds",
			testCase: func(pm *PrometheusMetrics) {
				pm.RecordPutBackendTTLSeconds(TenSeconds)
			},
			expDuration:      10,
			expXmlCount:      1,
			expJsonCount:     1,
			expInvalidCount:  1,
			expDefTTLCount:   1,
			expErrorCount:    1,
			expSizeHistSum:   16,
			expSizeHistCount: 1,
			expDefTTLSeconds: 10,
			expXmlLabels:     prometheus.Labels{FormatKey: XmlVal, SetKey: EmptyVal},
			expJsonLabels:    prometheus.Labels{FormatKey: JsonVal, SetKey: EmptyVal},
			expInvalidLabels: prometheus.Labels{FormatKey: InvFormatVal, SetKey: EmptyVal},
			expErrorLabels:   prometheus.Labels{FormatKey: ErrorVal, SetKey: EmptyVal},
		},
	}

	for _, test := range testCases {
		test.testCase(m)

		assertHistogram(t, test.description, m.PutsBackend.Duration, 1, test.expDuration)
		assertCounterVecValue(t, test.description, m.PutsBackend.PutBackendRequests, test.expXmlCount, test.expXmlLabels)
		assertCounterVecValue(t, test.description, m.PutsBackend.PutBackendRequests, test.expJsonCount, test.expJsonLabels)
		assertCounterVecValue(t, test.description, m.PutsBackend.PutBackendRequests, test.expInvalidCount, test.expInvalidLabels)
		assertCounterVecValue(t, test.description, m.PutsBackend.PutBackendRequests, test.expErrorCount, test.expErrorLabels)
		assertHistogram(t, test.description, m.PutsBackend.RequestLength, test.expSizeHistCount, test.expSizeHistSum)
	}
}

func TestConnectionMetrics(t *testing.T) {
	testCases := []struct {
		description                    string
		testCase                       func(pm *PrometheusMetrics)
		expectedOpenedConnectionCount  float64
		expectedClosedConnectionCount  float64
		expectedAcceptConnectionErrors float64
		expectedCloseConnectionErrors  float64
	}{
		{
			description: "Add a connection to the open connection count",
			testCase: func(pm *PrometheusMetrics) {
				pm.RecordConnectionOpen()
			},
			expectedOpenedConnectionCount:  1,
			expectedClosedConnectionCount:  0,
			expectedAcceptConnectionErrors: 0,
			expectedCloseConnectionErrors:  0,
		},
		{
			description: "Remove a connection from the open connection count",
			testCase: func(pm *PrometheusMetrics) {
				pm.RecordConnectionClosed()
			},
			expectedOpenedConnectionCount:  1,
			expectedClosedConnectionCount:  1,
			expectedAcceptConnectionErrors: 0,
			expectedCloseConnectionErrors:  0,
		},
		{
			description: "Count a close connection error",
			testCase: func(pm *PrometheusMetrics) {
				pm.RecordCloseConnectionErrors()
			},
			expectedOpenedConnectionCount:  1,
			expectedClosedConnectionCount:  1,
			expectedAcceptConnectionErrors: 0,
			expectedCloseConnectionErrors:  1,
		},
		{
			description: "Count an accept connection error",
			testCase: func(pm *PrometheusMetrics) {
				pm.RecordCloseConnectionErrors()
				pm.RecordAcceptConnectionErrors()
			},
			expectedOpenedConnectionCount:  1,
			expectedClosedConnectionCount:  1,
			expectedAcceptConnectionErrors: 1,
			expectedCloseConnectionErrors:  2,
		},
	}

	m := createPrometheusMetricsForTesting()

	for _, test := range testCases {
		test.testCase(m)

		assertCounterValue(t, test.description, m.Connections.ConnectionsOpened, test.expectedOpenedConnectionCount)
		assertCounterValue(t, test.description, m.Connections.ConnectionsClosed, test.expectedClosedConnectionCount)

		assertCounterVecValue(t, test.description, m.Connections.ConnectionsErrors, test.expectedAcceptConnectionErrors, prometheus.Labels{ConnErrorKey: AcceptVal})
		assertCounterVecValue(t, test.description, m.Connections.ConnectionsErrors, test.expectedCloseConnectionErrors, prometheus.Labels{ConnErrorKey: CloseVal})
	}
}

func TestMetricCountGatekeeping(t *testing.T) {
	expectedCardinalityCount := 100
	actualCardinalityCount := 0

	// Run test
	m := createPrometheusMetricsForTesting()
	metricFamilies, err := m.Registry.Gather()
	assert.NoError(t, err, "gather metics")

	// Assertions
	for _, metricFamily := range metricFamilies {
		actualCardinalityCount += len(metricFamily.GetMetric())
	}

	// This assertion provides a warning for newly added high-cardinality non-adapter specific metrics. The hardcoded limit
	// is an arbitrary soft ceiling. Thought should be given as to the value of the new metrics if you find yourself
	// needing to increase this number.
	assert.True(t, actualCardinalityCount <= expectedCardinalityCount, "General Cardinality doesn't match")
}
