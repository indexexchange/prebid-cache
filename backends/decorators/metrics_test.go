package decorators

import (
	"context"
	"errors"
	"testing"

	"github.com/prebid/prebid-cache/backends"
	"github.com/prebid/prebid-cache/metrics/metricstest"
	"github.com/prebid/prebid-cache/utils"
	"github.com/stretchr/testify/assert"
)

type failedBackend struct {
	returnError error
}

func (b *failedBackend) Get(ctx context.Context, key string, source string) (string, error) {
	return "", b.returnError
}

func (b *failedBackend) Put(ctx context.Context, key string, value string, ttlSeconds int, source string) error {
	return b.returnError
}

func (s *failedBackend) FetchSourceSet(source string) string { return "" }

func TestGetSuccessMetrics(t *testing.T) {

	m := metricstest.CreateMockMetrics()
	rawBackend := backends.NewMemoryBackend()
	rawBackend.Put(context.Background(), "foo", "xml<vast></vast>", 0, "defaultSet")
	backend := LogMetrics(rawBackend, m)
	backend.Get(context.Background(), "foo", "defaultSet")

	assert.Equalf(t, int64(1), metricstest.MockCounters["gets.backends.request.total"], "Successful backend request been accounted for in the total get backend request count")
	assert.Greater(t, metricstest.MockHistograms["gets.backends.duration"], 0.00, "Successful put request duration should be greater than zero")
}

func TestGetErrorMetrics(t *testing.T) {

	type testCase struct {
		desc         string
		inMetricName string
		outError     error
	}
	testGroups := []struct {
		groupName string
		tests     []testCase
	}{
		{
			"Any other error",
			[]testCase{
				{
					"Failed get backend request should be accounted under the error label",
					"gets.backends.request.error",
					errors.New("Other backend error"),
				},
			},
		},
		{
			"Special errors",
			[]testCase{
				{
					"Failed get backend request should be accounted as a key not found error",
					"gets.backend_error.key_not_found",
					utils.KeyNotFoundError{},
				},
				{
					"Failed get backend request should be accounted as a missing key (uuid) error",
					"gets.backend_error.missing_key",
					utils.MissingKeyError{},
				},
			},
		},
	}

	// Create metrics
	m := metricstest.CreateMockMetrics()

	errsTotal := 0
	for _, group := range testGroups {
		for _, test := range group.tests {
			// Create backend, assign metrics and defective test backend
			backend := LogMetrics(&failedBackend{test.outError}, m)

			// Run test
			backend.Get(context.Background(), "foo", "defaultSet")
			errsTotal++

			// Assert
			if group.groupName == "Special errors" {
				assert.Equal(t, int64(1), metricstest.MockCounters[test.inMetricName], test.desc)
			}
			assert.Equal(t, int64(errsTotal), metricstest.MockCounters["gets.backends.request.error"], test.desc)
			assert.Equal(t, int64(errsTotal), metricstest.MockCounters["gets.backends.request.total"], test.desc)
		}
	}
}

func TestPutSuccessMetrics(t *testing.T) {

	m := metricstest.CreateMockMetrics()
	backend := LogMetrics(backends.NewMemoryBackend(), m)
	backend.Put(context.Background(), "foo", "xml<vast></vast>", 0, "defaultSet")

	assert.Greater(t, metricstest.MockHistograms["puts.backends.request_duration"], 0.00, "Successful put request duration should be greater than zero")
	assert.Equal(t, int64(1), metricstest.MockCounters["puts.backends.xml"], "An xml request should have been logged.")
	assert.Equal(t, int64(0), metricstest.MockCounters["puts.backends.defines_ttl"], "An event for TTL defined shouldn't be logged if the TTL was 0")
}

func TestPutErrorMetrics(t *testing.T) {

	m := metricstest.CreateMockMetrics()
	backend := LogMetrics(&failedBackend{errors.New("Failure")}, m)
	backend.Put(context.Background(), "foo", "xml<vast></vast>", 0, "defaultSet")

	assert.Equal(t, int64(1), metricstest.MockCounters["puts.backends.xml"], "An xml request should have been logged.")
	assert.Equal(t, int64(1), metricstest.MockCounters["puts.backends.request.error"], "Failed get backend request should have been accounted under the error label")
}

func TestJsonPayloadMetrics(t *testing.T) {

	m := metricstest.CreateMockMetrics()
	backend := LogMetrics(backends.NewMemoryBackend(), m)
	backend.Put(context.Background(), "foo", "json{\"key\":\"value\"", 0, "defaultSet")
	backend.Get(context.Background(), "foo", "defaultSet")

	assert.Equal(t, int64(1), metricstest.MockCounters["puts.backends.json"], "A json request should have been logged.")
}

func TestPutSizeSampling(t *testing.T) {

	m := metricstest.CreateMockMetrics()
	payload := `json{"key":"value"}`
	backend := LogMetrics(backends.NewMemoryBackend(), m)
	backend.Put(context.Background(), "foo", payload, 0, "defaultSet")

	assert.Greater(t, metricstest.MockHistograms["puts.backends.request_size_bytes"], 0.00, "Successful put request size should be greater than zero")
}

func TestInvalidPayloadMetrics(t *testing.T) {

	m := metricstest.CreateMockMetrics()
	backend := LogMetrics(backends.NewMemoryBackend(), m)
	backend.Put(context.Background(), "foo", "bar", 0, "defaultSet")
	backend.Get(context.Background(), "foo", "defaultSet")

	assert.Equal(t, int64(1), metricstest.MockCounters["puts.backends.invalid_format"], "A Put request of invalid format should have been logged.")
}
