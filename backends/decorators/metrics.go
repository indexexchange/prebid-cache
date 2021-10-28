package decorators

import (
	"context"
	"strings"
	"time"

	"github.com/prebid/prebid-cache/backends"
	"github.com/prebid/prebid-cache/metrics"
	"github.com/prebid/prebid-cache/utils"
)

type backendWithMetrics struct {
	delegate backends.Backend
	metrics  *metrics.Metrics
}

func (b *backendWithMetrics) Get(ctx context.Context, key string, source string) (string, error) {

	set := b.delegate.FetchSourceSet(source)

	b.metrics.RecordGetBackendTotal(set)
	start := time.Now()

	val, err := b.delegate.Get(ctx, key, source)
	if err == nil {
		b.metrics.RecordGetBackendDuration(time.Since(start))
	} else {
		if _, isKeyNotFound := err.(utils.KeyNotFoundError); isKeyNotFound {
			b.metrics.RecordKeyNotFoundError(set)
		} else if _, isMissingUuidError := err.(utils.MissingKeyError); isMissingUuidError {
			b.metrics.RecordMissingKeyError(set)
		}
		b.metrics.RecordGetBackendError(set)
	}
	return val, err
}

func (b *backendWithMetrics) Put(ctx context.Context, key string, value string, ttlSeconds int, putOptions backends.PutOptions) error {

	set := b.delegate.FetchSourceSet(putOptions.Source)

	if strings.HasPrefix(value, backends.XML_PREFIX) {
		b.metrics.RecordPutBackendXml(set)
	} else if strings.HasPrefix(value, backends.JSON_PREFIX) {
		b.metrics.RecordPutBackendJson(set)
	} else {
		b.metrics.RecordPutBackendInvalid(set)
	}
	b.metrics.RecordPutBackendTTLSeconds(time.Duration(ttlSeconds) * time.Second)

	start := time.Now()
	err := b.delegate.Put(ctx, key, value, ttlSeconds, putOptions)
	if err == nil {
		b.metrics.RecordPutBackendDuration(time.Since(start))
	} else {
		b.metrics.RecordPutBackendError(set)
	}
	b.metrics.RecordPutBackendSize(float64(len(value)))
	return err
}
func (b *backendWithMetrics) FetchSourceSet(source string) string {
	return b.delegate.FetchSourceSet(source)
}

func LogMetrics(backend backends.Backend, m *metrics.Metrics) backends.Backend {
	return &backendWithMetrics{
		delegate: backend,
		metrics:  m,
	}
}
