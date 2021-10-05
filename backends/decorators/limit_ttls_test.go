package decorators_test

import (
	"context"
	"testing"

	"github.com/prebid/prebid-cache/backends/decorators"
)

func TestExcessiveTTL(t *testing.T) {
	delegate := &ttlCapturer{}
	wrapped := decorators.LimitTTLs(delegate, 100)
	wrapped.Put(context.Background(), "foo", "bar", 200, "defaultSet")
	if delegate.lastTTL != 100 {
		t.Errorf("lastTTL should be %d. Got %d", 100, delegate.lastTTL)
	}
}

func TestSafeTTL(t *testing.T) {
	delegate := &ttlCapturer{}
	wrapped := decorators.LimitTTLs(delegate, 100)
	wrapped.Put(context.Background(), "foo", "bar", 50, "defaultSet")
	if delegate.lastTTL != 50 {
		t.Errorf("lastTTL should be %d. Got %d", 50, delegate.lastTTL)
	}
}

type ttlCapturer struct {
	lastTTL int
}

func (c *ttlCapturer) Put(ctx context.Context, key string, value string, ttlSeconds int, source string) error {
	c.lastTTL = ttlSeconds
	return nil
}

func (c *ttlCapturer) Get(ctx context.Context, key string, source string) (string, error) {
	return "", nil
}

func (c *ttlCapturer) FetchSourceSet(source string) string { return "" }
