package decorators

import (
	"context"
	"testing"

	"github.com/prebid/prebid-cache/backends"
)

func TestLargePayload(t *testing.T) {
	delegate := &successfulBackend{}
	wrapped := EnforceSizeLimit(delegate, 5)
	assertBadPayloadError(t, wrapped.Put(context.Background(), "foo", "123456", 0, backends.PutOptions{Source: "defaultSet"}))
}

func TestAcceptablePayload(t *testing.T) {
	delegate := &successfulBackend{}
	wrapped := EnforceSizeLimit(delegate, 5)
	assertNilError(t, wrapped.Put(context.Background(), "foo", "12345", 0, backends.PutOptions{Source: "defaultSet"}))
}

func assertBadPayloadError(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Errorf("Expected an error, but got none.")
	}
	if _, ok := err.(*BadPayloadSize); !ok {
		t.Errorf("Expected a BadPayloadSize error. Got %#v", err)
	}
}

func assertNilError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
}

type successfulBackend struct{}

func (b *successfulBackend) Get(ctx context.Context, key string, source string) (string, error) {
	return "some-value", nil
}

func (b *successfulBackend) Put(ctx context.Context, key string, value string, ttlSeconds int, putOptions backends.PutOptions) error {
	return nil
}

func (s *successfulBackend) FetchSourceSet(source string) string { return "" }
