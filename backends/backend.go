package backends

import (
	"context"
)

// Backend interface for storing data
type Backend interface {
	Put(ctx context.Context, key string, value string, ttlSeconds int, putOptions PutOptions) error
	Get(ctx context.Context, key string, source string) (string, error)
	FetchSourceSet(source string) string
}
