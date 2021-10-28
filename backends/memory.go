package backends

import (
	"context"
	"fmt"
	"sync"
)

type MemoryBackend struct {
	db map[string]string
	mu sync.Mutex
}

func (b *MemoryBackend) Get(ctx context.Context, key string, source string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	v, ok := b.db[key]
	if !ok {
		return "", fmt.Errorf("Not found")
	}

	return v, nil
}

func (b *MemoryBackend) Put(ctx context.Context, key string, value string, ttlSeconds int, putOptions PutOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.db[key] = value
	return nil
}

func (s *MemoryBackend) FetchSourceSet(source string) string { return "" }

func NewMemoryBackend() *MemoryBackend {
	return &MemoryBackend{
		db: make(map[string]string),
	}
}
