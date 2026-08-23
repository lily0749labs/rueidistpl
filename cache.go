package rueidistpl

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	cachelib "github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	"github.com/redis/rueidis"
)

// ErrInvalidExpiration 表示要求过期的操作收到零值或负数 TTL。
var ErrInvalidExpiration = errors.New("rueidistpl: expiration must be greater than zero")

// Cache 是基于 gocache Rueidis Store 的 JSON 对象缓存。
type Cache[T any] struct {
	cache  *cachelib.Cache[string]
	prefix string
}

// NewCache 创建按业务 ID 分项存储的 TTL 对象缓存。
// 最终 key 形式为“全局前缀:prefix:id”。
func NewCache[T any](redis *Redis, prefix string) *Cache[T] {
	return &Cache[T]{cache: redis.cache, prefix: redis.Key(prefix)}
}

// Prefix 返回应用全局前缀后的真实缓存 key 前缀。
func (cache *Cache[T]) Prefix() string {
	return cache.prefix
}

// Set 写入对象。省略 ttl 时使用 Config.CacheTTL。
func (cache *Cache[T]) Set(ctx context.Context, key string, value T, ttl ...time.Duration) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(ttl) == 0 {
		return cache.cache.Set(ctx, cache.key(key), string(payload))
	}
	if ttl[0] <= 0 {
		return ErrInvalidExpiration
	}
	expiration := ttl[0]
	if expiration < time.Second {
		expiration = time.Second
	}
	return cache.cache.Set(ctx, cache.key(key), string(payload), store.WithExpiration(expiration))
}

// Get 读取并反序列化对象；缓存未命中可通过 IsNotFound 判断。
// 发生任何错误时返回 T 的零值。
func (cache *Cache[T]) Get(ctx context.Context, key string) (T, error) {
	raw, err := cache.cache.Get(ctx, cache.key(key))
	if err != nil {
		var zero T
		return zero, err
	}
	return decodeJSON[T](raw)
}

// GetWithTTL 同时返回对象及其剩余过期时间；发生任何错误时对象和 TTL 都返回零值。
func (cache *Cache[T]) GetWithTTL(ctx context.Context, key string) (T, time.Duration, error) {
	raw, ttl, err := cache.cache.GetWithTTL(ctx, cache.key(key))
	if err != nil {
		var zero T
		return zero, 0, err
	}
	value, err := decodeJSON[T](raw)
	if err != nil {
		var zero T
		return zero, 0, err
	}
	return value, ttl, nil
}

// Delete 删除指定业务 ID 的缓存对象。
func (cache *Cache[T]) Delete(ctx context.Context, key string) error {
	return cache.cache.Delete(ctx, cache.key(key))
}

func (cache *Cache[T]) key(key string) string {
	return joinKey(cache.prefix, key)
}

// IsNotFound 统一判断 gocache 缓存未命中和 Rueidis Redis Nil。
// 因此可用于 Cache、Value、Hash、List、Ranking 等单值读取 API。
func IsNotFound(err error) bool {
	return err != nil && (errors.Is(err, store.NotFound{}) || errors.Is(err, rueidis.Nil))
}
