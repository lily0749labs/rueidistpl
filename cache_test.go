package rueidistpl

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	cachelib "github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
)

// unitCacheStore 是仅用于单元测试的内存 Store，记录最后一次 TTL 配置。
// 它隔离了 gocache 的上层行为，测试过程中不会创建 Redis 连接。
type unitCacheStore struct {
	values      map[any]any
	ttls        map[any]time.Duration
	lastOptions *store.Options
}

func newUnitCacheStore() *unitCacheStore {
	return &unitCacheStore{values: make(map[any]any), ttls: make(map[any]time.Duration)}
}

func (memory *unitCacheStore) Get(_ context.Context, key any) (any, error) {
	value, exists := memory.values[key]
	if !exists {
		return nil, store.NotFound{}
	}
	return value, nil
}

func (memory *unitCacheStore) GetWithTTL(ctx context.Context, key any) (any, time.Duration, error) {
	value, err := memory.Get(ctx, key)
	return value, memory.ttls[key], err
}

func (memory *unitCacheStore) Set(_ context.Context, key, value any, options ...store.Option) error {
	memory.lastOptions = store.ApplyOptions(options...)
	memory.values[key] = value
	memory.ttls[key] = memory.lastOptions.Expiration
	return nil
}

func (memory *unitCacheStore) Delete(_ context.Context, key any) error {
	delete(memory.values, key)
	delete(memory.ttls, key)
	return nil
}

func (memory *unitCacheStore) Invalidate(context.Context, ...store.InvalidateOption) error {
	return nil
}
func (memory *unitCacheStore) Clear(context.Context) error { return nil }
func (memory *unitCacheStore) GetType() string             { return "unit" }

// TestCacheLifecycle 覆盖 Cache 的完整正常路径：
//  1. JSON 对象按“全局前缀:业务前缀:ID”写入；
//  2. 小于一秒的 TTL 提升到 Store 支持的一秒；
//  3. Get 和 Delete 保持类型及过期信息正确。
func TestCacheLifecycle(t *testing.T) {
	// Arrange：用内存 Store 组装真实 gocache Cache，隔离 Redis 连接但保留缓存编解码链路。
	ctx := context.Background()
	memory := newUnitCacheStore()
	redis, client := newMockRedis(t)
	redis.cache = cachelib.New[string](memory)
	cache := NewCache[unitUser](redis, "users")
	alice := unitUser{ID: 1, Name: "Alice"}
	bob := unitUser{ID: 2, Name: "Bob"}
	realPrefix := cache.Prefix()
	if realPrefix != "app:users" {
		t.Fatalf("Prefix value=%q", realPrefix)
	}

	// Act/Assert：省略 TTL 时写入 JSON，但不额外覆盖 Store 的默认过期配置。
	setErr := cache.Set(ctx, "1", alice)
	if setErr != nil {
		t.Fatal(setErr)
	}
	if memory.values["app:users:1"] != `{"id":1,"name":"Alice"}` || memory.lastOptions.Expiration != 0 {
		t.Fatalf("unexpected cache state value=%v options=%+v", memory.values["app:users:1"], memory.lastOptions)
	}
	// 显式传入纳秒级 TTL，验证封装会提升到 Rueidis Store 支持的一秒精度。
	setErr2 := cache.Set(ctx, "2", bob, time.Nanosecond)
	if setErr2 != nil {
		t.Fatal(setErr2)
	}
	if memory.lastOptions.Expiration != time.Second {
		t.Fatalf("expiration=%s want %s", memory.lastOptions.Expiration, time.Second)
	}
	// Get 使用 Store；GetWithTTL 从服务端原子读取对象与 TTL。
	getValue, getErr := cache.Get(ctx, "1")
	if getErr != nil || getValue != alice {
		t.Fatalf("Get value=%+v err=%v", getValue, getErr)
	}
	client.EXPECT().Do(ctx, retryable(matchLua([]string{"app:users:2"}, nil))).Return(
		rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString(`{"id":2,"name":"Bob"}`), rueidismock.RedisInt64(1000),
		)),
	)
	cachedValue, cachedTTL, cacheTTLErr := cache.GetWithTTL(ctx, "2")
	if cacheTTLErr != nil || cachedValue != bob || cachedTTL != time.Second {
		t.Fatalf("GetWithTTL value=%+v ttl=%s err=%v", cachedValue, cachedTTL, cacheTTLErr)
	}
	// 删除后再次读取必须得到 gocache 未命中，而不是对象零值。
	deleteErr := cache.Delete(ctx, "1")
	if deleteErr != nil {
		t.Fatal(deleteErr)
	}
	_, getErr2 := cache.Get(ctx, "1")
	if !IsNotFound(getErr2) {
		t.Fatalf("got %v, expected cache miss", getErr2)
	}
}

// TestCacheValidationAndDecodeErrors 覆盖调用方错误和脏数据：
// 非正 TTL、无法 JSON 序列化的对象、Store 中非法 JSON 都必须返回错误，
// IsNotFound 应统一匹配 gocache 与 Rueidis 的未命中错误，但不能吞掉其他异常。
func TestCacheValidationAndDecodeErrors(t *testing.T) {
	// Arrange：仍使用内存 Store，便于直接注入 Redis 中可能出现的脏字符串。
	ctx := context.Background()
	memory := newUnitCacheStore()
	redis, client := newMockRedis(t)
	redis.cache = cachelib.New[string](memory)
	redis.keyPrefix = ""
	cache := NewCache[unitUser](redis, "users")
	// 非正 TTL 应在写 Store 前失败。
	setErr := cache.Set(ctx, "1", unitUser{}, 0)
	if !errors.Is(setErr, ErrInvalidExpiration) {
		t.Fatalf("got %v want %v", setErr, ErrInvalidExpiration)
	}
	// 注入会先写入 ID、再因 Name 类型错误而失败的 JSON，确认不能泄漏部分解码对象。
	memory.values["users:bad"] = `{"id":99,"name":[]}`
	invalidValue, getErr := cache.Get(ctx, "bad")
	if getErr == nil || invalidValue != (unitUser{}) {
		t.Fatalf("Get value=%+v err=%v; want zero value and decode error", invalidValue, getErr)
	}
	// GetWithTTL 的任一错误都应让对象和 TTL 同时归零，避免调用方误用残留元数据。
	memory.values["users:bad-ttl"] = `{"id":99,"name":[]}`
	memory.ttls["users:bad-ttl"] = time.Minute
	client.EXPECT().Do(ctx, retryable(matchLua([]string{"users:bad-ttl"}, nil))).Return(
		rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString(`{"id":99,"name":[]}`), rueidismock.RedisInt64(60000),
		)),
	)
	invalidTTLValue, invalidTTL, cacheTTLErr := cache.GetWithTTL(ctx, "bad-ttl")
	if cacheTTLErr == nil || invalidTTLValue != (unitUser{}) || invalidTTL != 0 {
		t.Fatalf("GetWithTTL value=%+v ttl=%s err=%v; want zero results and decode error", invalidTTLValue, invalidTTL, cacheTTLErr)
	}

	// chan 无法被 encoding/json 编码，用它确认写入失败不会落入 Store。
	bad := NewCache[chan int](redis, "bad")
	setErr2 := bad.Set(ctx, "1", make(chan int))
	if setErr2 == nil {
		t.Fatal("Set should reject values unsupported by JSON")
	}
	// 两类未命中都应命中统一判断；nil 和普通错误必须继续返回 false。
	gocacheMissing := store.NotFound{}
	redisMissing := rueidis.Nil
	wrappedRedisMissing := fmt.Errorf("read value: %w", rueidis.Nil)
	if !IsNotFound(gocacheMissing) || !IsNotFound(redisMissing) || !IsNotFound(wrappedRedisMissing) {
		t.Fatalf("IsNotFound gocache=%t rueidis=%t wrapped=%t", IsNotFound(gocacheMissing), IsNotFound(redisMissing), IsNotFound(wrappedRedisMissing))
	}
	if IsNotFound(nil) || IsNotFound(errors.New("other")) {
		t.Fatal("IsNotFound should not match nil or ordinary errors")
	}
}
