package rueidistpl

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/redis/rueidis"
)

// 使用专用的 Redis Unix socket 显式启用集成测试，默认 go test 不依赖外部服务。
func integrationRedis(t *testing.T) *Redis {
	t.Helper()
	socket := os.Getenv("RUEIDISTPL_REDIS_SOCKET")
	if socket == "" {
		t.Skip("set RUEIDISTPL_REDIS_SOCKET to run Redis integration tests")
	}
	redis, err := New(Config{
		KeyPrefix: fmt.Sprintf("rueidistpl:test:%s:%d", t.Name(), time.Now().UnixNano()),
		ClientOption: rueidis.ClientOption{
			InitAddress:       []string{"localhost:6379"},
			ForceSingleClient: true,
			DialCtxFn: func(ctx context.Context, _ string, dialer *net.Dialer, _ *tls.Config) (net.Conn, error) {
				return dialer.DialContext(ctx, "unix", socket)
			},
		},
	})
	if err != nil {
		t.Fatalf("connect to integration Redis: %v", err)
	}
	t.Cleanup(redis.Close)
	return redis
}

func cleanupIntegrationKeys(t *testing.T, redis *Redis, keys ...string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, key := range keys {
			if err := redis.client.Do(ctx, redis.client.B().Del().Key(key).Build()).Error(); err != nil {
				t.Errorf("clean up integration key %q: %v", key, err)
			}
		}
	})
}

func TestRedisIntegrationCacheTTL(t *testing.T) {
	redis := integrationRedis(t)
	ctx := t.Context()
	cache := NewCache[string](redis, "cache")
	cleanupIntegrationKeys(t, redis, redis.Key("cache:item"))
	if err := cache.Set(ctx, "item", "long", 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	// 先填充本地缓存，再检查 GetWithTTL 是否返回完整的服务端 TTL。
	if _, err := cache.Get(ctx, "item"); err != nil {
		t.Fatal(err)
	}
	value, ttl, err := cache.GetWithTTL(ctx, "item")
	if err != nil || value != "long" || ttl < 9*time.Minute || ttl > 10*time.Minute {
		t.Fatalf("GetWithTTL() = (%q, %s, %v), want long and approximately 10 minutes", value, ttl, err)
	}
	if err := redis.SetString(ctx, "cache:item", `"short"`, 1500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	value, ttl, err = cache.GetWithTTL(ctx, "item")
	if err != nil || value != "short" || ttl <= 0 || ttl > 1500*time.Millisecond {
		t.Fatalf("GetWithTTL() after update = (%q, %s, %v)", value, ttl, err)
	}
	if err := redis.SetString(ctx, "cache:item", `"persistent"`); err != nil {
		t.Fatal(err)
	}
	value, ttl, err = cache.GetWithTTL(ctx, "item")
	if err != nil || value != "persistent" || ttl != -time.Millisecond {
		t.Fatalf("GetWithTTL() persistent = (%q, %s, %v)", value, ttl, err)
	}
	if err := cache.Delete(ctx, "item"); err != nil {
		t.Fatal(err)
	}
	value, ttl, err = cache.GetWithTTL(ctx, "item")
	if value != "" || ttl != 0 || !IsNotFound(err) {
		t.Fatalf("GetWithTTL() missing = (%q, %s, %v)", value, ttl, err)
	}
}

func TestRedisIntegrationArrayHoles(t *testing.T) {
	redis := integrationRedis(t)
	ctx := t.Context()
	array := NewArray[string](redis, "array")
	cleanupIntegrationKeys(t, redis, array.Key())
	if _, err := array.Insert(ctx, "a", "b", "c"); err != nil {
		t.Fatal(err)
	}
	if _, err := array.Delete(ctx, 1); err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		values, err := array.LastItems(ctx, 2, reverse)
		if err != nil || !reflect.DeepEqual(values, []string{"c"}) {
			t.Fatalf("LastItems(reverse=%v) = (%v, %v), want c", reverse, values, err)
		}
	}
	items, err := array.GetMany(ctx, 0, 1, 2)
	if err != nil || len(items) != 3 || !items[0].Exists || items[1].Exists || !items[2].Exists {
		t.Fatalf("GetMany() = (%+v, %v), want preserved empty slot", items, err)
	}
}

func TestRedisIntegrationCompositeKeysAndRankingPage(t *testing.T) {
	redis := integrationRedis(t)
	ctx := t.Context()
	ranking := NewRanking[string](redis, "rank{}")
	valuesKey, scoresKey := ranking.Keys()
	cleanupIntegrationKeys(t, redis, valuesKey, scoresKey)
	for index, name := range []string{"a", "b", "c"} {
		if err := ranking.Upsert(ctx, name, float64(index), name); err != nil {
			t.Fatal(err)
		}
	}
	items, err := ranking.Page(ctx, 1, 2)
	if err != nil || !reflect.DeepEqual(items, []Ranked[string]{{ID: "b", Score: 1, Value: "b"}, {ID: "a", Score: 0, Value: "a"}}) {
		t.Fatalf("Page() = (%+v, %v), want b then a", items, err)
	}
	if _, err := ranking.Page(ctx, 0, MaxRankingPageSize+1); !errors.Is(err, ErrInvalidRankingPage) {
		t.Fatalf("oversized Page() error = %v", err)
	}
	queue := NewDelayQueue[string](redis, "queue{}")
	queueValues, queueScores := queue.Keys()
	cleanupIntegrationKeys(t, redis, queueValues, queueScores)
	now := time.Now()
	if err := queue.Schedule(ctx, "task", now, "payload"); err != nil {
		t.Fatal(err)
	}
	tasks, err := queue.Poll(ctx, now, 1)
	if err != nil || len(tasks) != 1 || tasks[0].Value != "payload" {
		t.Fatalf("Poll() = (%+v, %v), want scheduled payload", tasks, err)
	}
}

func TestRedisIntegrationBatches(t *testing.T) {
	redis := integrationRedis(t)
	ctx := t.Context()
	keys := make([]string, keyBatchSize+3)
	actualKeys := make([]string, len(keys))
	commands := make([]rueidis.Completed, len(keys))
	for index := range keys {
		keys[index] = fmt.Sprintf("key-%d", index)
		actualKeys[index] = redis.Key(keys[index])
		commands[index] = redis.client.B().Set().Key(actualKeys[index]).Value("1").Build()
	}
	cleanupIntegrationKeys(t, redis, actualKeys...)
	for _, result := range redis.client.DoMulti(ctx, commands...) {
		if err := result.Error(); err != nil {
			t.Fatal(err)
		}
	}
	existing := redis.ExistsEach(ctx, append(keys, keys[0])...)
	if existing.Err != nil || existing.Existing != int64(len(keys)+1) {
		t.Fatalf("ExistsEach() = (%d, %v), want %d", existing.Existing, existing.Err, len(keys)+1)
	}
	deleted := redis.DeleteEach(ctx, append(keys, keys[0])...)
	if deleted.Err != nil || deleted.Deleted != int64(len(keys)) || deleted.Results[len(keys)].Deleted {
		t.Fatalf("DeleteEach() = (%d, %v), want %d unique deletions", deleted.Deleted, deleted.Err, len(keys))
	}
}
