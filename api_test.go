package rueidistpl

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

// TestCacheKey 验证对象缓存 key 的两层拼接规则：
// 普通业务 ID 应追加到缓存前缀后，空 ID 则直接使用缓存前缀本身。
func TestCacheKey(t *testing.T) {
	// 直接构造 Cache，避免引入 Store，只验证 key 组合这一项职责。
	cache := &Cache[int]{prefix: "app:user"}
	// 非空 ID 必须追加一层冒号，不能与其他对象共享同一个 Redis key。
	generatedKey := cache.key("1")
	if generatedKey != "app:user:1" {
		t.Fatalf("got %q", generatedKey)
	}
	// 空 ID 用于允许调用方直接把 prefix 当作完整 key，不能产生末尾多余冒号。
	baseKey := cache.key("")
	if baseKey != "app:user" {
		t.Fatalf("got %q", baseKey)
	}
}

// TestRedisKeyPrefix 验证统一门面会为逻辑 key 添加应用级前缀，
// 这是不同服务共享 Redis 实例时隔离 key 空间的基础。
func TestRedisKeyPrefix(t *testing.T) {
	// 这里只构造最小 Redis 门面，Key 方法本身不需要真实客户端。
	redis := &Redis{keyPrefix: "app"}
	// 断言逻辑 key 被稳定转换为应用命名空间下的真实 key。
	generatedKey := redis.Key("users")
	if generatedKey != "app:users" {
		t.Fatalf("got %q", generatedKey)
	}
}

// TestCacheRejectsInvalidExpiration 验证显式传入负 TTL 会在访问 Store 前被拒绝，
// 防止调用方误把负数理解为“永不过期”。
func TestCacheRejectsInvalidExpiration(t *testing.T) {
	// Cache 没有配置 Store；若参数校验正确，函数应在访问 Store 之前返回。
	cache := &Cache[int]{}
	// 负 TTL 不是“永不过期”，必须明确提示调用方修正参数。
	setErr := cache.Set(context.Background(), "key", 1, -time.Second)
	if !errors.Is(setErr, ErrInvalidExpiration) {
		t.Fatalf("got %v want %v", setErr, ErrInvalidExpiration)
	}
}

// TestRankingKeysShareHashTag 验证排行榜内部 Hash 与 ZSet 共享 Cluster hash tag，
// 从而保证同时操作两个 key 的 Lua 脚本可以在 Redis Cluster 中执行。
func TestRankingKeysShareHashTag(t *testing.T) {
	// 构造排行榜后提取内部 Hash 与 ZSet 的真实 key。
	ranking := NewRanking[int](&Redis{}, "app:ranking")
	valuesKey, scoresKey := ranking.Keys()
	// 从载荷 key 中提取 {...}，确认分数 key 包含完全相同的 Cluster hash tag。
	valuesTag := valuesKey[strings.IndexByte(valuesKey, '{') : strings.IndexByte(valuesKey, '}')+1]
	if !strings.Contains(scoresKey, valuesTag) {
		t.Fatalf("keys do not share hash tag: %q %q", valuesKey, scoresKey)
	}
}

// TestDelayQueueKeysShareHashTag 验证延迟队列的四个内部 key 位于同一 slot，
// 避免 Schedule、Claim、Ack、Nack、Poll、Remove 的原子 Lua 脚本产生 CROSSSLOT 错误。
func TestDelayQueueKeysShareHashTag(t *testing.T) {
	queue := NewDelayQueue[int](&Redis{keyPrefix: "app"}, "jobs")
	valuesKey, scoresKey := queue.Keys()
	processingKey, claimsKey := queue.ReliableKeys()
	start := strings.IndexByte(valuesKey, '{')
	end := strings.IndexByte(valuesKey, '}')
	if start < 0 || end < start {
		t.Fatalf("values key has no hash tag: %q", valuesKey)
	}
	// 调度、处理中和所有权 key 必须复用载荷 key 中的完整 hash tag。
	tag := valuesKey[start : end+1]
	for _, key := range []string{scoresKey, processingKey, claimsKey} {
		if !strings.Contains(key, tag) {
			t.Fatalf("keys do not share hash tag: %q %q", valuesKey, key)
		}
	}
}

// TestRedisScore 验证 float64 到 Redis 分数边界字符串的转换，
// 特别确认正负无穷会转换为 Redis 接受的 +inf 和 -inf。
func TestRedisScore(t *testing.T) {
	// 表驱动同时覆盖普通浮点数和 Redis 分数查询常用的两个无穷边界。
	tests := map[float64]string{
		math.Inf(-1): "-inf",
		math.Inf(1):  "+inf",
		12.5:         "12.5",
	}
	// 每个输入都必须生成 Redis 命令可以直接接受的十进制或 inf 字符串。
	for input, want := range tests {
		scoreText := redisScore(input)
		if scoreText != want {
			t.Fatalf("redisScore(%v) = %q, want %q", input, scoreText, want)
		}
	}
}

// TestPubSubChannelPrefix 验证发布时添加命名空间，接收时移除命名空间，
// 使业务 handler 始终只接触稳定的逻辑 channel 名称。
func TestPubSubChannelPrefix(t *testing.T) {
	// 使用全局前缀 app 和 Pub/Sub 命名空间 events 构造两层隔离。
	pubsub := NewPubSub[int](&Redis{keyPrefix: "app"}, "events")
	// 发布侧应添加完整前缀，避免不同模块的同名 channel 相互干扰。
	redisChannel := pubsub.channel("created")
	if redisChannel != "app:events:created" {
		t.Fatalf("got %q", redisChannel)
	}
	// 订阅侧应移除内部前缀，让业务 handler 只看到逻辑名称。
	logicalChannel := pubsub.logicalChannel("app:events:created")
	if logicalChannel != "created" {
		t.Fatalf("got %q", logicalChannel)
	}
}

// TestPubSubRejectsEmptyChannels 验证空订阅列表会立即返回明确错误，
// 而不是向 Rueidis 发送一条不完整的 SUBSCRIBE 命令。
func TestPubSubRejectsEmptyChannels(t *testing.T) {
	// 不提供 mock 客户端，用来证明空 channel 会在构造 Redis 命令之前被拦截。
	pubsub := &PubSub[int]{}
	subscribeErr := pubsub.Subscribe(context.Background(), nil)
	if !errors.Is(subscribeErr, ErrEmptyChannels) {
		t.Fatalf("got %v want %v", subscribeErr, ErrEmptyChannels)
	}
}

// TestIdempotencyRejectsInvalidTTL 验证幂等标记必须设置正 TTL，
// 防止永久标记导致同一业务 ID 再也不能执行。
func TestIdempotencyRejectsInvalidTTL(t *testing.T) {
	// 不配置客户端；正确实现应先验证 TTL，再尝试生成 SET NX PX 命令。
	idempotency := &Idempotency{}
	_, tryErr := idempotency.Try(context.Background(), "event", 0)
	if !errors.Is(tryErr, ErrInvalidExpiration) {
		t.Fatalf("got %v want %v", tryErr, ErrInvalidExpiration)
	}
}

// TestLockAfterClose 验证关闭后的门面不能再获取锁，且返回统一的 ErrClosed。
func TestLockAfterClose(t *testing.T) {
	// 手动标记关闭状态，确保测试不会真的创建 rueidislock 连接。
	redis := &Redis{closed: true}
	// TryLock 必须统一返回 ErrClosed，便于调用方通过 errors.Is 判断生命周期错误。
	_, _, err := redis.TryLock(context.Background(), "job")
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v want %v", err, ErrClosed)
	}
}
