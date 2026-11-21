package rueidistpl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"github.com/redis/rueidis/rueidislock"
	"go.uber.org/mock/gomock"
)

// unitLocker 是锁门面的测试替身，只记录调用，不访问 Redis。
// 它让测试可以确认 Lock/TryLock 是否正确委托给 rueidislock。
type unitLocker struct {
	withNames    []string
	tryNames     []string
	forceNames   []string
	closed       bool
	returnedCtx  context.Context
	returnedStop context.CancelFunc
}

func (locker *unitLocker) WithContext(_ context.Context, name string) (context.Context, context.CancelFunc, error) {
	locker.withNames = append(locker.withNames, name)
	return locker.returnedCtx, locker.returnedStop, nil
}

func (locker *unitLocker) TryWithContext(_ context.Context, name string) (context.Context, context.CancelFunc, error) {
	locker.tryNames = append(locker.tryNames, name)
	return locker.returnedCtx, locker.returnedStop, nil
}

func (locker *unitLocker) ForceWithContext(_ context.Context, name string) (context.Context, context.CancelFunc, error) {
	locker.forceNames = append(locker.forceNames, name)
	return locker.returnedCtx, locker.returnedStop, nil
}

func (locker *unitLocker) Client() rueidis.Client { return nil }
func (locker *unitLocker) Close()                 { locker.closed = true }

// TestNewRedis 验证统一客户端构造逻辑：
//  1. ClientOption 原样传给 Rueidis；
//  2. 自动补全锁 key 前缀、安全多数派和续租参数；
//  3. 创建过程不会提前创建锁连接。
func TestNewRedis(t *testing.T) {
	// Arrange：替换客户端工厂，使 New 完整执行配置逻辑但不建立网络连接。
	client := rueidismock.NewClient(gomock.NewController(t))
	originalFactory := rueidisClientFactory
	t.Cleanup(func() { rueidisClientFactory = originalFactory })

	// 工厂内部核对 Rueidis 原生配置是否未经丢失或篡改地传入。
	called := 0
	rueidisClientFactory = func(option rueidis.ClientOption) (rueidis.Client, error) {
		called++
		if len(option.InitAddress) != 1 || option.InitAddress[0] != "redis.internal:6379" || option.Password != "secret" {
			t.Fatalf("客户端配置传递错误：%+v", option)
		}
		return client, nil
	}

	// Act：只提供连接和应用前缀，其余缓存、锁参数应走包内默认值。
	redis, err := New(Config{
		ClientOption: rueidis.ClientOption{InitAddress: []string{"redis.internal:6379"}, Password: "secret"},
		KeyPrefix:    "orders",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Assert：业务客户端和 gocache 必须完成初始化，但锁客户端仍保持惰性未创建。
	if called != 1 || redis.client != client || redis.cache == nil {
		t.Fatalf("构造结果不完整：called=%d redis=%+v", called, redis)
	}
	if redis.keyPrefix != "orders" || redis.locker != nil {
		t.Fatalf("前缀或惰性锁状态错误：prefix=%q locker=%v", redis.keyPrefix, redis.locker)
	}
	if redis.lockerOption.KeyPrefix != "orders:locks" ||
		redis.lockerOption.KeyMajority != 2 ||
		redis.lockerOption.KeyValidity != 5*time.Second ||
		redis.lockerOption.ExtendInterval != 2500*time.Millisecond {
		t.Fatalf("锁默认配置错误：%+v", redis.lockerOption)
	}

	// 最后确认构造出的门面能够正常关闭注入的底层客户端。
	client.EXPECT().Close()
	redis.Close()
}

// TestNewRedisRejectsUnsafeLockerTiming 验证续期间隔不能达到或超过租约有效期，
// 并确保无效配置在建立业务连接前就被拒绝。
func TestNewRedisRejectsUnsafeLockerTiming(t *testing.T) {
	originalFactory := rueidisClientFactory
	t.Cleanup(func() { rueidisClientFactory = originalFactory })
	called := false
	rueidisClientFactory = func(rueidis.ClientOption) (rueidis.Client, error) {
		called = true
		return nil, nil
	}

	redis, err := New(Config{LockerOption: rueidislock.LockerOption{
		KeyValidity:    time.Second,
		ExtendInterval: time.Second,
	}})
	if redis != nil || !errors.Is(err, ErrInvalidLockerConfig) || called {
		t.Fatalf("redis=%v err=%v clientFactoryCalled=%t", redis, err, called)
	}
}

// TestNewRedisClientError 验证底层 Rueidis 创建失败时，错误会原样返回，
// 且不会生成一个处于半初始化状态的 Redis 门面。
func TestNewRedisClientError(t *testing.T) {
	// Arrange：让底层客户端工厂稳定返回一个可识别错误。
	want := errors.New("create client failed")
	originalFactory := rueidisClientFactory
	t.Cleanup(func() { rueidisClientFactory = originalFactory })
	rueidisClientFactory = func(rueidis.ClientOption) (rueidis.Client, error) { return nil, want }

	// Act/Assert：New 必须原样返回错误，并且不能返回半初始化实例。
	redis, err := New(Config{})
	if redis != nil || !errors.Is(err, want) {
		t.Fatalf("redis=%v err=%v，期望 nil 和 %v", redis, err, want)
	}
}

// TestNewRedisCustomDefaults 验证调用方显式配置会被保留：
// 小于 Store 精度的正 CacheTTL 会走最小值归一化分支，自定义锁前缀和多数派不会被默认值覆盖，
// 正的客户端缓存 TTL 也不会回退到包内默认值。
func TestNewRedisCustomDefaults(t *testing.T) {
	// Arrange：注入 mock 客户端，同时准备一组非默认缓存和锁参数。
	client := rueidismock.NewClient(gomock.NewController(t))
	originalFactory := rueidisClientFactory
	t.Cleanup(func() { rueidisClientFactory = originalFactory })
	rueidisClientFactory = func(rueidis.ClientOption) (rueidis.Client, error) { return client, nil }

	// Act：执行构造以覆盖正 TTL、显式锁前缀和多节点多数派分支。
	redis, err := New(Config{
		CacheTTL:           time.Nanosecond,
		ClientSideCacheTTL: 2 * time.Second,
		LockerOption: rueidislock.LockerOption{
			KeyPrefix:   "custom-locks",
			KeyMajority: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Assert：调用方显式设置的锁配置优先级高于包内默认值。
	if redis.lockerOption.KeyPrefix != "custom-locks" || redis.lockerOption.KeyMajority != 2 {
		t.Fatalf("显式锁配置被覆盖：%+v", redis.lockerOption)
	}
	client.EXPECT().Close()
	redis.Close()
}

// TestRedisLockDelegation 验证锁客户端按需创建且只创建一次，
// Lock 与 TryLock 分别调用等待式和单次尝试式接口，Close 会同时关闭锁和业务客户端。
func TestRedisLockDelegation(t *testing.T) {
	// Arrange：业务客户端使用官方 mock，锁客户端使用只记录方法调用的 unitLocker。
	redis, client := newMockRedis(t)
	lockCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	locker := &unitLocker{returnedCtx: lockCtx, returnedStop: cancel}
	originalFactory := rueidisLockerFactory
	t.Cleanup(func() { rueidisLockerFactory = originalFactory })

	// 工厂计数用于确认第一次请求才创建锁客户端，第二次请求直接复用。
	created := 0
	rueidisLockerFactory = func(option rueidislock.LockerOption) (rueidislock.Locker, error) {
		created++
		if option.KeyPrefix != "" {
			t.Fatalf("锁配置被意外修改：%+v", option)
		}
		return locker, nil
	}

	// Act：先调用等待式 Lock，再调用非等待式 TryLock。
	gotCtx, gotCancel, err := redis.Lock(context.Background(), "refresh")
	if err != nil || gotCtx != lockCtx || gotCancel == nil {
		t.Fatalf("Lock ctx=%v cancel=%v err=%v", gotCtx, gotCancel, err)
	}
	_, _, tryLockErr := redis.TryLock(context.Background(), "settle")
	if tryLockErr != nil {
		t.Fatal(tryLockErr)
	}
	// Assert：两个入口委托给不同方法，且整个生命周期只创建一个 Locker。
	if created != 1 || len(locker.withNames) != 1 || locker.withNames[0] != "refresh" || len(locker.tryNames) != 1 || locker.tryNames[0] != "settle" {
		t.Fatalf("锁委托错误：created=%d with=%v try=%v", created, locker.withNames, locker.tryNames)
	}

	// Close 应同时释放已经创建的 Locker 和业务 Rueidis 客户端。
	client.EXPECT().Close()
	redis.Close()
	if !locker.closed {
		t.Fatal("Redis.Close 应关闭已经创建的锁客户端")
	}
}

// TestRedisFencedLock 验证获取锁后通过独立持久计数器生成 fencing token，
// 并确认计数器失败时立即释放刚取得的锁。
func TestRedisFencedLock(t *testing.T) {
	redis, client := newMockRedis(t)
	lockCtx := context.Background()
	released := 0
	locker := &unitLocker{returnedCtx: lockCtx, returnedStop: func() { released++ }}
	originalFactory := rueidisLockerFactory
	t.Cleanup(func() { rueidisLockerFactory = originalFactory })
	rueidisLockerFactory = func(rueidislock.LockerOption) (rueidislock.Locker, error) { return locker, nil }

	fencingKey := redis.FencingKey("refresh")
	client.EXPECT().Do(lockCtx, rueidismock.Match("INCR", fencingKey)).Return(rueidismock.Result(rueidismock.RedisInt64(42)))
	gotCtx, token, unlock, err := redis.LockFenced(context.Background(), "refresh")
	if err != nil || gotCtx != lockCtx || token != 42 || unlock == nil {
		t.Fatalf("ctx=%v token=%d unlock=%v err=%v", gotCtx, token, unlock, err)
	}
	unlock()
	if released != 1 || len(locker.withNames) != 1 || locker.withNames[0] != "refresh" {
		t.Fatalf("released=%d with=%v", released, locker.withNames)
	}

	redis.locker = nil
	locker = &unitLocker{returnedCtx: lockCtx, returnedStop: func() { released++ }}
	tokenErr := errors.New("counter unavailable")
	client.EXPECT().Do(lockCtx, rueidismock.Match("INCR", redis.FencingKey("settle"))).Return(rueidismock.ErrorResult(tokenErr))
	_, _, _, err = redis.TryLockFenced(context.Background(), "settle")
	if !errors.Is(err, tokenErr) || released != 2 {
		t.Fatalf("err=%v released=%d", err, released)
	}
}

// TestRedisWithLockReportsLostLease 验证 handler 即使忽略取消并返回 nil，
// 门面仍会把执行期间的锁租约丢失提升为明确错误。
func TestRedisWithLockReportsLostLease(t *testing.T) {
	redis, _ := newMockRedis(t)
	lockCtx, cancelLock := context.WithCancel(context.Background())
	cancelLock()
	locker := &unitLocker{returnedCtx: lockCtx, returnedStop: func() {}}
	originalFactory := rueidisLockerFactory
	t.Cleanup(func() { rueidisLockerFactory = originalFactory })
	rueidisLockerFactory = func(rueidislock.LockerOption) (rueidislock.Locker, error) { return locker, nil }

	err := redis.WithLock(context.Background(), "job", func(ctx context.Context) error {
		if ctx != lockCtx {
			t.Fatal("handler did not receive lock context")
		}
		return nil
	})
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("got %v want %v", err, ErrLockLost)
	}
	if !errors.Is(redis.WithLock(context.Background(), "job", nil), ErrNilLockHandler) {
		t.Fatal("nil handler should be rejected")
	}
}

// TestRedisLockerFactoryError 验证锁客户端首次创建失败时错误会返回给调用方，
// 并且失败实例不会被缓存，后续调用仍可重新尝试创建。
func TestRedisLockerFactoryError(t *testing.T) {
	// Arrange：锁工厂每次都失败，并记录实际创建次数。
	redis, _ := newMockRedis(t)
	want := errors.New("create locker failed")
	originalFactory := rueidisLockerFactory
	t.Cleanup(func() { rueidisLockerFactory = originalFactory })
	created := 0
	rueidisLockerFactory = func(rueidislock.LockerOption) (rueidislock.Locker, error) {
		created++
		return nil, want
	}

	// Act：分别从 Lock 和 TryLock 触发惰性创建。
	_, _, lockErr := redis.Lock(context.Background(), "job")
	if !errors.Is(lockErr, want) {
		t.Fatalf("got %v want %v", lockErr, want)
	}
	_, _, tryLockErr := redis.TryLock(context.Background(), "job")
	if !errors.Is(tryLockErr, want) {
		t.Fatalf("got %v want %v", tryLockErr, want)
	}
	// Assert：失败对象不能缓存，否则后续请求将无法重新恢复。
	if created != 2 || redis.locker != nil {
		t.Fatalf("失败锁不应缓存：created=%d locker=%v", created, redis.locker)
	}
}

// TestRedisStringAndKeyOperations 验证字符串、批量删除、批量存在性检查等基础门面：
// 除了断言返回值，还精确核对全局 key 前缀和发送给 Redis 的命令参数。
func TestRedisStringAndKeyOperations(t *testing.T) {
	// Arrange：所有预期命令都包含 app 前缀，从命令层验证门面没有漏加命名空间。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	if redis.Client() != client {
		t.Fatal("Client should expose the underlying Rueidis client")
	}

	// 按业务调用顺序声明 mock 响应；DoMulti 还模拟部分 key 不存在的情况。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("PING")).Return(rueidismock.Result(rueidismock.RedisString("PONG"))),
		client.EXPECT().Do(ctx, rueidismock.Match("GET", "app:name")).Return(rueidismock.Result(rueidismock.RedisBlobString("Alice"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:name", "Alice")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:name", "Alice", "PX", "1000")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:name", "Alice", "PX", "1")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().DoMulti(ctx, rueidismock.Match("DEL", "app:a"), rueidismock.Match("DEL", "app:b")).Return([]rueidis.RedisResult{
			rueidismock.Result(rueidismock.RedisInt64(1)),
			rueidismock.Result(rueidismock.RedisInt64(0)),
		}),
		client.EXPECT().DoMulti(ctx, rueidismock.Match("EXISTS", "app:a"), rueidismock.Match("EXISTS", "app:b")).Return([]rueidis.RedisResult{
			rueidismock.Result(rueidismock.RedisInt64(1)),
			rueidismock.Result(rueidismock.RedisInt64(1)),
		}),
		client.EXPECT().DoMulti(ctx, rueidismock.Match("EXISTS", "app:a")).Return([]rueidis.RedisResult{
			rueidismock.Result(rueidismock.RedisInt64(1)),
		}),
	)

	// Act/Assert：依次执行健康检查、字符串读写和批量 key 操作，核对聚合数量。
	pingErr := redis.Ping(ctx)
	if pingErr != nil {
		t.Fatal(pingErr)
	}
	getStringValue, getStringErr := redis.GetString(ctx, "name")
	if getStringErr != nil || getStringValue != "Alice" {
		t.Fatalf("GetString value=%q err=%v", getStringValue, getStringErr)
	}
	setStringErr := redis.SetString(ctx, "name", "Alice")
	if setStringErr != nil {
		t.Fatal(setStringErr)
	}
	setStringErr2 := redis.SetString(ctx, "name", "Alice", time.Second)
	if setStringErr2 != nil {
		t.Fatal(setStringErr2)
	}
	// Redis 的 PX 不接受 0；纳秒级正 TTL 必须提升为 1ms，不能生成非法的 PX 0。
	setStringErr3 := redis.SetString(ctx, "name", "Alice", time.Nanosecond)
	if setStringErr3 != nil {
		t.Fatal(setStringErr3)
	}
	deletedCount, deleteErr := redis.Delete(ctx, "a", "b")
	if deleteErr != nil || deletedCount != 1 {
		t.Fatalf("Delete value=%d err=%v", deletedCount, deleteErr)
	}
	existingCount, existsErr := redis.Exists(ctx, "a", "b")
	if existsErr != nil || existingCount != 2 {
		t.Fatalf("Exists value=%d err=%v", existingCount, existsErr)
	}
	hasKey, hasErr := redis.Has(ctx, "a")
	if hasErr != nil || !hasKey {
		t.Fatalf("Has value=%t err=%v", hasKey, hasErr)
	}
	// 空批量参数应直接返回 0，不能向 Rueidis 发送不完整命令。
	emptyDeletedCount, deleteErr2 := redis.Delete(ctx)
	if deleteErr2 != nil || emptyDeletedCount != 0 {
		t.Fatalf("empty Delete value=%d err=%v", emptyDeletedCount, deleteErr2)
	}
	emptyExistingCount, existsErr2 := redis.Exists(ctx)
	if existsErr2 != nil || emptyExistingCount != 0 {
		t.Fatalf("empty Exists value=%d err=%v", emptyExistingCount, existsErr2)
	}
}

// TestRedisCloseIsIdempotent 验证重复 Close 不会重复关闭底层连接。
func TestRedisCloseIsIdempotent(t *testing.T) {
	// Arrange：底层 mock 只允许 Close 被调用一次。
	redis, client := newMockRedis(t)
	client.EXPECT().Close().Times(1)
	// Act：连续关闭两次，第二次应该只读取 closed 状态后返回。
	redis.Close()
	redis.Close()
	// Assert：门面的生命周期状态必须同步标记为已关闭。
	if !redis.closed {
		t.Fatal("Redis should be marked closed")
	}
}

// TestRedisLockAfterClose 验证客户端关闭后不能再创建或获取分布式锁。
func TestRedisLockAfterClose(t *testing.T) {
	// Arrange：直接构造关闭状态，避免创建真实锁客户端。
	redis := &Redis{closed: true}
	// Act/Assert：等待式 Lock 必须立即返回统一生命周期错误。
	_, _, lockErr := redis.Lock(context.Background(), "job")
	if !errors.Is(lockErr, ErrClosed) {
		t.Fatalf("got %v want %v", lockErr, ErrClosed)
	}
}
