package rueidistpl

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"

	cachelib "github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	rueidisstore "github.com/eko/gocache/store/rueidis/v4"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidislock"
)

const (
	defaultCacheTTL           = 10 * time.Minute
	defaultClientSideCacheTTL = 5 * time.Second
)

// ErrClosed 表示 Redis 门面已经关闭，不能再创建或获取锁客户端。
var ErrClosed = errors.New("rueidistpl: redis client is closed")

// 工厂变量只作为包内测试注入点；生产环境始终使用 Rueidis 官方构造函数。
var rueidisClientFactory = rueidis.NewClient
var rueidisLockerFactory = rueidislock.NewLocker

// Config 复用 Rueidis 与 rueidislock 的原生配置，只补充模板需要的缓存默认值。
type Config struct {
	// ClientOption 是 Rueidis 原生连接配置，例如地址、认证、集群和连接池参数。
	ClientOption rueidis.ClientOption
	// KeyPrefix 会自动添加到本包管理的全部业务 key 前面。
	KeyPrefix string
	// CacheTTL 是 NewCache[T] 的默认过期时间，非正值使用 10 分钟。
	CacheTTL time.Duration
	// ClientSideCacheTTL 控制 gocache Rueidis Store 的客户端缓存时间。
	ClientSideCacheTTL time.Duration
	// LockerOption 配置 rueidislock；其中 ClientOption 会统一使用上面的连接配置。
	LockerOption rueidislock.LockerOption
}

// Redis 统一管理业务客户端、JSON 缓存和按需创建的分布式锁客户端。
type Redis struct {
	client       rueidis.Client
	cache        *cachelib.Cache[string]
	keyPrefix    string
	lockerOption rueidislock.LockerOption

	mutex  sync.Mutex
	locker rueidislock.Locker
	closed bool
}

// New 创建统一 Redis 门面。只有首次使用锁时才会建立 rueidislock 专用客户端。
func New(config Config) (*Redis, error) {
	client, err := rueidisClientFactory(config.ClientOption)
	if err != nil {
		return nil, err
	}

	cacheTTL := config.CacheTTL
	if cacheTTL <= 0 {
		cacheTTL = defaultCacheTTL
	} else if cacheTTL < time.Second {
		cacheTTL = time.Second
	}
	clientSideCacheTTL := config.ClientSideCacheTTL
	if clientSideCacheTTL <= 0 {
		clientSideCacheTTL = defaultClientSideCacheTTL
	}
	cacheManager := cachelib.New[string](rueidisstore.NewRueidis(
		client,
		store.WithExpiration(cacheTTL),
		store.WithClientSideCaching(clientSideCacheTTL),
	))

	lockerOption := config.LockerOption
	lockerOption.ClientOption = config.ClientOption
	if lockerOption.KeyPrefix == "" {
		lockerOption.KeyPrefix = joinKey(config.KeyPrefix, "locks")
	}
	if lockerOption.KeyMajority <= 0 {
		lockerOption.KeyMajority = 1
	}

	return &Redis{
		client:       client,
		cache:        cacheManager,
		keyPrefix:    config.KeyPrefix,
		lockerOption: lockerOption,
	}, nil
}

// Client 返回原生 Rueidis 客户端，用于模板未覆盖的命令。
func (redis *Redis) Client() rueidis.Client {
	return redis.client
}

// Key 返回应用前缀处理后的真实 Redis key。
func (redis *Redis) Key(key string) string {
	return joinKey(redis.keyPrefix, key)
}

// Ping 检查 Redis 业务连接是否可用。
func (redis *Redis) Ping(ctx context.Context) error {
	return redis.client.Do(ctx, redis.client.B().Ping().Build()).Error()
}

// Close 关闭业务客户端，以及已经按需创建的锁客户端。
func (redis *Redis) Close() {
	redis.mutex.Lock()
	if redis.closed {
		redis.mutex.Unlock()
		return
	}
	redis.closed = true
	locker := redis.locker
	redis.mutex.Unlock()

	if locker != nil {
		locker.Close()
	}
	redis.client.Close()
}

func (redis *Redis) getLocker() (rueidislock.Locker, error) {
	redis.mutex.Lock()
	defer redis.mutex.Unlock()

	if redis.closed {
		return nil, ErrClosed
	}
	if redis.locker != nil {
		return redis.locker, nil
	}
	locker, err := rueidisLockerFactory(redis.lockerOption)
	if err != nil {
		return nil, err
	}
	redis.locker = locker
	return locker, nil
}

// Lock 等待获得自动续期的分布式锁。
func (redis *Redis) Lock(ctx context.Context, name string) (context.Context, context.CancelFunc, error) {
	locker, err := redis.getLocker()
	if err != nil {
		return nil, nil, err
	}
	return locker.WithContext(ctx, name)
}

// TryLock 只尝试一次；锁已被占用时返回 rueidislock.ErrNotLocked。
func (redis *Redis) TryLock(ctx context.Context, name string) (context.Context, context.CancelFunc, error) {
	locker, err := redis.getLocker()
	if err != nil {
		return nil, nil, err
	}
	return locker.TryWithContext(ctx, name)
}

// GetString 读取普通字符串，并自动应用全局 key 前缀。
func (redis *Redis) GetString(ctx context.Context, key string) (string, error) {
	return redis.client.Do(ctx, redis.client.B().Get().Key(redis.Key(key)).Build()).ToString()
}

// SetString 写入字符串；可选 ttl，省略或不大于零时不过期，小于 1ms 的正值提升为 1ms。
func (redis *Redis) SetString(ctx context.Context, key, value string, ttl ...time.Duration) error {
	command := redis.client.B().Set().Key(redis.Key(key)).Value(value)
	if len(ttl) > 0 && ttl[0] > 0 {
		return redis.client.Do(ctx, command.Px(atLeastMillisecond(ttl[0])).Build()).Error()
	}
	return redis.client.Do(ctx, command.Build()).Error()
}

// Delete 删除一个或多个逻辑 key，并返回实际删除的 key 数量。
// 多个 key 分别发送 DEL，因此在 Redis Cluster 中不要求这些 key 位于同一 slot。
// 部分失败时返回已确认的删除数量和聚合错误；需要逐 key 明细时使用 DeleteEach。
func (redis *Redis) Delete(ctx context.Context, keys ...string) (int64, error) {
	result := redis.DeleteEach(ctx, keys...)
	return result.Deleted, result.Err
}

// Exists 返回实际存在的逻辑 key 数量；多个 key 会分别查询以兼容 Redis Cluster。
// 部分失败时返回已确认的存在数量和聚合错误；需要逐 key 明细时使用 ExistsEach。
func (redis *Redis) Exists(ctx context.Context, keys ...string) (int64, error) {
	result := redis.ExistsEach(ctx, keys...)
	return result.Existing, result.Err
}

// Has 判断单个逻辑 key 是否存在。
func (redis *Redis) Has(ctx context.Context, key string) (bool, error) {
	count, err := redis.Exists(ctx, key)
	return count > 0, err
}

func joinKey(prefix, key string) string {
	if prefix == "" {
		return key
	}
	if key == "" {
		return prefix
	}
	return prefix + ":" + key
}

func atLeastMillisecond(duration time.Duration) time.Duration {
	if duration > 0 && duration < time.Millisecond {
		return time.Millisecond
	}
	return duration
}

func compositeKeyPrefix(redis *Redis, namespace string) string {
	namespace = redis.Key(namespace)
	tag := base64.RawURLEncoding.EncodeToString([]byte(namespace))
	if tag == "" {
		tag = "default"
	}
	// Redis 遇到首个空 {} 后会对整个 key 哈希，不会继续使用后面的 tag。
	// 仅为这类原先不能共用 slot 的命名空间前置 tag，保留其他既有键名。
	if opening := strings.IndexByte(namespace, '{'); opening >= 0 && opening+1 < len(namespace) && namespace[opening+1] == '}' {
		return "{" + tag + "}:" + namespace
	}
	return namespace + ":{" + tag + "}"
}
