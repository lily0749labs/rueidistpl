package rueidistpl

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/rueidis"
)

// Value 使用单个 Redis String 保存 JSON 对象，不经过 gocache。
type Value[T any] struct {
	client rueidis.Client
	key    string
}

// NewValue 创建使用单个 Redis String 保存的类型化 JSON 值。
func NewValue[T any](redis *Redis, key string) *Value[T] {
	return &Value[T]{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (value *Value[T]) Key() string {
	return value.key
}

// Set 写入对象；省略或传入非正 ttl 时不过期。
func (value *Value[T]) Set(ctx context.Context, data T, ttl ...time.Duration) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}
	command := value.client.B().Set().Key(value.key).Value(string(payload))
	if len(ttl) > 0 && ttl[0] > 0 {
		return value.client.Do(ctx, command.Px(atLeastMillisecond(ttl[0])).Build()).Error()
	}
	return value.client.Do(ctx, command.Build()).Error()
}

// Get 读取并反序列化 JSON 对象；key 不存在时返回 Rueidis 的 Redis Nil 错误。
// 发生任何错误时返回 T 的零值。
func (value *Value[T]) Get(ctx context.Context) (T, error) {
	raw, err := value.client.Do(ctx, value.client.B().Get().Key(value.key).Build()).ToString()
	if err != nil {
		var zero T
		return zero, err
	}
	return decodeJSON[T](raw)
}

// GetDelete 原子读取并删除对象；key 不存在时返回 Redis Nil 错误。
// 该命令需要 Redis 6.2+；发生任何错误时返回 T 的零值。
func (value *Value[T]) GetDelete(ctx context.Context) (T, error) {
	result := value.client.Do(ctx, value.client.B().Getdel().Key(value.key).Build())
	raw, err := result.ToString()
	if err != nil {
		var zero T
		return zero, err
	}
	return decodeJSON[T](raw)
}

// SetNX 仅在 key 不存在时写入对象。
func (value *Value[T]) SetNX(ctx context.Context, data T, ttl time.Duration) (bool, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return false, err
	}
	command := value.client.B().Set().Key(value.key).Value(string(payload)).Nx()
	var result rueidis.RedisResult
	if ttl > 0 {
		result = value.client.Do(ctx, command.Px(atLeastMillisecond(ttl)).Build())
	} else {
		result = value.client.Do(ctx, command.Build())
	}
	if err := result.Error(); err != nil {
		if rueidis.IsRedisNil(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Exists 判断该值是否存在。
func (value *Value[T]) Exists(ctx context.Context) (bool, error) {
	count, err := value.client.Do(ctx, value.client.B().Exists().Key(value.key).Build()).AsInt64()
	return count > 0, err
}

// Expire 为已存在的值设置过期时间；key 不存在时返回 false。
func (value *Value[T]) Expire(ctx context.Context, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidExpiration
	}
	ttl = atLeastMillisecond(ttl)
	return value.client.Do(ctx, value.client.B().Pexpire().Key(value.key).Milliseconds(ttl.Milliseconds()).Build()).AsBool()
}

// TTL 返回 PTTL；-1ms 表示不过期，-2ms 表示 key 不存在。
func (value *Value[T]) TTL(ctx context.Context) (time.Duration, error) {
	milliseconds, err := value.client.Do(ctx, value.client.B().Pttl().Key(value.key).Build()).AsInt64()
	return time.Duration(milliseconds) * time.Millisecond, err
}

// Expiration 返回结构化过期状态，避免调用方自行解释 TTL 的 -1ms/-2ms。
func (value *Value[T]) Expiration(ctx context.Context) (ExpirationInfo, error) {
	milliseconds, err := value.client.Do(ctx, value.client.B().Pttl().Key(value.key).Build()).AsInt64()
	if err != nil {
		return ExpirationInfo{}, err
	}
	return expirationInfo(milliseconds)
}

// Delete 删除该值；实际删除成功返回 true。
func (value *Value[T]) Delete(ctx context.Context) (bool, error) {
	deleted, err := value.client.Do(ctx, value.client.B().Del().Key(value.key).Build()).AsInt64()
	return deleted > 0, err
}
