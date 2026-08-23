package rueidistpl

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/redis/rueidis"
)

// Hash 使用 Redis Hash 保存 JSON 对象，field 通常使用稳定业务 ID。
type Hash[T any] struct {
	client rueidis.Client
	key    string
}

// NewHash 创建 field 到 JSON 对象的类型化映射。
func NewHash[T any](redis *Redis, key string) *Hash[T] {
	return &Hash[T]{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (hash *Hash[T]) Key() string {
	return hash.key
}

// Set 新增或覆盖单个 field。
func (hash *Hash[T]) Set(ctx context.Context, field string, value T) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	command := hash.client.B().Hset().Key(hash.key).FieldValue().FieldValue(field, string(payload)).Build()
	return hash.client.Do(ctx, command).Error()
}

// SetNX 仅在 field 不存在时写入，写入成功返回 true。
func (hash *Hash[T]) SetNX(ctx context.Context, field string, value T) (bool, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	command := hash.client.B().Hsetnx().Key(hash.key).Field(field).Value(string(payload)).Build()
	return hash.client.Do(ctx, command).AsBool()
}

// SetMany 使用一次 HSET 按 field 字典序批量新增或覆盖；空 map 不发送命令。
func (hash *Hash[T]) SetMany(ctx context.Context, values map[string]T) error {
	if len(values) == 0 {
		return nil
	}
	fields := make([]string, 0, len(values))
	for field := range values {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	command := hash.client.B().Hset().Key(hash.key).FieldValue()
	for _, field := range fields {
		value := values[field]
		payload, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode hash field %q: %w", field, err)
		}
		command = command.FieldValue(field, string(payload))
	}
	return hash.client.Do(ctx, command.Build()).Error()
}

// Get 读取并反序列化单个 field；field 不存在时返回 Redis Nil 错误。
// 发生任何错误时返回 T 的零值。
func (hash *Hash[T]) Get(ctx context.Context, field string) (T, error) {
	raw, err := hash.client.Do(ctx, hash.client.B().Hget().Key(hash.key).Field(field).Build()).ToString()
	if err != nil {
		var zero T
		return zero, err
	}
	return decodeJSON[T](raw)
}

// GetMany 返回存在的 field；不存在的 field 不写入结果 map。
func (hash *Hash[T]) GetMany(ctx context.Context, fields ...string) (map[string]T, error) {
	values := make(map[string]T, len(fields))
	if len(fields) == 0 {
		return values, nil
	}
	messages, err := hash.client.Do(ctx, hash.client.B().Hmget().Key(hash.key).Field(fields...).Build()).ToArray()
	if err != nil {
		return nil, err
	}
	if len(messages) != len(fields) {
		return nil, fmt.Errorf("unexpected HMGET response length: got %d want %d", len(messages), len(fields))
	}
	for index, message := range messages {
		raw, valueErr := message.ToString()
		if rueidis.IsRedisNil(valueErr) {
			continue
		}
		if valueErr != nil {
			return nil, valueErr
		}
		value, err := decodeJSON[T](raw)
		if err != nil {
			return nil, fmt.Errorf("decode hash field %q: %w", fields[index], err)
		}
		values[fields[index]] = value
	}
	return values, nil
}

// All 返回 Hash 中的全部 field 和对象，适合数据量明确较小的场景。
func (hash *Hash[T]) All(ctx context.Context) (map[string]T, error) {
	rawValues, err := hash.client.Do(ctx, hash.client.B().Hgetall().Key(hash.key).Build()).AsStrMap()
	if err != nil {
		return nil, err
	}
	values := make(map[string]T, len(rawValues))
	for field, raw := range rawValues {
		value, err := decodeJSON[T](raw)
		if err != nil {
			return nil, fmt.Errorf("decode hash field %q: %w", field, err)
		}
		values[field] = value
	}
	return values, nil
}

// Keys 返回全部 field；大 Hash 应优先使用 Scan，避免单次返回过多数据。
func (hash *Hash[T]) Keys(ctx context.Context) ([]string, error) {
	return hash.client.Do(ctx, hash.client.B().Hkeys().Key(hash.key).Build()).AsStrSlice()
}

// Values 返回全部对象但不包含 field；大 Hash 应优先使用 Scan。
func (hash *Hash[T]) Values(ctx context.Context) ([]T, error) {
	rawValues, err := hash.client.Do(ctx, hash.client.B().Hvals().Key(hash.key).Build()).AsStrSlice()
	if err != nil {
		return nil, err
	}
	values := make([]T, len(rawValues))
	for index, raw := range rawValues {
		value, err := decodeJSON[T](raw)
		if err != nil {
			return nil, fmt.Errorf("decode hash value %d: %w", index, err)
		}
		values[index] = value
	}
	return values, nil
}

// HashScan 表示一次 HSCAN 的游标和本批对象。
type HashScan[T any] struct {
	// Cursor 为下一次扫描使用的游标；返回 0 表示扫描完成。
	Cursor uint64
	// Values 是本批次 field 到对象的映射。
	Values map[string]T
}

// Scan 分批扫描 Hash。pattern 为空表示不过滤，count 非正时默认使用 100。
// count 是 Redis 的数量提示，不保证每批严格返回该数量。
func (hash *Hash[T]) Scan(ctx context.Context, cursor uint64, pattern string, count int64) (HashScan[T], error) {
	if count <= 0 {
		count = 100
	}
	var (
		entry rueidis.ScanEntry
		err   error
	)
	if pattern == "" {
		entry, err = hash.client.Do(ctx, hash.client.B().Hscan().Key(hash.key).Cursor(cursor).Count(count).Build()).AsScanEntry()
	} else {
		entry, err = hash.client.Do(ctx, hash.client.B().Hscan().Key(hash.key).Cursor(cursor).Match(pattern).Count(count).Build()).AsScanEntry()
	}
	if err != nil {
		return HashScan[T]{}, err
	}
	if len(entry.Elements)%2 != 0 {
		return HashScan[T]{}, fmt.Errorf("unexpected HSCAN element count: %d", len(entry.Elements))
	}
	values := make(map[string]T, len(entry.Elements)/2)
	for index := 0; index < len(entry.Elements); index += 2 {
		field, raw := entry.Elements[index], entry.Elements[index+1]
		value, err := decodeJSON[T](raw)
		if err != nil {
			return HashScan[T]{}, fmt.Errorf("decode hash field %q: %w", field, err)
		}
		values[field] = value
	}
	return HashScan[T]{Cursor: entry.Cursor, Values: values}, nil
}

// IncrementInt 和 IncrementFloat 适用于保存数字字符串的独立 Hash field。
func (hash *Hash[T]) IncrementInt(ctx context.Context, field string, delta int64) (int64, error) {
	command := hash.client.B().Hincrby().Key(hash.key).Field(field).Increment(delta).Build()
	return hash.client.Do(ctx, command).AsInt64()
}

// IncrementFloat 原子增加浮点 field，并返回增加后的值。
func (hash *Hash[T]) IncrementFloat(ctx context.Context, field string, delta float64) (float64, error) {
	command := hash.client.B().Hincrbyfloat().Key(hash.key).Field(field).Increment(delta).Build()
	return hash.client.Do(ctx, command).AsFloat64()
}

// ExpireField 设置单个 Hash field 的 TTL，需要 Redis 7.4+。
func (hash *Hash[T]) ExpireField(ctx context.Context, field string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidExpiration
	}
	ttl = atLeastMillisecond(ttl)
	command := hash.client.B().Hpexpire().Key(hash.key).Milliseconds(ttl.Milliseconds()).Fields().Numfields(1).Field(field).Build()
	value, err := singleIntegerResult(hash.client.Do(ctx, command))
	return value == 1, err
}

// TTLField 返回 HPTTL；-1ms 表示不过期，-2ms 表示 field 不存在。
func (hash *Hash[T]) TTLField(ctx context.Context, field string) (time.Duration, error) {
	command := hash.client.B().Hpttl().Key(hash.key).Fields().Numfields(1).Field(field).Build()
	value, err := singleIntegerResult(hash.client.Do(ctx, command))
	if err != nil {
		return 0, err
	}
	return time.Duration(value) * time.Millisecond, nil
}

// ExpirationField 返回结构化 field 过期状态，避免调用方自行解释 HPTTL 的 -1ms/-2ms。
func (hash *Hash[T]) ExpirationField(ctx context.Context, field string) (ExpirationInfo, error) {
	command := hash.client.B().Hpttl().Key(hash.key).Fields().Numfields(1).Field(field).Build()
	milliseconds, err := singleIntegerResult(hash.client.Do(ctx, command))
	if err != nil {
		return ExpirationInfo{}, err
	}
	return expirationInfo(milliseconds)
}

// PersistField 移除单个 field 的过期时间，需要 Redis 7.4+。
func (hash *Hash[T]) PersistField(ctx context.Context, field string) (bool, error) {
	command := hash.client.B().Hpersist().Key(hash.key).Fields().Numfields(1).Field(field).Build()
	value, err := singleIntegerResult(hash.client.Do(ctx, command))
	return value == 1, err
}

// Exists 判断 field 是否存在。
func (hash *Hash[T]) Exists(ctx context.Context, field string) (bool, error) {
	return hash.client.Do(ctx, hash.client.B().Hexists().Key(hash.key).Field(field).Build()).AsBool()
}

// Delete 删除一个或多个 field，并返回实际删除数量。
func (hash *Hash[T]) Delete(ctx context.Context, fields ...string) (int64, error) {
	if len(fields) == 0 {
		return 0, nil
	}
	return hash.client.Do(ctx, hash.client.B().Hdel().Key(hash.key).Field(fields...).Build()).AsInt64()
}

// Len 返回 field 数量。
func (hash *Hash[T]) Len(ctx context.Context) (int64, error) {
	return hash.client.Do(ctx, hash.client.B().Hlen().Key(hash.key).Build()).AsInt64()
}

// Clear 删除整个 Hash key。
func (hash *Hash[T]) Clear(ctx context.Context) error {
	return hash.client.Do(ctx, hash.client.B().Del().Key(hash.key).Build()).Error()
}

func integerArray(result rueidis.RedisResult) ([]int64, error) {
	messages, err := result.ToArray()
	if err != nil {
		return nil, err
	}
	values := make([]int64, len(messages))
	for index, message := range messages {
		values[index], err = message.AsInt64()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func singleIntegerResult(result rueidis.RedisResult) (int64, error) {
	values, err := integerArray(result)
	if err != nil {
		return 0, err
	}
	if len(values) != 1 {
		return 0, fmt.Errorf("unexpected integer response length: got %d want 1", len(values))
	}
	return values[0], nil
}
