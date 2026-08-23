package rueidistpl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/rueidis"
)

// Set 使用 Redis Set 保存 JSON 对象，适合去重、成员判断和无序集合。
// Redis 以 JSON 字节串判断成员是否相同；自定义 MarshalJSON 时应保证同一业务值的输出稳定。
type Set[T any] struct {
	client rueidis.Client
	key    string
}

// SetScan 表示一次泛型 Set 游标扫描结果。
type SetScan[T any] struct {
	// Cursor 是下一次扫描使用的游标；返回 0 表示扫描完成。
	Cursor uint64
	// Values 是本批已完成 JSON 解码的成员，Redis Set 不保证顺序。
	Values []T
}

// NewSet 创建保存 JSON 对象的类型化 Redis Set。
func NewSet[T any](redis *Redis, key string) *Set[T] {
	return &Set[T]{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (set *Set[T]) Key() string {
	return set.key
}

// Add 添加一个或多个对象，返回实际新增的成员数；空输入返回 0 且不访问 Redis。
func (set *Set[T]) Add(ctx context.Context, values ...T) (int64, error) {
	encoded, err := encodeSetValues(values)
	if err != nil {
		return 0, err
	}
	if len(encoded) == 0 {
		return 0, nil
	}
	command := set.client.B().Sadd().Key(set.key).Member(encoded...).Build()
	return set.client.Do(ctx, command).AsInt64()
}

// Remove 删除 JSON 表示完全相同的对象，返回实际删除数；空输入返回 0。
func (set *Set[T]) Remove(ctx context.Context, values ...T) (int64, error) {
	encoded, err := encodeSetValues(values)
	if err != nil {
		return 0, err
	}
	if len(encoded) == 0 {
		return 0, nil
	}
	command := set.client.B().Srem().Key(set.key).Member(encoded...).Build()
	return set.client.Do(ctx, command).AsInt64()
}

// Contains 判断 JSON 表示完全相同的对象是否存在。
func (set *Set[T]) Contains(ctx context.Context, value T) (bool, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	command := set.client.B().Sismember().Key(set.key).Member(string(payload)).Build()
	return set.client.Do(ctx, command).AsBool()
}

// Members 返回全部强类型成员，顺序不固定；大集合应使用 Rueidis SSCAN Builder 分批扫描。
func (set *Set[T]) Members(ctx context.Context) ([]T, error) {
	rawValues, err := set.client.Do(ctx, set.client.B().Smembers().Key(set.key).Build()).AsStrSlice()
	if err != nil {
		return nil, err
	}
	values := make([]T, len(rawValues))
	for index, raw := range rawValues {
		value, err := decodeJSON[T](raw)
		if err != nil {
			return nil, fmt.Errorf("decode set member %d: %w", index, err)
		}
		values[index] = value
	}
	return values, nil
}

// Scan 分批扫描泛型 Set。pattern 为空时不过滤，count 非正时默认为 100。
// pattern 匹配的是成员的原始 JSON 字符串，不是 Go 字段或结构化 JSON 查询。
// count 是 Redis 的数量提示，实际返回条数可能不同。
func (set *Set[T]) Scan(ctx context.Context, cursor uint64, pattern string, count int64) (SetScan[T], error) {
	if count <= 0 {
		count = 100
	}
	command := set.client.B().Sscan().Key(set.key).Cursor(cursor)
	var (
		entry rueidis.ScanEntry
		err   error
	)
	if pattern == "" {
		entry, err = set.client.Do(ctx, command.Count(count).Build()).AsScanEntry()
	} else {
		entry, err = set.client.Do(ctx, command.Match(pattern).Count(count).Build()).AsScanEntry()
	}
	if err != nil {
		return SetScan[T]{}, err
	}
	values := make([]T, len(entry.Elements))
	for index, raw := range entry.Elements {
		value, err := decodeJSON[T](raw)
		if err != nil {
			return SetScan[T]{}, fmt.Errorf("decode set scan member %d: %w", index, err)
		}
		values[index] = value
	}
	return SetScan[T]{Cursor: entry.Cursor, Values: values}, nil
}

// Len 返回集合成员数。
func (set *Set[T]) Len(ctx context.Context) (int64, error) {
	return set.client.Do(ctx, set.client.B().Scard().Key(set.key).Build()).AsInt64()
}

// Clear 删除整个 Set key。
func (set *Set[T]) Clear(ctx context.Context) error {
	return set.client.Do(ctx, set.client.B().Del().Key(set.key).Build()).Error()
}

func encodeSetValues[T any](values []T) ([]string, error) {
	encoded := make([]string, len(values))
	for index, value := range values {
		payload, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode set member %d: %w", index, err)
		}
		encoded[index] = string(payload)
	}
	return encoded, nil
}
