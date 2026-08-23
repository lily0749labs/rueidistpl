package rueidistpl

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/rueidis"
)

// List 使用 Redis List 保存 JSON 对象。
type List[T any] struct {
	client rueidis.Client
	key    string
}

// NewList 创建保存 JSON 对象的 Redis List。
func NewList[T any](redis *Redis, key string) *List[T] {
	return &List[T]{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (list *List[T]) Key() string {
	return list.key
}

// PushLeft 将对象序列化后从左侧压入，并返回列表新长度。
func (list *List[T]) PushLeft(ctx context.Context, values ...T) (int64, error) {
	encoded, err := encodeListValues(values)
	if err != nil {
		return 0, err
	}
	if len(encoded) == 0 {
		return list.Len(ctx)
	}
	return list.client.Do(ctx, list.client.B().Lpush().Key(list.key).Element(encoded...).Build()).AsInt64()
}

// PushRight 将对象序列化后从右侧压入，并返回列表新长度。
func (list *List[T]) PushRight(ctx context.Context, values ...T) (int64, error) {
	encoded, err := encodeListValues(values)
	if err != nil {
		return 0, err
	}
	if len(encoded) == 0 {
		return list.Len(ctx)
	}
	return list.client.Do(ctx, list.client.B().Rpush().Key(list.key).Element(encoded...).Build()).AsInt64()
}

// PopLeft 从左侧弹出并反序列化一个对象；空列表返回 Redis Nil 错误，错误时返回 T 的零值。
func (list *List[T]) PopLeft(ctx context.Context) (T, error) {
	return decodeListResult[T](list.client.Do(ctx, list.client.B().Lpop().Key(list.key).Build()))
}

// PopRight 从右侧弹出并反序列化一个对象；空列表返回 Redis Nil 错误，错误时返回 T 的零值。
func (list *List[T]) PopRight(ctx context.Context) (T, error) {
	return decodeListResult[T](list.client.Do(ctx, list.client.B().Rpop().Key(list.key).Build()))
}

// Index 返回指定下标的对象，负数下标遵循 Redis 语义；错误时返回 T 的零值。
func (list *List[T]) Index(ctx context.Context, index int64) (T, error) {
	return decodeListResult[T](list.client.Do(ctx, list.client.B().Lindex().Key(list.key).Index(index).Build()))
}

// Set 覆盖指定下标的对象；下标越界时返回 Redis 错误。
func (list *List[T]) Set(ctx context.Context, index int64, value T) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return list.client.Do(ctx, list.client.B().Lset().Key(list.key).Index(index).Element(string(payload)).Build()).Error()
}

// Range 返回下标闭区间内的对象，stop=-1 表示列表末尾。
func (list *List[T]) Range(ctx context.Context, start, stop int64) ([]T, error) {
	rawValues, err := list.client.Do(ctx, list.client.B().Lrange().Key(list.key).Start(start).Stop(stop).Build()).AsStrSlice()
	if err != nil {
		return nil, err
	}
	values := make([]T, len(rawValues))
	for index, raw := range rawValues {
		value, err := decodeJSON[T](raw)
		if err != nil {
			return nil, fmt.Errorf("decode list index %d: %w", start+int64(index), err)
		}
		values[index] = value
	}
	return values, nil
}

// Remove 删除 count 个与 value JSON 表示完全相同的元素。
func (list *List[T]) Remove(ctx context.Context, count int64, value T) (int64, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	return list.client.Do(ctx, list.client.B().Lrem().Key(list.key).Count(count).Element(string(payload)).Build()).AsInt64()
}

// Len 返回列表长度。
func (list *List[T]) Len(ctx context.Context) (int64, error) {
	return list.client.Do(ctx, list.client.B().Llen().Key(list.key).Build()).AsInt64()
}

// Trim 只保留下标闭区间内的对象。
func (list *List[T]) Trim(ctx context.Context, start, stop int64) error {
	return list.client.Do(ctx, list.client.B().Ltrim().Key(list.key).Start(start).Stop(stop).Build()).Error()
}

// Clear 删除整个列表 key。
func (list *List[T]) Clear(ctx context.Context) error {
	return list.client.Do(ctx, list.client.B().Del().Key(list.key).Build()).Error()
}

func encodeListValues[T any](values []T) ([]string, error) {
	encoded := make([]string, len(values))
	for index, value := range values {
		payload, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode list index %d: %w", index, err)
		}
		encoded[index] = string(payload)
	}
	return encoded, nil
}

func decodeListResult[T any](result rueidis.RedisResult) (T, error) {
	raw, err := result.ToString()
	if err != nil {
		var zero T
		return zero, err
	}
	return decodeJSON[T](raw)
}
