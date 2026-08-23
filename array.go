package rueidistpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/redis/rueidis"
)

var (
	// ErrArrayValuesRequired 表示 ARINSERT/ARRING 缺少 Redis 要求的至少一个 value。
	ErrArrayValuesRequired = errors.New("rueidistpl: array requires at least one value")
	// ErrInvalidArrayRingSize 表示 ARRING 的窗口大小不是正数。
	ErrInvalidArrayRingSize = errors.New("rueidistpl: array ring size must be positive")
)

// Array 使用 Redis 8.8+ 原生 Array 保存 JSON 对象。
// Array 可以高效表示稀疏下标；连接 Redis 8.6 及以下版本时，调用这些方法会收到未知命令错误。
type Array[T any] struct {
	client rueidis.Client
	key    string
}

// ArrayItem 表示 Redis Array 的一个下标。
// Exists=false 表示该下标是空槽，此时 Value 保持 T 的零值。
type ArrayItem[T any] struct {
	Index  int64
	Exists bool
	Value  T
}

// ArrayRange 表示 ARDELRANGE 使用的下标闭区间。
type ArrayRange struct {
	Start int64
	End   int64
}

// NewArray 创建保存 JSON 对象的 Redis 8.8+ Array，并自动应用全局 key 前缀。
func NewArray[T any](redis *Redis, key string) *Array[T] {
	return &Array[T]{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (array *Array[T]) Key() string {
	return array.key
}

// Set 从 start 开始连续写入对象，返回原先为空、此次新建的槽位数。
// 空输入返回 0 且不访问 Redis。
func (array *Array[T]) Set(ctx context.Context, start int64, values ...T) (int64, error) {
	encoded, err := encodeArrayValues(values, start)
	if err != nil {
		return 0, err
	}
	if len(encoded) == 0 {
		return 0, nil
	}
	command := array.client.B().Arset().Key(array.key).Index(start).Value(encoded...).Build()
	return array.client.Do(ctx, command).AsInt64()
}

// SetMany 按任意下标批量写入对象，返回原先为空、此次新建的槽位数。
// 为使命令顺序保持稳定，下标会按升序发送；空 map 返回 0 且不访问 Redis。
func (array *Array[T]) SetMany(ctx context.Context, values map[int64]T) (int64, error) {
	if len(values) == 0 {
		return 0, nil
	}
	indices := make([]int64, 0, len(values))
	for index := range values {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(left, right int) bool { return indices[left] < indices[right] })

	firstIndex := indices[0]
	firstValue, err := encodeArrayValue(values[firstIndex], firstIndex)
	if err != nil {
		return 0, err
	}
	command := array.client.B().Armset().Key(array.key).Index(firstIndex).Value(firstValue)
	for _, index := range indices[1:] {
		encoded, err := encodeArrayValue(values[index], index)
		if err != nil {
			return 0, err
		}
		command = command.Index(index).Value(encoded)
	}
	return array.client.Do(ctx, command.Build()).AsInt64()
}

// Get 返回指定下标的对象。
// key 或槽位不存在时返回 Rueidis Nil 错误，其他错误同样返回 T 的零值。
func (array *Array[T]) Get(ctx context.Context, index int64) (T, error) {
	result := array.client.Do(ctx, array.client.B().Arget().Key(array.key).Index(index).Build())
	raw, err := result.ToString()
	if err != nil {
		var zero T
		return zero, err
	}
	return decodeJSON[T](raw)
}

// GetMany 按传入顺序批量读取下标，并通过 Exists 明确区分空槽与 T 的零值。
// 空输入返回已初始化的空切片且不访问 Redis。
func (array *Array[T]) GetMany(ctx context.Context, indices ...int64) ([]ArrayItem[T], error) {
	if len(indices) == 0 {
		return []ArrayItem[T]{}, nil
	}
	command := array.client.B().Armget().Key(array.key).Index(indices...).Build()
	messages, err := array.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	if len(messages) != len(indices) {
		return nil, fmt.Errorf("decode array multi-get: got %d values for %d indices", len(messages), len(indices))
	}
	items := make([]ArrayItem[T], len(messages))
	for position := range messages {
		item, err := decodeArrayItem[T](indices[position], &messages[position])
		if err != nil {
			return nil, fmt.Errorf("decode array index %d: %w", indices[position], err)
		}
		items[position] = item
	}
	return items, nil
}

// Range 返回 start 到 end 的闭区间；start>end 时按下标倒序返回。
// Redis 的空槽不会被丢弃，而是表示为 Exists=false 的 ArrayItem。
func (array *Array[T]) Range(ctx context.Context, start, end int64) ([]ArrayItem[T], error) {
	command := array.client.B().Argetrange().Key(array.key).Start(start).End(end).Build()
	messages, err := array.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	step := int64(1)
	if start > end {
		step = -1
	}
	items := make([]ArrayItem[T], len(messages))
	for position := range messages {
		index := start + int64(position)*step
		item, err := decodeArrayItem[T](index, &messages[position])
		if err != nil {
			return nil, fmt.Errorf("decode array index %d: %w", index, err)
		}
		items[position] = item
	}
	return items, nil
}

// Scan 只返回 start 到 end 闭区间内的非空槽位，start>end 时按下标倒序扫描。
// limit 大于 0 时限制最多返回的槽位数，非正值表示不添加 LIMIT。
func (array *Array[T]) Scan(ctx context.Context, start, end, limit int64) ([]ArrayItem[T], error) {
	builder := array.client.B().Arscan().Key(array.key).Start(start).End(end)
	var result rueidis.RedisResult
	if limit > 0 {
		result = array.client.Do(ctx, builder.Limit(limit).Build())
	} else {
		result = array.client.Do(ctx, builder.Build())
	}
	messages, err := result.ToArray()
	if err != nil {
		return nil, err
	}
	return decodeArrayScanMessages[T](messages)
}

func decodeArrayScanMessages[T any](messages []rueidis.RedisMessage) ([]ArrayItem[T], error) {
	// Redis 8.10 的 RESP3 响应是 [[index,value], ...]；同时兼容早期文档描述的
	// [index,value,...] 扁平响应，避免服务端或协议版本差异泄漏给调用方。
	if len(messages) > 0 && messages[0].IsArray() {
		items := make([]ArrayItem[T], len(messages))
		for position := range messages {
			pair, err := messages[position].ToArray()
			if err != nil {
				return nil, fmt.Errorf("decode array scan pair %d: %w", position, err)
			}
			if len(pair) != 2 {
				return nil, fmt.Errorf("decode array scan pair %d: expected 2 elements, got %d", position, len(pair))
			}
			item, err := decodeArrayScanPair[T](position, &pair[0], &pair[1])
			if err != nil {
				return nil, err
			}
			items[position] = item
		}
		return items, nil
	}
	if len(messages)%2 != 0 {
		return nil, fmt.Errorf("decode array scan: expected index-value pairs, got %d elements", len(messages))
	}
	items := make([]ArrayItem[T], len(messages)/2)
	for position := range items {
		item, err := decodeArrayScanPair[T](position, &messages[position*2], &messages[position*2+1])
		if err != nil {
			return nil, err
		}
		items[position] = item
	}
	return items, nil
}

func decodeArrayScanPair[T any](position int, indexMessage, valueMessage *rueidis.RedisMessage) (ArrayItem[T], error) {
	index, err := indexMessage.AsInt64()
	if err != nil {
		return ArrayItem[T]{}, fmt.Errorf("decode array scan pair %d index: %w", position, err)
	}
	item, err := decodeArrayItem[T](index, valueMessage)
	if err != nil {
		return ArrayItem[T]{}, fmt.Errorf("decode array index %d: %w", index, err)
	}
	if !item.Exists {
		return ArrayItem[T]{}, fmt.Errorf("decode array index %d: scan returned an empty slot", index)
	}
	return item, nil
}

// Insert 从内部插入游标开始连续追加对象，并返回最后写入的下标。
// 可用 Next 查看游标、Seek 调整游标；空输入返回 ErrArrayValuesRequired。
func (array *Array[T]) Insert(ctx context.Context, values ...T) (int64, error) {
	if len(values) == 0 {
		return 0, ErrArrayValuesRequired
	}
	encoded, err := encodeArrayValues(values, 0)
	if err != nil {
		return 0, err
	}
	command := array.client.B().Arinsert().Key(array.key).Value(encoded...).Build()
	return array.client.Do(ctx, command).AsInt64()
}

// Ring 按固定窗口大小循环写入对象，并返回最后写入的下标。
// size 必须为正数，values 不能为空；缩小 size 时 Redis 会截断超出窗口的槽位。
func (array *Array[T]) Ring(ctx context.Context, size int64, values ...T) (int64, error) {
	if size <= 0 {
		return 0, ErrInvalidArrayRingSize
	}
	if len(values) == 0 {
		return 0, ErrArrayValuesRequired
	}
	encoded, err := encodeArrayValues(values, 0)
	if err != nil {
		return 0, err
	}
	command := array.client.B().Arring().Key(array.key).Size(size).Value(encoded...).Build()
	return array.client.Do(ctx, command).AsInt64()
}

// LastItems 返回最近插入的最多 count 个对象。
// reverse=false 按从旧到新的插入顺序返回，reverse=true 按从新到旧返回；count 非正时返回空切片。
func (array *Array[T]) LastItems(ctx context.Context, count int64, reverse bool) ([]T, error) {
	if count <= 0 {
		return []T{}, nil
	}
	builder := array.client.B().Arlastitems().Key(array.key).Count(count)
	var result rueidis.RedisResult
	if reverse {
		result = array.client.Do(ctx, builder.Rev().Build())
	} else {
		result = array.client.Do(ctx, builder.Build())
	}
	messages, err := result.ToArray()
	if err != nil {
		return nil, err
	}
	values := make([]T, len(messages))
	for position := range messages {
		raw, err := messages[position].ToString()
		if err != nil {
			return nil, fmt.Errorf("decode array recent item %d: %w", position, err)
		}
		value, err := decodeJSON[T](raw)
		if err != nil {
			return nil, fmt.Errorf("decode array recent item %d: %w", position, err)
		}
		values[position] = value
	}
	return values, nil
}

// Len 返回逻辑长度，即最高使用下标加一；稀疏数组的 Len 可能大于 Count。
func (array *Array[T]) Len(ctx context.Context) (int64, error) {
	return array.client.Do(ctx, array.client.B().Arlen().Key(array.key).Build()).AsInt64()
}

// Count 返回非空槽位数。
func (array *Array[T]) Count(ctx context.Context) (int64, error) {
	return array.client.Do(ctx, array.client.B().Arcount().Key(array.key).Build()).AsInt64()
}

// Next 返回下一次 Insert/Ring 将使用的下标。
// 插入游标溢出时 Redis 返回 Nil，此方法返回 0 和对应错误。
func (array *Array[T]) Next(ctx context.Context) (int64, error) {
	return array.client.Do(ctx, array.client.B().Arnext().Key(array.key).Build()).AsInt64()
}

// Seek 设置 Insert/Ring 的下一个写入下标，并返回 key 是否存在。
func (array *Array[T]) Seek(ctx context.Context, index int64) (bool, error) {
	return array.client.Do(ctx, array.client.B().Arseek().Key(array.key).Index(index).Build()).AsBool()
}

// Delete 删除指定下标的非空槽位并返回实际删除数；空输入返回 0。
func (array *Array[T]) Delete(ctx context.Context, indices ...int64) (int64, error) {
	if len(indices) == 0 {
		return 0, nil
	}
	command := array.client.B().Ardel().Key(array.key).Index(indices...).Build()
	return array.client.Do(ctx, command).AsInt64()
}

// DeleteRanges 删除一个或多个下标闭区间并返回实际删除数；空输入返回 0。
func (array *Array[T]) DeleteRanges(ctx context.Context, ranges ...ArrayRange) (int64, error) {
	if len(ranges) == 0 {
		return 0, nil
	}
	firstRange := ranges[0]
	command := array.client.B().Ardelrange().Key(array.key).Start(firstRange.Start).End(firstRange.End)
	for _, arrayRange := range ranges[1:] {
		command = command.Start(arrayRange.Start).End(arrayRange.End)
	}
	return array.client.Do(ctx, command.Build()).AsInt64()
}

// Clear 删除整个 Array key。
func (array *Array[T]) Clear(ctx context.Context) error {
	return array.client.Do(ctx, array.client.B().Del().Key(array.key).Build()).Error()
}

func encodeArrayValues[T any](values []T, start int64) ([]string, error) {
	encoded := make([]string, len(values))
	for position, value := range values {
		index := start + int64(position)
		payload, err := encodeArrayValue(value, index)
		if err != nil {
			return nil, err
		}
		encoded[position] = payload
	}
	return encoded, nil
}

func encodeArrayValue[T any](value T, index int64) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("encode array index %d: %w", index, err)
	}
	return string(payload), nil
}

func decodeArrayItem[T any](index int64, message *rueidis.RedisMessage) (ArrayItem[T], error) {
	if message.IsNil() {
		return ArrayItem[T]{Index: index}, nil
	}
	raw, err := message.ToString()
	if err != nil {
		return ArrayItem[T]{}, err
	}
	value, err := decodeJSON[T](raw)
	if err != nil {
		return ArrayItem[T]{}, err
	}
	return ArrayItem[T]{Index: index, Exists: true, Value: value}, nil
}
