package rueidistpl

import (
	"context"
	"errors"
	"strconv"
)

var (
	// ErrInvalidBitOffset 表示位偏移为负数。
	ErrInvalidBitOffset = errors.New("rueidistpl: bit offset must not be negative")
	// ErrInvalidBitmapOperation 表示 BITOP 操作不是 AND、OR、XOR 或 NOT。
	ErrInvalidBitmapOperation = errors.New("rueidistpl: invalid bitmap operation")
)

// BitmapOperation 表示 BITOP 支持的经典位运算。
type BitmapOperation string

const (
	BitmapAnd BitmapOperation = "AND"
	BitmapOr  BitmapOperation = "OR"
	BitmapXor BitmapOperation = "XOR"
	BitmapNot BitmapOperation = "NOT"
)

// Bitmap 使用 Redis String 的位操作保存布尔状态集合。
type Bitmap struct {
	redis *Redis
	key   string
}

// Bitmap 创建位图并自动应用全局 key 前缀。
func (redis *Redis) Bitmap(key string) *Bitmap {
	return &Bitmap{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (bitmap *Bitmap) Key() string {
	return bitmap.key
}

// Set 设置 offset 对应的位并返回修改前的值。
func (bitmap *Bitmap) Set(ctx context.Context, offset int64, value bool) (bool, error) {
	if offset < 0 {
		return false, ErrInvalidBitOffset
	}
	bit := int64(0)
	if value {
		bit = 1
	}
	command := bitmap.redis.client.B().Setbit().Key(bitmap.key).Offset(offset).Value(bit).Build()
	return bitmap.redis.client.Do(ctx, command).AsBool()
}

// Get 返回 offset 对应的位。
func (bitmap *Bitmap) Get(ctx context.Context, offset int64) (bool, error) {
	if offset < 0 {
		return false, ErrInvalidBitOffset
	}
	command := bitmap.redis.client.B().Getbit().Key(bitmap.key).Offset(offset).Build()
	return bitmap.redis.client.Do(ctx, command).AsBool()
}

// Count 返回整个位图中值为 1 的位数。
func (bitmap *Bitmap) Count(ctx context.Context) (int64, error) {
	command := bitmap.redis.client.B().Bitcount().Key(bitmap.key).Build()
	return bitmap.redis.client.Do(ctx, command).AsInt64()
}

// CountBits 返回位下标闭区间内值为 1 的位数。
func (bitmap *Bitmap) CountBits(ctx context.Context, start, end int64) (int64, error) {
	command := bitmap.redis.client.B().Bitcount().Key(bitmap.key).Start(start).End(end).Bit().Build()
	return bitmap.redis.client.Do(ctx, command).AsInt64()
}

// Position 返回整个位图中第一个等于 value 的位下标；未找到时返回 -1。
func (bitmap *Bitmap) Position(ctx context.Context, value bool) (int64, error) {
	bit := int64(0)
	if value {
		bit = 1
	}
	command := bitmap.redis.client.B().Bitpos().Key(bitmap.key).Bit(bit).Build()
	return bitmap.redis.client.Do(ctx, command).AsInt64()
}

// PositionBits 返回位下标闭区间内第一个等于 value 的位下标；未找到时返回 -1。
func (bitmap *Bitmap) PositionBits(ctx context.Context, value bool, start, end int64) (int64, error) {
	bit := int64(0)
	if value {
		bit = 1
	}
	command := bitmap.redis.client.B().Bitpos().Key(bitmap.key).Bit(bit).Start(start).End(end).Bit().Build()
	return bitmap.redis.client.Do(ctx, command).AsInt64()
}

// Combine 将当前位图和 additionalSources 做位运算，结果写入逻辑 destination key，并返回结果字节数。
// operation 支持 AND、OR、XOR 和 NOT；NOT 只能使用当前位图，不能提供 additionalSources。
// Redis Cluster 要求 destination、当前 key 和所有 source 位于同一 slot。
func (bitmap *Bitmap) Combine(ctx context.Context, operation BitmapOperation, destination string, additionalSources ...string) (int64, error) {
	if operation != BitmapAnd && operation != BitmapOr && operation != BitmapXor && operation != BitmapNot {
		return 0, ErrInvalidBitmapOperation
	}
	if operation == BitmapNot && len(additionalSources) != 0 {
		return 0, ErrInvalidBitmapOperation
	}
	destinationKey := bitmap.redis.Key(destination)
	keys := make([]string, 0, len(additionalSources)+2)
	keys = append(keys, destinationKey, bitmap.key)
	for _, source := range additionalSources {
		keys = append(keys, bitmap.redis.Key(source))
	}
	command := bitmap.redis.client.B().Arbitrary("BITOP", string(operation)).Keys(keys...).Build()
	return bitmap.redis.client.Do(ctx, command).AsInt64()
}

// Clear 删除整个位图 key。
func (bitmap *Bitmap) Clear(ctx context.Context) error {
	return bitmap.redis.client.Do(ctx, bitmap.redis.client.B().Del().Key(bitmap.key).Build()).Error()
}

// BitFieldGet 将 String 中的一段位按 Redis BITFIELD 编码读取为整数。
// encoding 使用 Redis 原生格式，例如 i8、u16；复杂的批量和溢出策略应直接使用 Rueidis Builder。
func (bitmap *Bitmap) BitFieldGet(ctx context.Context, encoding string, offset int64) (int64, error) {
	command := bitmap.redis.client.B().BitfieldRo().Key(bitmap.key).Get().Get(encoding, offset).Build()
	values, err := bitmap.redis.client.Do(ctx, command).AsIntSlice()
	if err != nil {
		return 0, err
	}
	if len(values) != 1 {
		return 0, errors.New("rueidistpl: unexpected BITFIELD_RO response length " + strconv.Itoa(len(values)))
	}
	return values[0], nil
}
