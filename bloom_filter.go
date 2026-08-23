package rueidistpl

import (
	"context"
	"errors"
	"strconv"
)

var (
	// ErrInvalidBloomErrorRate 表示 Bloom Filter 误判率不在 (0,1) 范围内。
	ErrInvalidBloomErrorRate = errors.New("rueidistpl: bloom filter error rate must be between 0 and 1")
	// ErrInvalidBloomCapacity 表示 Bloom Filter 预计容量不是正数。
	ErrInvalidBloomCapacity = errors.New("rueidistpl: bloom filter capacity must be positive")
)

// BloomFilter 封装 RedisBloom 的 Bloom Filter 常用操作。
// 它适合空间敏感且允许少量假阳性的存在性判断。
type BloomFilter struct {
	redis *Redis
	key   string
}

// BloomFilter 创建过滤器并自动应用全局 key 前缀。
func (redis *Redis) BloomFilter(key string) *BloomFilter {
	return &BloomFilter{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (filter *BloomFilter) Key() string {
	return filter.key
}

// Reserve 按目标误判率和预计容量显式创建过滤器。
func (filter *BloomFilter) Reserve(ctx context.Context, errorRate float64, capacity int64) error {
	if !isProbability(errorRate) {
		return ErrInvalidBloomErrorRate
	}
	if capacity <= 0 {
		return ErrInvalidBloomCapacity
	}
	args := []string{strconv.FormatFloat(errorRate, 'g', -1, 64), strconv.FormatInt(capacity, 10)}
	command := filter.redis.client.B().Arbitrary("BF.RESERVE").Keys(filter.key).Args(args...).Build()
	return filter.redis.client.Do(ctx, command).Error()
}

// Add 添加元素；至少一个位从 0 变成 1 时返回 true。
// 未 Reserve 时 Redis 会使用默认配置自动创建过滤器。
func (filter *BloomFilter) Add(ctx context.Context, item string) (bool, error) {
	command := filter.redis.client.B().Arbitrary("BF.ADD").Keys(filter.key).Args(item).Build()
	return filter.redis.client.Do(ctx, command).AsBool()
}

// AddMany 批量添加并返回与输入顺序一一对应的变化结果；空输入不发送命令。
func (filter *BloomFilter) AddMany(ctx context.Context, items ...string) ([]bool, error) {
	if len(items) == 0 {
		return []bool{}, nil
	}
	command := filter.redis.client.B().Arbitrary("BF.MADD").Keys(filter.key).Args(items...).Build()
	return filter.redis.client.Do(ctx, command).AsBoolSlice()
}

// Exists 判断元素可能存在时返回 true；true 可能是假阳性，false 一定表示不存在。
func (filter *BloomFilter) Exists(ctx context.Context, item string) (bool, error) {
	command := filter.redis.client.B().Arbitrary("BF.EXISTS").Keys(filter.key).Args(item).ReadOnly()
	return filter.redis.client.Do(ctx, command).AsBool()
}

// ExistsMany 批量判断并保持输入顺序；空输入不发送命令。
func (filter *BloomFilter) ExistsMany(ctx context.Context, items ...string) ([]bool, error) {
	if len(items) == 0 {
		return []bool{}, nil
	}
	command := filter.redis.client.B().Arbitrary("BF.MEXISTS").Keys(filter.key).Args(items...).ReadOnly()
	return filter.redis.client.Do(ctx, command).AsBoolSlice()
}

// Card 返回成功加入过滤器的近似唯一元素数。
func (filter *BloomFilter) Card(ctx context.Context) (int64, error) {
	command := filter.redis.client.B().Arbitrary("BF.CARD").Keys(filter.key).ReadOnly()
	return filter.redis.client.Do(ctx, command).AsInt64()
}

// Clear 删除整个过滤器。
func (filter *BloomFilter) Clear(ctx context.Context) error {
	return filter.redis.client.Do(ctx, filter.redis.client.B().Del().Key(filter.key).Build()).Error()
}

func isProbability(value float64) bool {
	return isFinite(value) && value > 0 && value < 1
}
