package rueidistpl

import (
	"context"
	"errors"
	"strconv"
)

// ErrInvalidCuckooCapacity 表示 Cuckoo Filter 预计容量不是正数。
var ErrInvalidCuckooCapacity = errors.New("rueidistpl: cuckoo filter capacity must be positive")

// CuckooFilter 封装 RedisBloom 的 Cuckoo Filter 常用操作。
// 相比 Bloom Filter，它支持删除元素和统计同一元素的插入次数。
type CuckooFilter struct {
	redis *Redis
	key   string
}

// CuckooFilter 创建过滤器并自动应用全局 key 前缀。
func (redis *Redis) CuckooFilter(key string) *CuckooFilter {
	return &CuckooFilter{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (filter *CuckooFilter) Key() string {
	return filter.key
}

// Reserve 按预计容量显式创建过滤器。
func (filter *CuckooFilter) Reserve(ctx context.Context, capacity int64) error {
	if capacity <= 0 {
		return ErrInvalidCuckooCapacity
	}
	command := filter.redis.client.B().Arbitrary("CF.RESERVE").Keys(filter.key).
		Args(strconv.FormatInt(capacity, 10)).Build()
	return filter.redis.client.Do(ctx, command).Error()
}

// Add 添加元素；未 Reserve 时 Redis 会使用默认配置自动创建过滤器。
func (filter *CuckooFilter) Add(ctx context.Context, item string) (bool, error) {
	command := filter.redis.client.B().Arbitrary("CF.ADD").Keys(filter.key).Args(item).Build()
	return filter.redis.client.Do(ctx, command).AsBool()
}

// AddNX 仅当元素被判断为不存在时添加；由于概率结构可能假阳性，false 不保证元素确实已插入过。
func (filter *CuckooFilter) AddNX(ctx context.Context, item string) (bool, error) {
	command := filter.redis.client.B().Arbitrary("CF.ADDNX").Keys(filter.key).Args(item).Build()
	return filter.redis.client.Do(ctx, command).AsBool()
}

// Exists 判断元素可能存在时返回 true。
func (filter *CuckooFilter) Exists(ctx context.Context, item string) (bool, error) {
	command := filter.redis.client.B().Arbitrary("CF.EXISTS").Keys(filter.key).Args(item).ReadOnly()
	return filter.redis.client.Do(ctx, command).AsBool()
}

// ExistsMany 批量判断并保持输入顺序；空输入不发送命令。
func (filter *CuckooFilter) ExistsMany(ctx context.Context, items ...string) ([]bool, error) {
	if len(items) == 0 {
		return []bool{}, nil
	}
	command := filter.redis.client.B().Arbitrary("CF.MEXISTS").Keys(filter.key).Args(items...).ReadOnly()
	return filter.redis.client.Do(ctx, command).AsBoolSlice()
}

// Delete 删除一次元素指纹，实际删除时返回 true。
func (filter *CuckooFilter) Delete(ctx context.Context, item string) (bool, error) {
	command := filter.redis.client.B().Arbitrary("CF.DEL").Keys(filter.key).Args(item).Build()
	return filter.redis.client.Do(ctx, command).AsBool()
}

// Count 返回元素大约被添加了多少次。
func (filter *CuckooFilter) Count(ctx context.Context, item string) (int64, error) {
	command := filter.redis.client.B().Arbitrary("CF.COUNT").Keys(filter.key).Args(item).ReadOnly()
	return filter.redis.client.Do(ctx, command).AsInt64()
}

// Clear 删除整个过滤器。
func (filter *CuckooFilter) Clear(ctx context.Context) error {
	return filter.redis.client.Do(ctx, filter.redis.client.B().Del().Key(filter.key).Build()).Error()
}
