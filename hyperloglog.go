package rueidistpl

import (
	"context"
	"errors"
)

// ErrHyperLogLogSourcesRequired 表示 PFMERGE 没有提供任何来源 key。
var ErrHyperLogLogSourcesRequired = errors.New("rueidistpl: hyperloglog merge requires at least one source")

// HyperLogLog 是 Redis 近似去重计数结构。
type HyperLogLog struct {
	redis *Redis
	key   string
}

// HyperLogLog 创建近似去重计数器并自动应用全局 key 前缀。
func (redis *Redis) HyperLogLog(key string) *HyperLogLog {
	return &HyperLogLog{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (hyperLogLog *HyperLogLog) Key() string {
	return hyperLogLog.key
}

// Add 添加元素并返回内部估算是否发生变化；空输入返回 false。
func (hyperLogLog *HyperLogLog) Add(ctx context.Context, elements ...string) (bool, error) {
	if len(elements) == 0 {
		return false, nil
	}
	command := hyperLogLog.redis.client.B().Pfadd().Key(hyperLogLog.key).Element(elements...).Build()
	return hyperLogLog.redis.client.Do(ctx, command).AsBool()
}

// Count 返回当前 HyperLogLog 的近似基数。
func (hyperLogLog *HyperLogLog) Count(ctx context.Context) (int64, error) {
	command := hyperLogLog.redis.client.B().Pfcount().Key(hyperLogLog.key).Build()
	return hyperLogLog.redis.client.Do(ctx, command).AsInt64()
}

// CountWith 返回当前 HyperLogLog 与其他逻辑 key 合并后的近似基数，但不写入结果。
// Redis Cluster 要求所有 key 位于同一 slot。
func (hyperLogLog *HyperLogLog) CountWith(ctx context.Context, others ...string) (int64, error) {
	keys := make([]string, 0, len(others)+1)
	keys = append(keys, hyperLogLog.key)
	for _, other := range others {
		keys = append(keys, hyperLogLog.redis.Key(other))
	}
	command := hyperLogLog.redis.client.B().Pfcount().Key(keys...).Build()
	return hyperLogLog.redis.client.Do(ctx, command).AsInt64()
}

// MergeFrom 将一个或多个逻辑来源 key 合并到当前 HyperLogLog。
// Redis Cluster 要求目标和来源 key 位于同一 slot。
func (hyperLogLog *HyperLogLog) MergeFrom(ctx context.Context, sources ...string) error {
	if len(sources) == 0 {
		return ErrHyperLogLogSourcesRequired
	}
	sourceKeys := make([]string, len(sources))
	for index, source := range sources {
		sourceKeys[index] = hyperLogLog.redis.Key(source)
	}
	command := hyperLogLog.redis.client.B().Pfmerge().Destkey(hyperLogLog.key).Sourcekey(sourceKeys...).Build()
	return hyperLogLog.redis.client.Do(ctx, command).Error()
}

// Clear 删除整个 HyperLogLog key。
func (hyperLogLog *HyperLogLog) Clear(ctx context.Context) error {
	return hyperLogLog.redis.client.Do(ctx, hyperLogLog.redis.client.B().Del().Key(hyperLogLog.key).Build()).Error()
}
