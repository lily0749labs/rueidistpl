package rueidistpl

import (
	"context"
	"errors"
	"strconv"
)

var (
	// ErrInvalidCountMinDimensions 表示宽度或深度不是正数。
	ErrInvalidCountMinDimensions = errors.New("rueidistpl: count-min sketch width and depth must be positive")
	// ErrInvalidCountMinProbability 表示误差率或失败概率不在 (0,1) 范围内。
	ErrInvalidCountMinProbability = errors.New("rueidistpl: count-min sketch error rate and probability must be between 0 and 1")
	// ErrInvalidCountMinIncrement 表示增量不是正数。
	ErrInvalidCountMinIncrement = errors.New("rueidistpl: count-min sketch increment must be positive")
)

// CountMinIncrement 是一个元素及其正整数增量。
type CountMinIncrement struct {
	Item  string
	Delta int64
}

// CountMinSketch 封装 RedisBloom 的 Count-Min Sketch 高频计数操作。
type CountMinSketch struct {
	redis *Redis
	key   string
}

// CountMinSketch 创建草图并自动应用全局 key 前缀。
func (redis *Redis) CountMinSketch(key string) *CountMinSketch {
	return &CountMinSketch{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (sketch *CountMinSketch) Key() string {
	return sketch.key
}

// InitByDimensions 按宽度和深度显式初始化草图。
func (sketch *CountMinSketch) InitByDimensions(ctx context.Context, width, depth int64) error {
	if width <= 0 || depth <= 0 {
		return ErrInvalidCountMinDimensions
	}
	args := []string{strconv.FormatInt(width, 10), strconv.FormatInt(depth, 10)}
	command := sketch.redis.client.B().Arbitrary("CMS.INITBYDIM").Keys(sketch.key).Args(args...).Build()
	return sketch.redis.client.Do(ctx, command).Error()
}

// InitByProbability 按误差率和失败概率显式初始化草图。
func (sketch *CountMinSketch) InitByProbability(ctx context.Context, errorRate, probability float64) error {
	if !isProbability(errorRate) || !isProbability(probability) {
		return ErrInvalidCountMinProbability
	}
	args := []string{
		strconv.FormatFloat(errorRate, 'g', -1, 64),
		strconv.FormatFloat(probability, 'g', -1, 64),
	}
	command := sketch.redis.client.B().Arbitrary("CMS.INITBYPROB").Keys(sketch.key).Args(args...).Build()
	return sketch.redis.client.Do(ctx, command).Error()
}

// Increment 批量增加元素计数，并返回与输入顺序对应的新估算值；空输入不发送命令。
func (sketch *CountMinSketch) Increment(ctx context.Context, increments ...CountMinIncrement) ([]int64, error) {
	if len(increments) == 0 {
		return []int64{}, nil
	}
	args := make([]string, 0, len(increments)*2)
	for _, increment := range increments {
		if increment.Delta <= 0 {
			return nil, ErrInvalidCountMinIncrement
		}
		args = append(args, increment.Item, strconv.FormatInt(increment.Delta, 10))
	}
	command := sketch.redis.client.B().Arbitrary("CMS.INCRBY").Keys(sketch.key).Args(args...).Build()
	return sketch.redis.client.Do(ctx, command).AsIntSlice()
}

// Query 批量查询估算计数并保持输入顺序；空输入不发送命令。
func (sketch *CountMinSketch) Query(ctx context.Context, items ...string) ([]int64, error) {
	if len(items) == 0 {
		return []int64{}, nil
	}
	command := sketch.redis.client.B().Arbitrary("CMS.QUERY").Keys(sketch.key).Args(items...).ReadOnly()
	return sketch.redis.client.Do(ctx, command).AsIntSlice()
}

// Clear 删除整个草图。
func (sketch *CountMinSketch) Clear(ctx context.Context) error {
	return sketch.redis.client.Do(ctx, sketch.redis.client.B().Del().Key(sketch.key).Build()).Error()
}
