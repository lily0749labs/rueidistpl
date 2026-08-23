package rueidistpl

import (
	"context"
	"errors"
	"fmt"
	"strconv"
)

var (
	// ErrInvalidTopK 表示跟踪的 Top-K 数量不是正数。
	ErrInvalidTopK = errors.New("rueidistpl: top-k size must be positive")
	// ErrInvalidTopKOptions 表示 width、depth 或 decay 配置无效。
	ErrInvalidTopKOptions = errors.New("rueidistpl: invalid top-k options")
)

// TopKOptions 配置 Top-K 草图的精度和衰减。
type TopKOptions struct {
	Width int64
	Depth int64
	Decay float64
}

// TopKAddResult 表示一次添加及可能被 Top-K 列表挤出的元素。
type TopKAddResult struct {
	Item      string
	Kicked    string
	HasKicked bool
}

// TopKItem 是一个高频元素及其估算计数。
type TopKItem struct {
	Item  string
	Count int64
}

// TopK 封装 RedisBloom 的 Top-K 高频元素追踪操作。
type TopK struct {
	redis *Redis
	key   string
}

// TopK 创建草图并自动应用全局 key 前缀。
func (redis *Redis) TopK(key string) *TopK {
	return &TopK{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (top *TopK) Key() string {
	return top.key
}

// Reserve 使用 Redis 默认精度参数初始化 Top-K 草图。
func (top *TopK) Reserve(ctx context.Context, size int64) error {
	if size <= 0 {
		return ErrInvalidTopK
	}
	command := top.redis.client.B().Arbitrary("TOPK.RESERVE").Keys(top.key).
		Args(strconv.FormatInt(size, 10)).Build()
	return top.redis.client.Do(ctx, command).Error()
}

// ReserveWithOptions 使用完整精度参数初始化 Top-K 草图。
func (top *TopK) ReserveWithOptions(ctx context.Context, size int64, options TopKOptions) error {
	if size <= 0 {
		return ErrInvalidTopK
	}
	if options.Width <= 0 || options.Depth <= 0 || !isFinite(options.Decay) || options.Decay <= 0 || options.Decay >= 1 {
		return ErrInvalidTopKOptions
	}
	args := []string{
		strconv.FormatInt(size, 10),
		strconv.FormatInt(options.Width, 10),
		strconv.FormatInt(options.Depth, 10),
		strconv.FormatFloat(options.Decay, 'g', -1, 64),
	}
	command := top.redis.client.B().Arbitrary("TOPK.RESERVE").Keys(top.key).Args(args...).Build()
	return top.redis.client.Do(ctx, command).Error()
}

// Add 批量添加元素，并逐项说明是否有旧元素被挤出；空输入不发送命令。
func (top *TopK) Add(ctx context.Context, items ...string) ([]TopKAddResult, error) {
	if len(items) == 0 {
		return []TopKAddResult{}, nil
	}
	command := top.redis.client.B().Arbitrary("TOPK.ADD").Keys(top.key).Args(items...).Build()
	messages, err := top.redis.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	if len(messages) != len(items) {
		return nil, fmt.Errorf("unexpected TOPK.ADD response length: got %d want %d", len(messages), len(items))
	}
	results := make([]TopKAddResult, len(items))
	for index, message := range messages {
		results[index].Item = items[index]
		if message.IsNil() {
			continue
		}
		kicked, kickedErr := message.ToString()
		if kickedErr != nil {
			return nil, fmt.Errorf("decode top-k add result %d: %w", index, kickedErr)
		}
		results[index].Kicked = kicked
		results[index].HasKicked = true
	}
	return results, nil
}

// Query 批量判断元素当前是否属于 Top-K；空输入不发送命令。
func (top *TopK) Query(ctx context.Context, items ...string) ([]bool, error) {
	if len(items) == 0 {
		return []bool{}, nil
	}
	command := top.redis.client.B().Arbitrary("TOPK.QUERY").Keys(top.key).Args(items...).ReadOnly()
	return top.redis.client.Do(ctx, command).AsBoolSlice()
}

// Count 批量返回元素的估算计数并保持输入顺序；空输入不发送命令。
func (top *TopK) Count(ctx context.Context, items ...string) ([]int64, error) {
	if len(items) == 0 {
		return []int64{}, nil
	}
	command := top.redis.client.B().Arbitrary("TOPK.COUNT").Keys(top.key).Args(items...).ReadOnly()
	return top.redis.client.Do(ctx, command).AsIntSlice()
}

// List 返回当前 Top-K 元素及估算计数，顺序由 Redis 保持。
func (top *TopK) List(ctx context.Context) ([]TopKItem, error) {
	command := top.redis.client.B().Arbitrary("TOPK.LIST").Keys(top.key).Args("WITHCOUNT").ReadOnly()
	messages, err := top.redis.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	if len(messages)%2 != 0 {
		return nil, fmt.Errorf("unexpected TOPK.LIST response length: %d", len(messages))
	}
	items := make([]TopKItem, 0, len(messages)/2)
	for index := 0; index < len(messages); index += 2 {
		item, itemErr := messages[index].ToString()
		if itemErr != nil {
			return nil, fmt.Errorf("decode top-k item %d: %w", index/2, itemErr)
		}
		count, countErr := messages[index+1].AsInt64()
		if countErr != nil {
			return nil, fmt.Errorf("decode top-k count %d: %w", index/2, countErr)
		}
		items = append(items, TopKItem{Item: item, Count: count})
	}
	return items, nil
}

// Clear 删除整个 Top-K 草图。
func (top *TopK) Clear(ctx context.Context) error {
	return top.redis.client.Do(ctx, top.redis.client.B().Del().Key(top.key).Build()).Error()
}
