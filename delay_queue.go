package rueidistpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

// MaxDelayQueuePollBatch 是 Poll 允许的最大单次任务数。
// 该上限同时约束 Lua unpack 参数量、Redis 执行时间和网络响应大小。
const MaxDelayQueuePollBatch int64 = 1000

// ErrDelayQueuePollLimit 表示 Poll 的 limit 超过了安全批次上限。
var ErrDelayQueuePollLimit = errors.New("rueidistpl: delay queue poll limit exceeds maximum batch size")

var delayQueueScheduleScript = rueidis.NewLuaScriptRetryable(`
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
return redis.call("ZADD", KEYS[2], ARGV[3], ARGV[1])`)

// Remove 和 Poll 会删除数据，不能在响应丢失时由客户端自动重试。
// 否则 Remove 的布尔结果会失真，Poll 还可能继续取出并删除下一批任务。
var delayQueueRemoveScript = rueidis.NewLuaScript(`
local removed = redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[1], ARGV[1])
return removed`)

var delayQueuePollScript = rueidis.NewLuaScript(`
local members = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", ARGV[1], "WITHSCORES", "LIMIT", 0, ARGV[2])
local result = {}
local ids = {}
for index = 1, #members, 2 do
  local id = members[index]
  local score = members[index + 1]
  local payload = redis.call("HGET", KEYS[1], id)
  table.insert(ids, id)
  if payload then
    table.insert(result, id)
    table.insert(result, score)
    table.insert(result, payload)
  end
end
if #ids > 0 then
  redis.call("ZREM", KEYS[2], unpack(ids))
  redis.call("HDEL", KEYS[1], unpack(ids))
end
return result`)

// Delayed 表示从延迟队列取出的到期任务。
type Delayed[T any] struct {
	// ID 是调度时指定的稳定任务标识；重复调度相同 ID 会覆盖原任务。
	ID string
	// DueAt 是任务原定执行时间，精度为毫秒。
	DueAt time.Time
	// Value 是反序列化后的任务载荷。
	Value T
}

// DelayQueue 是基于 Hash + ZSet 的至多一次延迟队列。
// Poll 会在返回任务前将其原子移除；需要 ACK/重试语义时应使用 Stream。
type DelayQueue[T any] struct {
	client    rueidis.Client
	valuesKey string
	scoresKey string
}

// NewDelayQueue 创建轻量至多一次延迟队列。
// 内部 Hash 和 ZSet 自动使用相同的 Redis Cluster hash tag。
func NewDelayQueue[T any](redis *Redis, namespace string) *DelayQueue[T] {
	prefix := compositeKeyPrefix(redis, namespace)
	return &DelayQueue[T]{
		client:    redis.client,
		valuesKey: prefix + ":values",
		scoresKey: prefix + ":schedule",
	}
}

// Keys 返回内部载荷 Hash 和调度 ZSet 的真实 key，主要用于监控和清理。
func (queue *DelayQueue[T]) Keys() (valuesKey, scoresKey string) {
	return queue.valuesKey, queue.scoresKey
}

// Schedule 原子保存任务载荷和执行时间；相同 ID 会覆盖之前的任务。
func (queue *DelayQueue[T]) Schedule(ctx context.Context, id string, dueAt time.Time, value T) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return delayQueueScheduleScript.Exec(
		ctx,
		queue.client,
		[]string{queue.valuesKey, queue.scoresKey},
		[]string{id, string(payload), strconv.FormatInt(dueAt.UnixMilli(), 10)},
	).Error()
}

// Poll 原子取出并删除不晚于 now 的任务。
// limit 不得超过 MaxDelayQueuePollBatch；由于是至多一次语义，被取出后即使 JSON 解码失败，任务也已从 Redis 删除。
func (queue *DelayQueue[T]) Poll(ctx context.Context, now time.Time, limit int64) ([]Delayed[T], error) {
	if limit <= 0 {
		return []Delayed[T]{}, nil
	}
	if limit > MaxDelayQueuePollBatch {
		return nil, ErrDelayQueuePollLimit
	}
	values, err := delayQueuePollScript.Exec(
		ctx,
		queue.client,
		[]string{queue.valuesKey, queue.scoresKey},
		[]string{strconv.FormatInt(now.UnixMilli(), 10), strconv.FormatInt(limit, 10)},
	).AsStrSlice()
	if err != nil {
		return nil, err
	}
	if len(values)%3 != 0 {
		return nil, fmt.Errorf("unexpected delay queue response length: %d", len(values))
	}
	items := make([]Delayed[T], len(values)/3)
	for index := range items {
		base := index * 3
		milliseconds, err := strconv.ParseInt(values[base+1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode delayed task %q score: %w", values[base], err)
		}
		value, err := decodeJSON[T](values[base+2])
		if err != nil {
			return nil, fmt.Errorf("decode delayed task %q: %w", values[base], err)
		}
		items[index] = Delayed[T]{
			ID:    values[base],
			DueAt: time.UnixMilli(milliseconds),
			Value: value,
		}
	}
	return items, nil
}

// Remove 原子删除任务载荷和调度记录；任务存在时返回 true。
func (queue *DelayQueue[T]) Remove(ctx context.Context, id string) (bool, error) {
	removed, err := delayQueueRemoveScript.Exec(
		ctx,
		queue.client,
		[]string{queue.valuesKey, queue.scoresKey},
		[]string{id},
	).AsInt64()
	return removed > 0, err
}

// Len 返回当前等待调度的任务数量。
func (queue *DelayQueue[T]) Len(ctx context.Context) (int64, error) {
	return queue.client.Do(ctx, queue.client.B().Zcard().Key(queue.scoresKey).Build()).AsInt64()
}

// Clear 删除队列的载荷 Hash 和调度 ZSet。
func (queue *DelayQueue[T]) Clear(ctx context.Context) error {
	return queue.client.Do(ctx, queue.client.B().Del().Key(queue.valuesKey, queue.scoresKey).Build()).Error()
}
