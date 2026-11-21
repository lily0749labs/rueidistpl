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

// MaxDelayQueuePollBatch 是 Poll 和 Claim 允许的最大单次任务数。
// 该上限同时约束 Lua unpack 参数量、Redis 执行时间和网络响应大小。
const MaxDelayQueuePollBatch int64 = 1000

// ErrDelayQueuePollLimit 表示 Poll 或 Claim 的 limit 超过了安全批次上限。
var ErrDelayQueuePollLimit = errors.New("rueidistpl: delay queue poll limit exceeds maximum batch size")

var delayQueueScheduleScript = rueidis.NewLuaScriptRetryable(`
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("HDEL", KEYS[4], ARGV[1])
return redis.call("ZADD", KEYS[2], ARGV[3], ARGV[1])`)

// Remove 和 Poll 会删除数据，不能在响应丢失时由客户端自动重试。
// 否则 Remove 的布尔结果会失真，Poll 还可能继续取出并删除下一批任务。
var delayQueueRemoveScript = rueidis.NewLuaScript(`
local removed = redis.call("ZREM", KEYS[2], ARGV[1])
removed = removed + redis.call("ZREM", KEYS[3], ARGV[1])
redis.call("HDEL", KEYS[1], ARGV[1])
redis.call("HDEL", KEYS[4], ARGV[1])
if removed > 0 then return 1 end
return 0`)

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

// Claim 会移动任务并生成新的所有权令牌，响应丢失时不能自动重试，否则可能额外领取一批任务。
var delayQueueClaimScript = rueidis.NewLuaScript(`
local redisTime = redis.call("TIME")
local now = redisTime[1] * 1000 + math.floor(redisTime[2] / 1000)
local limit = tonumber(ARGV[1])
local leaseUntil = now + tonumber(ARGV[2])

local expired = redis.call("ZRANGEBYSCORE", KEYS[3], "-inf", now, "LIMIT", 0, limit)
for _, id in ipairs(expired) do
  if redis.call("ZREM", KEYS[3], id) == 1 then
    redis.call("HDEL", KEYS[4], id)
    if redis.call("HEXISTS", KEYS[1], id) == 1 then
      redis.call("ZADD", KEYS[2], now, id)
    end
  end
end

local members = redis.call("ZRANGEBYSCORE", KEYS[2], "-inf", now, "WITHSCORES", "LIMIT", 0, limit)
local result = {}
local item = 0
for index = 1, #members, 2 do
  local id = members[index]
  local score = members[index + 1]
  local payload = redis.call("HGET", KEYS[1], id)
  redis.call("ZREM", KEYS[2], id)
  if payload then
    item = item + 1
    local token = ARGV[3] .. ":" .. item
    redis.call("ZADD", KEYS[3], leaseUntil, id)
    redis.call("HSET", KEYS[4], id, token)
    table.insert(result, id)
    table.insert(result, score)
    table.insert(result, payload)
    table.insert(result, token)
  end
end
return result`)

var delayQueueAckScript = rueidis.NewLuaScript(`
if redis.call("HGET", KEYS[3], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
redis.call("HDEL", KEYS[1], ARGV[1])
return 1`)

var delayQueueNackScript = rueidis.NewLuaScript(`
if redis.call("HGET", KEYS[3], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[3], ARGV[1])
redis.call("ZADD", KEYS[1], ARGV[3], ARGV[1])
return 1`)

var delayQueueExtendScript = rueidis.NewLuaScript(`
if redis.call("HGET", KEYS[2], ARGV[1]) ~= ARGV[2] then
  return 0
end
local currentDeadline = redis.call("ZSCORE", KEYS[1], ARGV[1])
if not currentDeadline then
  return 0
end
local redisTime = redis.call("TIME")
local now = redisTime[1] * 1000 + math.floor(redisTime[2] / 1000)
if tonumber(currentDeadline) <= now then
  return 0
end
redis.call("ZADD", KEYS[1], "XX", now + tonumber(ARGV[3]), ARGV[1])
return 1`)

// Delayed 表示从延迟队列取出的到期任务。
type Delayed[T any] struct {
	// ID 是调度时指定的稳定任务标识；重复调度相同 ID 会覆盖原任务。
	ID string
	// DueAt 是任务原定执行时间，精度为毫秒。
	DueAt time.Time
	// Value 是反序列化后的任务载荷。
	Value T
}

// ClaimedDelayed 是通过 Claim 取得、等待 ACK 或 NACK 的任务。
// Token 是本次领取的所有权凭证；旧 worker 的 token 不能确认后来重新投递的同一任务。
type ClaimedDelayed[T any] struct {
	Delayed[T]
	Token string
}

// DelayQueue 是基于 Hash + ZSet 的延迟队列。
// Claim 提供可见性超时与 ACK/NACK；兼容入口 Poll 仍是取出即删除的至多一次语义。
type DelayQueue[T any] struct {
	client        rueidis.Client
	valuesKey     string
	scoresKey     string
	processingKey string
	claimsKey     string
}

// NewDelayQueue 创建延迟队列；内部 key 自动使用相同的 Redis Cluster hash tag。
func NewDelayQueue[T any](redis *Redis, namespace string) *DelayQueue[T] {
	prefix := compositeKeyPrefix(redis, namespace)
	return &DelayQueue[T]{
		client:        redis.client,
		valuesKey:     prefix + ":values",
		scoresKey:     prefix + ":schedule",
		processingKey: prefix + ":processing",
		claimsKey:     prefix + ":claims",
	}
}

// Keys 返回内部载荷 Hash 和调度 ZSet 的真实 key，主要用于监控和清理。
func (queue *DelayQueue[T]) Keys() (valuesKey, scoresKey string) {
	return queue.valuesKey, queue.scoresKey
}

// ReliableKeys 返回可靠消费额外使用的处理中 ZSet 和所有权 Hash。
func (queue *DelayQueue[T]) ReliableKeys() (processingKey, claimsKey string) {
	return queue.processingKey, queue.claimsKey
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
		[]string{queue.valuesKey, queue.scoresKey, queue.processingKey, queue.claimsKey},
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

// Claim 使用 Redis 服务端时间领取到期任务，并设置 visibilityTimeout。
// worker 崩溃后，租约到期的任务会在后续 Claim 中自动重新投递；成功必须调用 Ack，失败可调用 Nack。
// 网络错误意味着领取结果不确定，调用方应等待租约到期，不能立即无条件重试同一批业务副作用。
func (queue *DelayQueue[T]) Claim(ctx context.Context, visibilityTimeout time.Duration, limit int64) ([]ClaimedDelayed[T], error) {
	if visibilityTimeout <= 0 {
		return nil, ErrInvalidExpiration
	}
	if limit <= 0 {
		return []ClaimedDelayed[T]{}, nil
	}
	if limit > MaxDelayQueuePollBatch {
		return nil, ErrDelayQueuePollLimit
	}
	visibilityTimeout = atLeastMillisecond(visibilityTimeout)
	token, err := newOwnershipToken()
	if err != nil {
		return nil, err
	}
	values, err := delayQueueClaimScript.Exec(
		ctx,
		queue.client,
		[]string{queue.valuesKey, queue.scoresKey, queue.processingKey, queue.claimsKey},
		[]string{
			strconv.FormatInt(limit, 10),
			strconv.FormatInt(visibilityTimeout.Milliseconds(), 10),
			token,
		},
	).AsStrSlice()
	if err != nil {
		return nil, err
	}
	if len(values)%4 != 0 {
		return nil, fmt.Errorf("unexpected claimed delay queue response length: %d", len(values))
	}
	items := make([]ClaimedDelayed[T], len(values)/4)
	for index := range items {
		base := index * 4
		milliseconds, err := strconv.ParseInt(values[base+1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode claimed delayed task %q score: %w", values[base], err)
		}
		value, err := decodeJSON[T](values[base+2])
		if err != nil {
			return nil, fmt.Errorf("decode claimed delayed task %q: %w", values[base], err)
		}
		items[index] = ClaimedDelayed[T]{
			Delayed: Delayed[T]{ID: values[base], DueAt: time.UnixMilli(milliseconds), Value: value},
			Token:   values[base+3],
		}
	}
	return items, nil
}

// Ack 按所有权 token 确认任务完成，并永久删除载荷；token 已失效时返回 false。
func (queue *DelayQueue[T]) Ack(ctx context.Context, id, token string) (bool, error) {
	acked, err := delayQueueAckScript.Exec(
		ctx,
		queue.client,
		[]string{queue.valuesKey, queue.processingKey, queue.claimsKey},
		[]string{id, token},
	).AsInt64()
	return acked == 1, err
}

// Nack 按所有权 token 将任务重新调度到 dueAt；token 已失效时返回 false。
func (queue *DelayQueue[T]) Nack(ctx context.Context, id, token string, dueAt time.Time) (bool, error) {
	requeued, err := delayQueueNackScript.Exec(
		ctx,
		queue.client,
		[]string{queue.scoresKey, queue.processingKey, queue.claimsKey},
		[]string{id, token, strconv.FormatInt(dueAt.UnixMilli(), 10)},
	).AsInt64()
	return requeued == 1, err
}

// Extend 按所有权 token 延长尚未到期的处理租约；租约已失效时返回 false。
func (queue *DelayQueue[T]) Extend(ctx context.Context, id, token string, visibilityTimeout time.Duration) (bool, error) {
	if visibilityTimeout <= 0 {
		return false, ErrInvalidExpiration
	}
	visibilityTimeout = atLeastMillisecond(visibilityTimeout)
	extended, err := delayQueueExtendScript.Exec(
		ctx,
		queue.client,
		[]string{queue.processingKey, queue.claimsKey},
		[]string{id, token, strconv.FormatInt(visibilityTimeout.Milliseconds(), 10)},
	).AsInt64()
	return extended == 1, err
}

// Remove 原子删除任务载荷和调度记录；任务存在时返回 true。
func (queue *DelayQueue[T]) Remove(ctx context.Context, id string) (bool, error) {
	removed, err := delayQueueRemoveScript.Exec(
		ctx,
		queue.client,
		[]string{queue.valuesKey, queue.scoresKey, queue.processingKey, queue.claimsKey},
		[]string{id},
	).AsInt64()
	return removed > 0, err
}

// InFlight 返回正在处理、尚未 ACK/NACK 的任务数量。
func (queue *DelayQueue[T]) InFlight(ctx context.Context) (int64, error) {
	return queue.client.Do(ctx, queue.client.B().Zcard().Key(queue.processingKey).Build()).AsInt64()
}

// Len 返回当前等待调度的任务数量。
func (queue *DelayQueue[T]) Len(ctx context.Context) (int64, error) {
	return queue.client.Do(ctx, queue.client.B().Zcard().Key(queue.scoresKey).Build()).AsInt64()
}

// Clear 删除队列的载荷、调度、处理中任务和所有权记录。
func (queue *DelayQueue[T]) Clear(ctx context.Context) error {
	return queue.client.Do(ctx, queue.client.B().Del().Key(
		queue.valuesKey,
		queue.scoresKey,
		queue.processingKey,
		queue.claimsKey,
	).Build()).Error()
}
