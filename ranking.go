package rueidistpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/rueidis"
)

// ErrRankingValueMissing 表示 ZSet 中存在排行榜 ID，但对应 Hash 对象已经缺失。
var ErrRankingValueMissing = errors.New("rueidistpl: ranking member has no object value")

// ErrRankingScoreMissing 表示 Hash 中存在排行榜对象，但对应 ZSet 分数已经缺失。
var ErrRankingScoreMissing = errors.New("rueidistpl: ranking object has no score")

var rankingUpsertScript = rueidis.NewLuaScriptRetryable(`
redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])
return redis.call("ZADD", KEYS[2], ARGV[3], ARGV[1])`)

// Remove 重复执行虽然最终状态一致，但第二次会把返回值从 1 变成 0，因此禁止自动重试。
var rankingRemoveScript = rueidis.NewLuaScript(`
local removed = redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("HDEL", KEYS[1], ARGV[1])
return removed`)

// Get 和 Range 在同一次 Lua 中读取 Hash 与 ZSet，避免并发更新或删除时拼出跨时刻结果。
// 脚本只读且返回值稳定，因此允许 Rueidis 在网络异常时安全重试。
var rankingGetScript = rueidis.NewLuaScriptRetryable(`
local payload = redis.call("HGET", KEYS[1], ARGV[1])
local score = redis.call("ZSCORE", KEYS[2], ARGV[1])
return {payload or false, score or false}`)

var rankingRangeScript = rueidis.NewLuaScriptRetryable(`
local members = redis.call("ZREVRANGE", KEYS[2], ARGV[1], ARGV[2], "WITHSCORES")
local result = {}
for index = 1, #members, 2 do
  local id = members[index]
  local payload = redis.call("HGET", KEYS[1], id)
  table.insert(result, id)
  table.insert(result, members[index + 1])
  table.insert(result, payload or false)
end
return result`)

// Ranked 是包含业务 ID、分数和对象内容的排行榜条目。
type Ranked[T any] struct {
	// ID 是稳定业务标识，同时作为 ZSet member 和 Hash field。
	ID string
	// Score 是用于倒序排名的分数。
	Score float64
	// Value 是与 ID 对应的业务对象。
	Value T
}

// Ranking 使用 Hash 保存对象、ZSet 保存稳定 ID 与分数。
// 两个键使用同一 Redis Cluster hash tag，Upsert 和 Remove 由 Lua 原子执行。
type Ranking[T any] struct {
	client    rueidis.Client
	values    *Hash[T]
	scoresKey string
}

// NewRanking 创建对象排行榜。内部两个 key 自动使用相同的 Cluster hash tag。
func NewRanking[T any](redis *Redis, namespace string) *Ranking[T] {
	keyPrefix := compositeKeyPrefix(redis, namespace)
	return &Ranking[T]{
		client:    redis.client,
		values:    &Hash[T]{client: redis.client, key: keyPrefix + ":values"},
		scoresKey: keyPrefix + ":scores",
	}
}

// Keys 返回内部对象 Hash 和分数 ZSet 的真实 key，主要用于监控和清理。
func (ranking *Ranking[T]) Keys() (valuesKey, scoresKey string) {
	return ranking.values.Key(), ranking.scoresKey
}

// Upsert 原子新增或更新对象及其分数。
func (ranking *Ranking[T]) Upsert(ctx context.Context, id string, score float64, value T) error {
	if err := validateRedisScore(score); err != nil {
		return fmt.Errorf("%w: member %q", err, id)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return rankingUpsertScript.Exec(
		ctx,
		ranking.client,
		[]string{ranking.values.Key(), ranking.scoresKey},
		[]string{id, string(payload), strconv.FormatFloat(score, 'g', -1, 64)},
	).Error()
}

// Get 原子读取指定 ID 的对象和当前分数；ID 完全不存在时返回 Redis Nil 错误。
func (ranking *Ranking[T]) Get(ctx context.Context, id string) (Ranked[T], error) {
	values, err := rankingGetScript.Exec(
		ctx,
		ranking.client,
		[]string{ranking.values.Key(), ranking.scoresKey},
		[]string{id},
	).ToArray()
	if err != nil {
		return Ranked[T]{}, err
	}
	if len(values) != 2 {
		return Ranked[T]{}, fmt.Errorf("unexpected ranking get response length: got %d want 2", len(values))
	}
	payload, payloadErr := values[0].ToString()
	score, scoreErr := values[1].AsFloat64()
	if rueidis.IsRedisNil(payloadErr) && rueidis.IsRedisNil(scoreErr) {
		return Ranked[T]{}, rueidis.Nil
	}
	if rueidis.IsRedisNil(payloadErr) {
		return Ranked[T]{}, fmt.Errorf("%w: %s", ErrRankingValueMissing, id)
	}
	if payloadErr != nil {
		return Ranked[T]{}, payloadErr
	}
	if rueidis.IsRedisNil(scoreErr) {
		return Ranked[T]{}, fmt.Errorf("%w: %s", ErrRankingScoreMissing, id)
	}
	if scoreErr != nil {
		return Ranked[T]{}, scoreErr
	}
	value, err := decodeJSON[T](payload)
	if err != nil {
		return Ranked[T]{}, fmt.Errorf("decode ranking member %q: %w", id, err)
	}
	return Ranked[T]{ID: id, Score: score, Value: value}, nil
}

// Top 按分数降序返回前 limit 项；limit 非正时返回空切片。
func (ranking *Ranking[T]) Top(ctx context.Context, limit int64) ([]Ranked[T], error) {
	if limit <= 0 {
		return []Ranked[T]{}, nil
	}
	return ranking.Range(ctx, 0, limit-1)
}

// Range 按分数降序返回零基排名闭区间内的对象。
func (ranking *Ranking[T]) Range(ctx context.Context, start, stop int64) ([]Ranked[T], error) {
	values, err := rankingRangeScript.Exec(
		ctx,
		ranking.client,
		[]string{ranking.values.Key(), ranking.scoresKey},
		[]string{strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10)},
	).ToArray()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return []Ranked[T]{}, nil
	}
	if len(values)%3 != 0 {
		return nil, fmt.Errorf("unexpected ranking range response length: %d", len(values))
	}
	items := make([]Ranked[T], len(values)/3)
	for index := range items {
		base := index * 3
		id, err := values[base].ToString()
		if err != nil {
			return nil, fmt.Errorf("decode ranking member %d id: %w", index, err)
		}
		score, err := values[base+1].AsFloat64()
		if err != nil {
			return nil, fmt.Errorf("decode ranking member %q score: %w", id, err)
		}
		payload, err := values[base+2].ToString()
		if rueidis.IsRedisNil(err) {
			return nil, fmt.Errorf("%w: %s", ErrRankingValueMissing, id)
		}
		if err != nil {
			return nil, fmt.Errorf("decode ranking member %q payload: %w", id, err)
		}
		value, err := decodeJSON[T](payload)
		if err != nil {
			return nil, fmt.Errorf("decode ranking member %q: %w", id, err)
		}
		items[index] = Ranked[T]{ID: id, Score: score, Value: value}
	}
	return items, nil
}

// Remove 原子删除对象和分数，实际删除排行榜成员时返回 true。
func (ranking *Ranking[T]) Remove(ctx context.Context, id string) (bool, error) {
	removed, err := rankingRemoveScript.Exec(
		ctx,
		ranking.client,
		[]string{ranking.values.Key(), ranking.scoresKey},
		[]string{id},
	).AsInt64()
	return removed > 0, err
}

// Len 返回排行榜成员数量。
func (ranking *Ranking[T]) Len(ctx context.Context) (int64, error) {
	return ranking.client.Do(ctx, ranking.client.B().Zcard().Key(ranking.scoresKey).Build()).AsInt64()
}

// Clear 删除内部 Hash 和 ZSet。
func (ranking *Ranking[T]) Clear(ctx context.Context) error {
	_, err := ranking.client.Do(
		ctx,
		ranking.client.B().Del().Key(ranking.values.Key(), ranking.scoresKey).Build(),
	).AsInt64()
	return err
}
