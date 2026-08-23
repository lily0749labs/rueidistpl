package rueidistpl

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/redis/rueidis"
)

// ErrInvalidScore 表示 ZSet 操作收到了 Redis 不接受的 NaN 分数。
// 正负无穷是 Redis 支持的合法边界，因此不会触发该错误。
var ErrInvalidScore = errors.New("rueidistpl: sorted set score must not be NaN")

// ScoredMember 表示 ZSet 的字符串成员及其分数。
type ScoredMember struct {
	// Member 是稳定的字符串成员。
	Member string
	// Score 用于排序，Redis 按 float64 保存。
	Score float64
}

// SortedSet 是 member 为字符串的通用 Redis ZSet。
type SortedSet struct {
	client rueidis.Client
	key    string
}

// SortedSet 创建通用字符串有序集合。
func (redis *Redis) SortedSet(key string) *SortedSet {
	return &SortedSet{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (set *SortedSet) Key() string {
	return set.key
}

// Add 新增成员或覆盖已有成员的分数。
func (set *SortedSet) Add(ctx context.Context, member string, score float64) error {
	if err := validateRedisScore(score); err != nil {
		return err
	}
	command := set.client.B().Zadd().Key(set.key).ScoreMember().ScoreMember(score, member).Build()
	return set.client.Do(ctx, command).Error()
}

// AddMany 使用一次 ZADD 批量新增或更新成员。
func (set *SortedSet) AddMany(ctx context.Context, members ...ScoredMember) error {
	if len(members) == 0 {
		return nil
	}
	for _, member := range members {
		if err := validateRedisScore(member.Score); err != nil {
			return fmt.Errorf("%w: member %q", err, member.Member)
		}
	}
	command := set.client.B().Zadd().Key(set.key).ScoreMember()
	for _, member := range members {
		command = command.ScoreMember(member.Score, member.Member)
	}
	return set.client.Do(ctx, command.Build()).Error()
}

// Increment 原子增加成员分数，并返回增加后的分数。
func (set *SortedSet) Increment(ctx context.Context, member string, delta float64) (float64, error) {
	if err := validateRedisScore(delta); err != nil {
		return 0, err
	}
	command := set.client.B().Zincrby().Key(set.key).Increment(delta).Member(member).Build()
	return set.client.Do(ctx, command).AsFloat64()
}

// Score 返回成员分数；成员不存在时返回 Redis Nil 错误。
func (set *SortedSet) Score(ctx context.Context, member string) (float64, error) {
	return set.client.Do(ctx, set.client.B().Zscore().Key(set.key).Member(member).Build()).AsFloat64()
}

// Rank 返回成员按分数升序排列的零基排名。
func (set *SortedSet) Rank(ctx context.Context, member string) (int64, error) {
	return set.client.Do(ctx, set.client.B().Zrank().Key(set.key).Member(member).Build()).AsInt64()
}

// ReverseRank 返回成员按分数降序排列的零基排名。
func (set *SortedSet) ReverseRank(ctx context.Context, member string) (int64, error) {
	return set.client.Do(ctx, set.client.B().Zrevrank().Key(set.key).Member(member).Build()).AsInt64()
}

// Range 按分数升序返回排名闭区间内的成员及分数。
func (set *SortedSet) Range(ctx context.Context, start, stop int64) ([]ScoredMember, error) {
	result := set.client.Do(
		ctx,
		set.client.B().Zrange().Key(set.key).
			Min(strconv.FormatInt(start, 10)).Max(strconv.FormatInt(stop, 10)).Withscores().Build(),
	)
	return scoredMembers(result)
}

// ReverseRange 按分数降序返回排名闭区间内的成员及分数。
func (set *SortedSet) ReverseRange(ctx context.Context, start, stop int64) ([]ScoredMember, error) {
	result := set.client.Do(
		ctx,
		set.client.B().Zrevrange().Key(set.key).Start(start).Stop(stop).Withscores().Build(),
	)
	return scoredMembers(result)
}

// RangeByScore 按分数升序查询，min/max 为闭区间；可使用 math.Inf 表示无穷。
func (set *SortedSet) RangeByScore(ctx context.Context, min, max float64, offset, count int64) ([]ScoredMember, error) {
	if err := validateRedisScore(min); err != nil {
		return nil, fmt.Errorf("%w: min", err)
	}
	if err := validateRedisScore(max); err != nil {
		return nil, fmt.Errorf("%w: max", err)
	}
	command := set.client.B().Zrange().Key(set.key).
		Min(redisScore(min)).Max(redisScore(max)).Byscore()
	if count > 0 {
		result := set.client.Do(ctx, command.Limit(offset, count).Withscores().Build())
		return scoredMembers(result)
	}
	return scoredMembers(set.client.Do(ctx, command.Withscores().Build()))
}

// Remove 删除成员并返回实际删除数量。
func (set *SortedSet) Remove(ctx context.Context, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	return set.client.Do(ctx, set.client.B().Zrem().Key(set.key).Member(members...).Build()).AsInt64()
}

// RemoveByScore 删除闭区间分数内的成员，可使用 math.Inf 表示无穷。
func (set *SortedSet) RemoveByScore(ctx context.Context, min, max float64) (int64, error) {
	if err := validateRedisScore(min); err != nil {
		return 0, fmt.Errorf("%w: min", err)
	}
	if err := validateRedisScore(max); err != nil {
		return 0, fmt.Errorf("%w: max", err)
	}
	command := set.client.B().Zremrangebyscore().Key(set.key).Min(redisScore(min)).Max(redisScore(max)).Build()
	return set.client.Do(ctx, command).AsInt64()
}

// PopMin 原子弹出分数最低的 count 个成员。
func (set *SortedSet) PopMin(ctx context.Context, count int64) ([]ScoredMember, error) {
	if count <= 0 {
		return []ScoredMember{}, nil
	}
	return scoredMembers(set.client.Do(ctx, set.client.B().Zpopmin().Key(set.key).Count(count).Build()))
}

// PopMax 原子弹出分数最高的 count 个成员。
func (set *SortedSet) PopMax(ctx context.Context, count int64) ([]ScoredMember, error) {
	if count <= 0 {
		return []ScoredMember{}, nil
	}
	return scoredMembers(set.client.Do(ctx, set.client.B().Zpopmax().Key(set.key).Count(count).Build()))
}

// Len 返回成员数量。
func (set *SortedSet) Len(ctx context.Context) (int64, error) {
	return set.client.Do(ctx, set.client.B().Zcard().Key(set.key).Build()).AsInt64()
}

// Clear 删除整个 ZSet key。
func (set *SortedSet) Clear(ctx context.Context) error {
	return set.client.Do(ctx, set.client.B().Del().Key(set.key).Build()).Error()
}

func scoredMembers(result rueidis.RedisResult) ([]ScoredMember, error) {
	scores, err := result.AsZScores()
	if err != nil {
		return nil, err
	}
	members := make([]ScoredMember, len(scores))
	for index, score := range scores {
		members[index] = ScoredMember{Member: score.Member, Score: score.Score}
	}
	return members, nil
}

func redisScore(score float64) string {
	switch {
	case math.IsInf(score, -1):
		return "-inf"
	case math.IsInf(score, 1):
		return "+inf"
	default:
		return strconv.FormatFloat(score, 'g', -1, 64)
	}
}

func validateRedisScore(score float64) error {
	if math.IsNaN(score) {
		return ErrInvalidScore
	}
	return nil
}
