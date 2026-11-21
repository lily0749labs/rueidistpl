package rueidistpl

import (
	"context"
	"errors"
	"math"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestSortedSetLifecycle 覆盖通用 ZSet 的所有正常操作：
// 单个/批量写入、分数递增、分数和正反排名查询、按排名/分数范围查询、删除、弹出和清空；
// 测试同时核对 WITHSCORES、BYSCORE、LIMIT 以及无穷边界的命令顺序。
func TestSortedSetLifecycle(t *testing.T) {
	// Arrange：构造两个带分数成员，并准备 Redis WITHSCORES 的交替数组响应。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	set := redis.SortedSet("scores")
	scoreRows := redisStrings("alice", "25", "bob", "20")

	if set.Key() != "app:scores" {
		t.Fatalf("unexpected key %q", set.Key())
	}

	// 预期命令覆盖 ZSet 全部便利入口，重点核对范围参数和 WITHSCORES 位置。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("ZADD", "app:scores", "10", "alice")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZADD", "app:scores", "10", "alice", "20", "bob")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZINCRBY", "app:scores", "15", "alice")).Return(rueidismock.Result(rueidismock.RedisBlobString("25"))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZSCORE", "app:scores", "alice")).Return(rueidismock.Result(rueidismock.RedisBlobString("25"))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZRANK", "app:scores", "bob")).Return(rueidismock.Result(rueidismock.RedisInt64(0))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZREVRANK", "app:scores", "alice")).Return(rueidismock.Result(rueidismock.RedisInt64(0))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZRANGE", "app:scores", "0", "-1", "WITHSCORES")).Return(rueidismock.Result(scoreRows)),
		client.EXPECT().Do(ctx, rueidismock.Match("ZREVRANGE", "app:scores", "0", "-1", "WITHSCORES")).Return(rueidismock.Result(scoreRows)),
		client.EXPECT().Do(ctx, rueidismock.Match("ZRANGE", "app:scores", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "2", "WITHSCORES")).Return(rueidismock.Result(scoreRows)),
		client.EXPECT().Do(ctx, rueidismock.Match("ZRANGE", "app:scores", "0", "30", "BYSCORE", "WITHSCORES")).Return(rueidismock.Result(scoreRows)),
		client.EXPECT().Do(ctx, rueidismock.Match("ZREM", "app:scores", "alice", "bob")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZREMRANGEBYSCORE", "app:scores", "0", "10")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZPOPMIN", "app:scores", "1")).Return(rueidismock.Result(redisStrings("bob", "20"))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZPOPMAX", "app:scores", "1")).Return(rueidismock.Result(redisStrings("alice", "25"))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZCARD", "app:scores")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:scores")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 写入和基本查询：验证新增、批量新增、分数递增、分数和正反排名。
	addErr := set.Add(ctx, "alice", 10)
	if addErr != nil {
		t.Fatal(addErr)
	}
	addManyErr := set.AddMany(ctx, ScoredMember{Member: "alice", Score: 10}, ScoredMember{Member: "bob", Score: 20})
	if addManyErr != nil {
		t.Fatal(addManyErr)
	}
	incrementScore, incrementErr := set.Increment(ctx, "alice", 15)
	if incrementErr != nil || incrementScore != 25 {
		t.Fatalf("Increment score=%f err=%v", incrementScore, incrementErr)
	}
	score, scoreErr := set.Score(ctx, "alice")
	if scoreErr != nil || score != 25 {
		t.Fatalf("Score value=%f err=%v", score, scoreErr)
	}
	rank, rankErr := set.Rank(ctx, "bob")
	if rankErr != nil || rank != 0 {
		t.Fatalf("Rank value=%d err=%v", rank, rankErr)
	}
	reverseRank, reverseRankErr := set.ReverseRank(ctx, "alice")
	if reverseRankErr != nil || reverseRank != 0 {
		t.Fatalf("ReverseRank value=%d err=%v", reverseRank, reverseRankErr)
	}
	// 使用有序测试表执行四类范围查询，避免 map 遍历打乱 gomock.InOrder。
	queries := []struct {
		name string
		run  func() ([]ScoredMember, error)
	}{
		{name: "range", run: func() ([]ScoredMember, error) { return set.Range(ctx, 0, -1) }},
		{name: "reverse range", run: func() ([]ScoredMember, error) { return set.ReverseRange(ctx, 0, -1) }},
		{name: "score limit", run: func() ([]ScoredMember, error) { return set.RangeByScore(ctx, math.Inf(-1), math.Inf(1), 1, 2) }},
		{name: "score range", run: func() ([]ScoredMember, error) { return set.RangeByScore(ctx, 0, 30, 0, 0) }},
	}
	for _, query := range queries {
		values, err := query.run()
		if err != nil || len(values) != 2 || values[0] != (ScoredMember{Member: "alice", Score: 25}) {
			t.Fatalf("%s values=%+v err=%v", query.name, values, err)
		}
	}
	// 变更与清理：验证按成员/分数删除、两端弹出、计数和整个 key 删除。
	removedCount, removeErr := set.Remove(ctx, "alice", "bob")
	if removeErr != nil || removedCount != 2 {
		t.Fatalf("Remove value=%d err=%v", removedCount, removeErr)
	}
	removedByScore, removeByScoreErr := set.RemoveByScore(ctx, 0, 10)
	if removeByScoreErr != nil || removedByScore != 1 {
		t.Fatalf("RemoveByScore value=%d err=%v", removedByScore, removeByScoreErr)
	}
	minValues, popMinErr := set.PopMin(ctx, 1)
	if popMinErr != nil || len(minValues) != 1 || minValues[0].Member != "bob" {
		t.Fatalf("PopMin values=%+v err=%v", minValues, popMinErr)
	}
	maxValues, popMaxErr := set.PopMax(ctx, 1)
	if popMaxErr != nil || len(maxValues) != 1 || maxValues[0].Member != "alice" {
		t.Fatalf("PopMax values=%+v err=%v", maxValues, popMaxErr)
	}
	setLength, lenErr := set.Len(ctx)
	if lenErr != nil || setLength != 2 {
		t.Fatalf("Len value=%d err=%v", setLength, lenErr)
	}
	clearErr := set.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestSortedSetEmptyOperations 验证空批量写入、空删除和非正弹出数量直接返回空结果，
// 不会构造 Redis 不接受的空成员命令。
func TestSortedSetEmptyOperations(t *testing.T) {
	// 不提供客户端，证明四种空操作都在本地直接返回。
	set := &SortedSet{}
	ctx := context.Background()
	addManyErr := set.AddMany(ctx)
	if addManyErr != nil {
		t.Fatal(addManyErr)
	}
	removedCount, removeErr := set.Remove(ctx)
	if removeErr != nil || removedCount != 0 {
		t.Fatalf("Remove value=%d err=%v", removedCount, removeErr)
	}
	minValues, popMinErr := set.PopMin(ctx, 0)
	if popMinErr != nil || len(minValues) != 0 {
		t.Fatalf("PopMin values=%+v err=%v", minValues, popMinErr)
	}
	maxValues, popMaxErr := set.PopMax(ctx, -1)
	if popMaxErr != nil || len(maxValues) != 0 {
		t.Fatalf("PopMax values=%+v err=%v", maxValues, popMaxErr)
	}
}

// TestSortedSetRejectsNaN 验证所有接收分数的便利入口都会在本地拒绝 NaN，
// 防止生成 Redis 不接受的命令；正负无穷仍由正常范围测试覆盖并保持合法。
func TestSortedSetRejectsNaN(t *testing.T) {
	// Arrange：不配置客户端，任何漏掉的参数校验都会因尝试构造或发送命令而使测试失败。
	set := &SortedSet{}
	ctx := context.Background()
	nan := math.NaN()

	// Act/Assert：单写、批量写和增量写都返回同一个可由 errors.Is 判断的错误。
	if err := set.Add(ctx, "alice", nan); !errors.Is(err, ErrInvalidScore) {
		t.Fatalf("Add error=%v", err)
	}
	if err := set.AddMany(ctx, ScoredMember{Member: "alice", Score: 1}, ScoredMember{Member: "bad", Score: nan}); !errors.Is(err, ErrInvalidScore) {
		t.Fatalf("AddMany error=%v", err)
	}
	if _, err := set.Increment(ctx, "alice", nan); !errors.Is(err, ErrInvalidScore) {
		t.Fatalf("Increment error=%v", err)
	}
	// 两类分数范围 API 的 min/max 任一为 NaN 都必须在访问 Redis 前失败。
	for _, bounds := range [][2]float64{{nan, 1}, {0, nan}} {
		if _, err := set.RangeByScore(ctx, bounds[0], bounds[1], 0, 0); !errors.Is(err, ErrInvalidScore) {
			t.Fatalf("RangeByScore(%v) error=%v", bounds, err)
		}
		if _, err := set.RemoveByScore(ctx, bounds[0], bounds[1]); !errors.Is(err, ErrInvalidScore) {
			t.Fatalf("RemoveByScore(%v) error=%v", bounds, err)
		}
	}
}
