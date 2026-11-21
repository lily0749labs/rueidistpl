package rueidistpl

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestCountMinSketchLifecycle 验证两种初始化、批量增量、查询和清理。
func TestCountMinSketchLifecycle(t *testing.T) {
	// Arrange：增量使用切片而非 map，确保返回计数能稳定对应输入元素。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	sketch := redis.CountMinSketch("page-views")
	actualKey := sketch.Key()
	if actualKey != "app:page-views" {
		t.Fatalf("Key value=%q", actualKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("CMS.INITBYDIM", "app:page-views", "2000", "5")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("CMS.INITBYPROB", "app:page-views", "0.01", "0.001")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("CMS.INCRBY", "app:page-views", "/home", "3", "/docs", "2")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(3), rueidismock.RedisInt64(2)))),
		client.EXPECT().Do(ctx, rueidismock.Match("CMS.QUERY", "app:page-views", "/home", "/docs")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(3), rueidismock.RedisInt64(2)))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:page-views")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 两种初始化入口分别适合已知内存尺寸和已知误差目标的调用场景。
	dimensionsErr := sketch.InitByDimensions(ctx, 2000, 5)
	probabilityErr := sketch.InitByProbability(ctx, 0.01, 0.001)
	if dimensionsErr != nil || probabilityErr != nil {
		t.Fatalf("InitByDimensions=%v InitByProbability=%v", dimensionsErr, probabilityErr)
	}

	// Increment 返回增加后的估算值，Query 以相同顺序返回当前估算计数。
	counts, incrementErr := sketch.Increment(ctx,
		CountMinIncrement{Item: "/home", Delta: 3},
		CountMinIncrement{Item: "/docs", Delta: 2},
	)
	queried, queryErr := sketch.Query(ctx, "/home", "/docs")
	clearErr := sketch.Clear(ctx)
	if incrementErr != nil || !slices.Equal(counts, []int64{3, 2}) || queryErr != nil || !slices.Equal(queried, []int64{3, 2}) || clearErr != nil {
		t.Fatalf("Increment=%+v/%v Query=%+v/%v Clear=%v", counts, incrementErr, queried, queryErr, clearErr)
	}
}

// TestCountMinSketchValidationAndEmptyInputs 验证尺寸、概率、增量及空输入边界。
func TestCountMinSketchValidationAndEmptyInputs(t *testing.T) {
	// 所有非法参数都必须在构造客户端命令前发现，避免 Redis 返回版本相关错误文本。
	ctx := context.Background()
	sketch := (&Redis{}).CountMinSketch("views")
	dimensionsErr := sketch.InitByDimensions(ctx, 0, 1)
	probabilityErr := sketch.InitByProbability(ctx, 0.01, math.NaN())
	badCounts, incrementErr := sketch.Increment(ctx, CountMinIncrement{Item: "a", Delta: 0})
	emptyCounts, emptyIncrementErr := sketch.Increment(ctx)
	emptyQuery, emptyQueryErr := sketch.Query(ctx)
	if !errors.Is(dimensionsErr, ErrInvalidCountMinDimensions) || !errors.Is(probabilityErr, ErrInvalidCountMinProbability) {
		t.Fatalf("dimensions err=%v probability err=%v", dimensionsErr, probabilityErr)
	}
	if badCounts != nil || !errors.Is(incrementErr, ErrInvalidCountMinIncrement) || emptyIncrementErr != nil || len(emptyCounts) != 0 || emptyQueryErr != nil || len(emptyQuery) != 0 {
		t.Fatalf("bad=%+v/%v empty Increment=%+v/%v Query=%+v/%v", badCounts, incrementErr, emptyCounts, emptyIncrementErr, emptyQuery, emptyQueryErr)
	}
}
