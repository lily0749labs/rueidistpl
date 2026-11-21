package rueidistpl

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestTimeSeriesLifecycle 验证创建、写样本、区间查询、增减、区间删除和清理。
func TestTimeSeriesLifecycle(t *testing.T) {
	// Arrange：标签 map 必须按字典序输出，避免测试和实际请求随 map 遍历顺序波动。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	series := redis.TimeSeries("temperature")
	actualKey := series.Key()
	if actualKey != "app:temperature" {
		t.Fatalf("Key value=%q", actualKey)
	}
	firstSample := rueidismock.RedisArray(rueidismock.RedisInt64(1000), rueidismock.RedisBlobString("21.5"))
	secondSample := rueidismock.RedisArray(rueidismock.RedisInt64(2000), rueidismock.RedisBlobString("22.25"))
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("TS.CREATE", "app:temperature", "RETENTION", "60000", "LABELS", "city", "shanghai", "sensor", "s1")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.ADD", "app:temperature", "1000", "21.5")).Return(rueidismock.Result(rueidismock.RedisInt64(1000))),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.ADD", "app:temperature", "*", "22.25")).Return(rueidismock.Result(rueidismock.RedisInt64(2000))),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.GET", "app:temperature")).Return(rueidismock.Result(secondSample)),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.RANGE", "app:temperature", "1000", "2000", "COUNT", "2")).Return(rueidismock.Result(rueidismock.RedisArray(firstSample, secondSample))),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.REVRANGE", "app:temperature", "1000", "2000")).Return(rueidismock.Result(rueidismock.RedisArray(secondSample, firstSample))),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.RANGE", "app:temperature", "-", "+", "COUNT", "1")).Return(rueidismock.Result(rueidismock.RedisArray(firstSample))),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.INCRBY", "app:temperature", "0.5")).Return(rueidismock.Result(rueidismock.RedisInt64(3000))),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.DECRBY", "app:temperature", "0.25")).Return(rueidismock.Result(rueidismock.RedisInt64(4000))),
		client.EXPECT().Do(ctx, rueidismock.Match("TS.DEL", "app:temperature", "1000", "1500")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:temperature")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 创建配置显式保留 60 秒，两个 Add 入口分别使用业务时间戳和 Redis 当前时间。
	createErr := series.Create(ctx, TimeSeriesOptions{Retention: time.Minute, Labels: map[string]string{"sensor": "s1", "city": "shanghai"}})
	addedTimestamp, addErr := series.Add(ctx, 1000, 21.5)
	nowTimestamp, addNowErr := series.AddNow(ctx, 22.25)
	if createErr != nil || addErr != nil || addedTimestamp != 1000 || addNowErr != nil || nowTimestamp != 2000 {
		t.Fatalf("Create=%v Add=%d/%v AddNow=%d/%v", createErr, addedTimestamp, addErr, nowTimestamp, addNowErr)
	}

	// 最新样本和三种范围入口均返回强类型切片，断点中可直接查看时间戳与浮点值。
	latest, latestErr := series.Get(ctx)
	ascending, rangeErr := series.Range(ctx, 1000, 2000, 2)
	descending, reverseErr := series.ReverseRange(ctx, 1000, 2000, 0)
	firstOnly, allErr := series.RangeAll(ctx, 1)
	wantAscending := []TimeSeriesSample{{Timestamp: 1000, Value: 21.5}, {Timestamp: 2000, Value: 22.25}}
	wantDescending := []TimeSeriesSample{{Timestamp: 2000, Value: 22.25}, {Timestamp: 1000, Value: 21.5}}
	if latestErr != nil || latest != wantAscending[1] || rangeErr != nil || !slices.Equal(ascending, wantAscending) {
		t.Fatalf("Get=%+v/%v Range=%+v/%v", latest, latestErr, ascending, rangeErr)
	}
	if reverseErr != nil || !slices.Equal(descending, wantDescending) || allErr != nil || !slices.Equal(firstOnly, wantAscending[:1]) {
		t.Fatalf("Reverse=%+v/%v All=%+v/%v", descending, reverseErr, firstOnly, allErr)
	}

	// 原子增减返回新建样本的时间戳，删除返回样本数，Clear 删除整个序列。
	incrementTimestamp, incrementErr := series.Increment(ctx, 0.5)
	decrementTimestamp, decrementErr := series.Decrement(ctx, 0.25)
	deleted, deleteErr := series.DeleteRange(ctx, 1000, 1500)
	clearErr := series.Clear(ctx)
	if incrementErr != nil || incrementTimestamp != 3000 || decrementErr != nil || decrementTimestamp != 4000 || deleteErr != nil || deleted != 1 || clearErr != nil {
		t.Fatalf("Increment=%d/%v Decrement=%d/%v Delete=%d/%v Clear=%v", incrementTimestamp, incrementErr, decrementTimestamp, decrementErr, deleted, deleteErr, clearErr)
	}
}

// TestTimeSeriesValidationAndDecodeErrors 验证非法参数、空最新值及畸形样本响应。
func TestTimeSeriesValidationAndDecodeErrors(t *testing.T) {
	// 非法保留时间和非有限浮点值必须本地失败，防止不同 Redis 小版本产生不同结果。
	ctx := context.Background()
	emptySeries := (&Redis{}).TimeSeries("temperature")
	createErr := emptySeries.Create(ctx, TimeSeriesOptions{Retention: -time.Millisecond})
	nanTimestamp, nanErr := emptySeries.Add(ctx, 1, math.NaN())
	infTimestamp, infErr := emptySeries.Increment(ctx, math.Inf(1))
	if !errors.Is(createErr, ErrInvalidTimeSeriesRetention) || nanTimestamp != 0 || !errors.Is(nanErr, ErrInvalidTimeSeriesValue) || infTimestamp != 0 || !errors.Is(infErr, ErrInvalidTimeSeriesValue) {
		t.Fatalf("Create=%v Add=%d/%v Increment=%d/%v", createErr, nanTimestamp, nanErr, infTimestamp, infErr)
	}

	// 空数组代表没有最新样本；长度错误和数值错误必须返回零值及可见错误。
	redis, client := newMockRedis(t)
	series := redis.TimeSeries("temperature")
	command := rueidismock.Match("TS.GET", "app:temperature")
	gomock.InOrder(
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray())),
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1)))),
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("bad"), rueidismock.RedisBlobString("1")))),
	)
	missing, missingErr := series.Get(ctx)
	badLength, badLengthErr := series.Get(ctx)
	badTimestamp, badTimestampErr := series.Get(ctx)
	if missing != (TimeSeriesSample{}) || !errors.Is(missingErr, rueidis.Nil) {
		t.Fatalf("missing=%+v err=%v", missing, missingErr)
	}
	if badLength != (TimeSeriesSample{}) || badLengthErr == nil || badTimestamp != (TimeSeriesSample{}) || badTimestampErr == nil {
		t.Fatalf("badLength=%+v/%v badTimestamp=%+v/%v", badLength, badLengthErr, badTimestamp, badTimestampErr)
	}
}
