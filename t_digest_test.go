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

// TestTDigestLifecycle 验证创建、写样本、分位/CDF/排名、极值、截尾均值、重置和清理。
func TestTDigestLifecycle(t *testing.T) {
	// Arrange：所有浮点参数使用稳定文本格式，批量响应顺序与查询参数一一对应。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	digest := redis.TDigest("latency")
	actualKey := digest.Key()
	if actualKey != "app:latency" {
		t.Fatalf("Key value=%q", actualKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.CREATE", "app:latency", "COMPRESSION", "100")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.ADD", "app:latency", "10", "20", "30")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.QUANTILE", "app:latency", "0.5", "0.9")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisFloat64(20), rueidismock.RedisFloat64(30)))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.CDF", "app:latency", "20", "30")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisFloat64(0.5), rueidismock.RedisFloat64(0.833333)))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.RANK", "app:latency", "20", "30")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1), rueidismock.RedisInt64(2)))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.REVRANK", "app:latency", "20", "30")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1), rueidismock.RedisInt64(0)))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.MIN", "app:latency")).Return(rueidismock.Result(rueidismock.RedisFloat64(10))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.MAX", "app:latency")).Return(rueidismock.Result(rueidismock.RedisFloat64(30))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.TRIMMED_MEAN", "app:latency", "0.1", "0.9")).Return(rueidismock.Result(rueidismock.RedisFloat64(20))),
		client.EXPECT().Do(ctx, rueidismock.Match("TDIGEST.RESET", "app:latency")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:latency")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// Create 明确摘要精度，Add 批量写入三个可观测样本。
	createErr := digest.Create(ctx, 100)
	addErr := digest.Add(ctx, 10, 20, 30)
	if createErr != nil || addErr != nil {
		t.Fatalf("Create=%v Add=%v", createErr, addErr)
	}

	// 分位点和 CDF 返回浮点切片，普通排名和反向排名返回整数切片。
	quantiles, quantileErr := digest.Quantiles(ctx, 0.5, 0.9)
	cdf, cdfErr := digest.CDF(ctx, 20, 30)
	ranks, rankErr := digest.Rank(ctx, 20, 30)
	reverseRanks, reverseRankErr := digest.ReverseRank(ctx, 20, 30)
	if quantileErr != nil || !slices.Equal(quantiles, []float64{20, 30}) || cdfErr != nil || !slices.Equal(cdf, []float64{0.5, 0.833333}) {
		t.Fatalf("Quantiles=%+v/%v CDF=%+v/%v", quantiles, quantileErr, cdf, cdfErr)
	}
	if rankErr != nil || !slices.Equal(ranks, []int64{1, 2}) || reverseRankErr != nil || !slices.Equal(reverseRanks, []int64{1, 0}) {
		t.Fatalf("Rank=%+v/%v ReverseRank=%+v/%v", ranks, rankErr, reverseRanks, reverseRankErr)
	}

	// 极值和截尾平均值直接返回标量；Reset 保留配置，Clear 最终删除 key。
	minimum, minErr := digest.Min(ctx)
	maximum, maxErr := digest.Max(ctx)
	mean, meanErr := digest.TrimmedMean(ctx, 0.1, 0.9)
	resetErr := digest.Reset(ctx)
	clearErr := digest.Clear(ctx)
	if minErr != nil || minimum != 10 || maxErr != nil || maximum != 30 || meanErr != nil || mean != 20 || resetErr != nil || clearErr != nil {
		t.Fatalf("Min=%f/%v Max=%f/%v Mean=%f/%v Reset=%v Clear=%v", minimum, minErr, maximum, maxErr, mean, meanErr, resetErr, clearErr)
	}
}

// TestTDigestValidationAndEmptyInputs 验证压缩、有限数、分位区间和所有空批量分支。
func TestTDigestValidationAndEmptyInputs(t *testing.T) {
	// 无客户端门面用于确认无效输入不会创建 Redis key，也不会触发 nil client。
	ctx := context.Background()
	digest := (&Redis{}).TDigest("latency")
	compressionErr := digest.Create(ctx, 0)
	addErr := digest.Add(ctx, math.NaN())
	badQuantiles, quantileErr := digest.Quantiles(ctx, 1.1)
	badMean, meanErr := digest.TrimmedMean(ctx, 0.9, 0.1)
	if !errors.Is(compressionErr, ErrInvalidTDigestCompression) || !errors.Is(addErr, ErrInvalidTDigestValue) {
		t.Fatalf("Create=%v Add=%v", compressionErr, addErr)
	}
	if badQuantiles != nil || !errors.Is(quantileErr, ErrInvalidTDigestQuantile) || badMean != 0 || !errors.Is(meanErr, ErrInvalidTDigestQuantile) {
		t.Fatalf("Quantiles=%+v/%v Mean=%f/%v", badQuantiles, quantileErr, badMean, meanErr)
	}

	// 空批量是稳定的空切片或 nil error，适合上层动态组装参数后直接调用。
	emptyAddErr := digest.Add(ctx)
	emptyQuantiles, emptyQuantileErr := digest.Quantiles(ctx)
	emptyCDF, emptyCDFErr := digest.CDF(ctx)
	emptyRank, emptyRankErr := digest.Rank(ctx)
	emptyReverseRank, emptyReverseRankErr := digest.ReverseRank(ctx)
	if emptyAddErr != nil || emptyQuantileErr != nil || len(emptyQuantiles) != 0 || emptyCDFErr != nil || len(emptyCDF) != 0 {
		t.Fatalf("Add=%v Quantiles=%+v/%v CDF=%+v/%v", emptyAddErr, emptyQuantiles, emptyQuantileErr, emptyCDF, emptyCDFErr)
	}
	if emptyRankErr != nil || len(emptyRank) != 0 || emptyReverseRankErr != nil || len(emptyReverseRank) != 0 {
		t.Fatalf("Rank=%+v/%v ReverseRank=%+v/%v", emptyRank, emptyRankErr, emptyReverseRank, emptyReverseRankErr)
	}
}
