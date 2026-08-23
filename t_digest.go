package rueidistpl

import (
	"context"
	"errors"
	"strconv"
)

var (
	// ErrInvalidTDigestCompression 表示压缩参数不是正数。
	ErrInvalidTDigestCompression = errors.New("rueidistpl: t-digest compression must be positive")
	// ErrInvalidTDigestValue 表示输入样本或查询值不是有限数。
	ErrInvalidTDigestValue = errors.New("rueidistpl: t-digest value must be finite")
	// ErrInvalidTDigestQuantile 表示分位点不在 [0,1] 范围内。
	ErrInvalidTDigestQuantile = errors.New("rueidistpl: t-digest quantile must be between 0 and 1")
)

// TDigest 封装 RedisBloom 的 t-digest 分布统计操作。
type TDigest struct {
	redis *Redis
	key   string
}

// TDigest 创建摘要并自动应用全局 key 前缀。
func (redis *Redis) TDigest(key string) *TDigest {
	return &TDigest{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (digest *TDigest) Key() string {
	return digest.key
}

// Create 按压缩参数创建摘要；压缩值越高通常精度和内存占用越高。
func (digest *TDigest) Create(ctx context.Context, compression int64) error {
	if compression <= 0 {
		return ErrInvalidTDigestCompression
	}
	command := digest.redis.client.B().Arbitrary("TDIGEST.CREATE").Keys(digest.key).
		Args("COMPRESSION", strconv.FormatInt(compression, 10)).Build()
	return digest.redis.client.Do(ctx, command).Error()
}

// Add 批量加入样本；空输入不发送命令。
func (digest *TDigest) Add(ctx context.Context, values ...float64) error {
	args, err := finiteFloatArguments(values)
	if err != nil {
		return err
	}
	if len(args) == 0 {
		return nil
	}
	command := digest.redis.client.B().Arbitrary("TDIGEST.ADD").Keys(digest.key).Args(args...).Build()
	return digest.redis.client.Do(ctx, command).Error()
}

// Quantiles 批量查询分位点对应值，并保持输入顺序；空输入不发送命令。
func (digest *TDigest) Quantiles(ctx context.Context, quantiles ...float64) ([]float64, error) {
	if len(quantiles) == 0 {
		return []float64{}, nil
	}
	args := make([]string, len(quantiles))
	for index, quantile := range quantiles {
		if !isFinite(quantile) || quantile < 0 || quantile > 1 {
			return nil, ErrInvalidTDigestQuantile
		}
		args[index] = strconv.FormatFloat(quantile, 'g', -1, 64)
	}
	command := digest.redis.client.B().Arbitrary("TDIGEST.QUANTILE").Keys(digest.key).Args(args...).ReadOnly()
	return digest.redis.client.Do(ctx, command).AsFloatSlice()
}

// CDF 返回每个值在估算累积分布中的比例。
func (digest *TDigest) CDF(ctx context.Context, values ...float64) ([]float64, error) {
	return digest.floatQuery(ctx, "TDIGEST.CDF", values)
}

// Rank 返回小于各值的估算样本数量。
func (digest *TDigest) Rank(ctx context.Context, values ...float64) ([]int64, error) {
	return digest.integerQuery(ctx, "TDIGEST.RANK", values)
}

// ReverseRank 返回大于各值的估算样本数量。
func (digest *TDigest) ReverseRank(ctx context.Context, values ...float64) ([]int64, error) {
	return digest.integerQuery(ctx, "TDIGEST.REVRANK", values)
}

func (digest *TDigest) floatQuery(ctx context.Context, commandName string, values []float64) ([]float64, error) {
	args, err := finiteFloatArguments(values)
	if err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return []float64{}, nil
	}
	command := digest.redis.client.B().Arbitrary(commandName).Keys(digest.key).Args(args...).ReadOnly()
	return digest.redis.client.Do(ctx, command).AsFloatSlice()
}

func (digest *TDigest) integerQuery(ctx context.Context, commandName string, values []float64) ([]int64, error) {
	args, err := finiteFloatArguments(values)
	if err != nil {
		return nil, err
	}
	if len(args) == 0 {
		return []int64{}, nil
	}
	command := digest.redis.client.B().Arbitrary(commandName).Keys(digest.key).Args(args...).ReadOnly()
	return digest.redis.client.Do(ctx, command).AsIntSlice()
}

// Min 返回摘要中的最小样本。
func (digest *TDigest) Min(ctx context.Context) (float64, error) {
	command := digest.redis.client.B().Arbitrary("TDIGEST.MIN").Keys(digest.key).ReadOnly()
	return digest.redis.client.Do(ctx, command).AsFloat64()
}

// Max 返回摘要中的最大样本。
func (digest *TDigest) Max(ctx context.Context) (float64, error) {
	command := digest.redis.client.B().Arbitrary("TDIGEST.MAX").Keys(digest.key).ReadOnly()
	return digest.redis.client.Do(ctx, command).AsFloat64()
}

// TrimmedMean 返回 lowerQuantile 到 upperQuantile 区间内的截尾平均值。
func (digest *TDigest) TrimmedMean(ctx context.Context, lowerQuantile, upperQuantile float64) (float64, error) {
	if !isFinite(lowerQuantile) || !isFinite(upperQuantile) || lowerQuantile < 0 || upperQuantile > 1 || lowerQuantile > upperQuantile {
		return 0, ErrInvalidTDigestQuantile
	}
	args := []string{
		strconv.FormatFloat(lowerQuantile, 'g', -1, 64),
		strconv.FormatFloat(upperQuantile, 'g', -1, 64),
	}
	command := digest.redis.client.B().Arbitrary("TDIGEST.TRIMMED_MEAN").Keys(digest.key).Args(args...).ReadOnly()
	return digest.redis.client.Do(ctx, command).AsFloat64()
}

// Reset 清空摘要中的样本但保留摘要配置。
func (digest *TDigest) Reset(ctx context.Context) error {
	command := digest.redis.client.B().Arbitrary("TDIGEST.RESET").Keys(digest.key).Build()
	return digest.redis.client.Do(ctx, command).Error()
}

// Clear 删除整个 t-digest key。
func (digest *TDigest) Clear(ctx context.Context) error {
	return digest.redis.client.Do(ctx, digest.redis.client.B().Del().Key(digest.key).Build()).Error()
}

func finiteFloatArguments(values []float64) ([]string, error) {
	args := make([]string, len(values))
	for index, value := range values {
		if !isFinite(value) {
			return nil, ErrInvalidTDigestValue
		}
		args[index] = strconv.FormatFloat(value, 'g', -1, 64)
	}
	return args, nil
}
