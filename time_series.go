package rueidistpl

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

var (
	// ErrInvalidTimeSeriesRetention 表示保留时间不能为负数。
	ErrInvalidTimeSeriesRetention = errors.New("rueidistpl: time series retention must not be negative")
	// ErrInvalidTimeSeriesValue 表示样本值是 NaN 或无穷大。
	ErrInvalidTimeSeriesValue = errors.New("rueidistpl: time series value must be finite")
)

// TimeSeriesSample 是一个毫秒时间戳及其浮点样本值。
type TimeSeriesSample struct {
	Timestamp int64
	Value     float64
}

// TimeSeriesOptions 配置时间序列的保留时间和标签。
type TimeSeriesOptions struct {
	// Retention 为零表示永久保留；负数无效。
	Retention time.Duration
	// Labels 用于查询和聚合；写入命令会按名称排序以保证稳定。
	Labels map[string]string
}

// TimeSeries 封装 RedisTimeSeries 的常用单序列操作。
// Redis 8 已内置 RedisTimeSeries；旧版 Redis 需要单独安装模块。
type TimeSeries struct {
	redis *Redis
	key   string
}

// TimeSeries 创建时间序列并自动应用全局 key 前缀。
func (redis *Redis) TimeSeries(key string) *TimeSeries {
	return &TimeSeries{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (series *TimeSeries) Key() string {
	return series.key
}

// Create 显式创建时间序列。直接调用 Add 时 Redis 也可以按默认配置自动创建。
func (series *TimeSeries) Create(ctx context.Context, options TimeSeriesOptions) error {
	if options.Retention < 0 {
		return ErrInvalidTimeSeriesRetention
	}
	args := make([]string, 0, 3+len(options.Labels)*2)
	if options.Retention > 0 {
		retention := atLeastMillisecond(options.Retention).Milliseconds()
		args = append(args, "RETENTION", strconv.FormatInt(retention, 10))
	}
	if len(options.Labels) != 0 {
		labels := make([]string, 0, len(options.Labels))
		for label := range options.Labels {
			labels = append(labels, label)
		}
		sort.Strings(labels)
		args = append(args, "LABELS")
		for _, label := range labels {
			args = append(args, label, options.Labels[label])
		}
	}
	command := series.redis.client.B().Arbitrary("TS.CREATE").Keys(series.key).Args(args...).Build()
	return series.redis.client.Do(ctx, command).Error()
}

// Add 写入指定毫秒时间戳的样本，并返回 Redis 最终使用的时间戳。
func (series *TimeSeries) Add(ctx context.Context, timestamp int64, value float64) (int64, error) {
	return series.add(ctx, strconv.FormatInt(timestamp, 10), value)
}

// AddNow 由 Redis 生成当前毫秒时间戳，并返回最终时间戳。
func (series *TimeSeries) AddNow(ctx context.Context, value float64) (int64, error) {
	return series.add(ctx, "*", value)
}

func (series *TimeSeries) add(ctx context.Context, timestamp string, value float64) (int64, error) {
	if !isFinite(value) {
		return 0, ErrInvalidTimeSeriesValue
	}
	args := []string{timestamp, strconv.FormatFloat(value, 'g', -1, 64)}
	command := series.redis.client.B().Arbitrary("TS.ADD").Keys(series.key).Args(args...).Build()
	return series.redis.client.Do(ctx, command).AsInt64()
}

// Get 返回时间戳最大的最新样本；序列不存在或没有样本时返回 Redis Nil。
func (series *TimeSeries) Get(ctx context.Context) (TimeSeriesSample, error) {
	command := series.redis.client.B().Arbitrary("TS.GET").Keys(series.key).ReadOnly()
	message, err := series.redis.client.Do(ctx, command).ToArray()
	if err != nil {
		return TimeSeriesSample{}, err
	}
	if len(message) == 0 {
		return TimeSeriesSample{}, rueidis.Nil
	}
	return decodeTimeSeriesSample(message)
}

// Range 返回毫秒时间戳闭区间内按时间升序排列的样本；count 非正时不限制数量。
func (series *TimeSeries) Range(ctx context.Context, start, end int64, count int64) ([]TimeSeriesSample, error) {
	return series.rangeSamples(ctx, "TS.RANGE", strconv.FormatInt(start, 10), strconv.FormatInt(end, 10), count)
}

// ReverseRange 返回毫秒时间戳闭区间内按时间降序排列的样本；count 非正时不限制数量。
func (series *TimeSeries) ReverseRange(ctx context.Context, start, end int64, count int64) ([]TimeSeriesSample, error) {
	return series.rangeSamples(ctx, "TS.REVRANGE", strconv.FormatInt(start, 10), strconv.FormatInt(end, 10), count)
}

// RangeAll 返回整个时间序列；count 非正时不限制数量。
func (series *TimeSeries) RangeAll(ctx context.Context, count int64) ([]TimeSeriesSample, error) {
	return series.rangeSamples(ctx, "TS.RANGE", "-", "+", count)
}

func (series *TimeSeries) rangeSamples(ctx context.Context, name, start, end string, count int64) ([]TimeSeriesSample, error) {
	args := []string{start, end}
	if count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(count, 10))
	}
	command := series.redis.client.B().Arbitrary(name).Keys(series.key).Args(args...).ReadOnly()
	messages, err := series.redis.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	samples := make([]TimeSeriesSample, len(messages))
	for index, message := range messages {
		parts, partErr := message.ToArray()
		if partErr != nil {
			return nil, fmt.Errorf("decode time series sample %d: %w", index, partErr)
		}
		sample, sampleErr := decodeTimeSeriesSample(parts)
		if sampleErr != nil {
			return nil, fmt.Errorf("decode time series sample %d: %w", index, sampleErr)
		}
		samples[index] = sample
	}
	return samples, nil
}

func decodeTimeSeriesSample(parts []rueidis.RedisMessage) (TimeSeriesSample, error) {
	if len(parts) != 2 {
		return TimeSeriesSample{}, fmt.Errorf("unexpected time series sample length: got %d want 2", len(parts))
	}
	timestamp, err := parts[0].AsInt64()
	if err != nil {
		return TimeSeriesSample{}, err
	}
	value, err := parts[1].AsFloat64()
	if err != nil {
		return TimeSeriesSample{}, err
	}
	return TimeSeriesSample{Timestamp: timestamp, Value: value}, nil
}

// DeleteRange 删除毫秒时间戳闭区间内的样本，并返回删除数量。
func (series *TimeSeries) DeleteRange(ctx context.Context, start, end int64) (int64, error) {
	args := []string{strconv.FormatInt(start, 10), strconv.FormatInt(end, 10)}
	command := series.redis.client.B().Arbitrary("TS.DEL").Keys(series.key).Args(args...).Build()
	return series.redis.client.Do(ctx, command).AsInt64()
}

// Increment 原子增加最新值；序列为空时从 delta 开始，并返回新样本时间戳。
func (series *TimeSeries) Increment(ctx context.Context, delta float64) (int64, error) {
	return series.change(ctx, "TS.INCRBY", delta)
}

// Decrement 原子减少最新值；序列为空时从零开始减少，并返回新样本时间戳。
func (series *TimeSeries) Decrement(ctx context.Context, delta float64) (int64, error) {
	return series.change(ctx, "TS.DECRBY", delta)
}

func (series *TimeSeries) change(ctx context.Context, commandName string, delta float64) (int64, error) {
	if !isFinite(delta) {
		return 0, ErrInvalidTimeSeriesValue
	}
	command := series.redis.client.B().Arbitrary(commandName).Keys(series.key).
		Args(strconv.FormatFloat(delta, 'g', -1, 64)).Build()
	return series.redis.client.Do(ctx, command).AsInt64()
}

// Clear 删除整个时间序列。
func (series *TimeSeries) Clear(ctx context.Context) error {
	return series.redis.client.Do(ctx, series.redis.client.B().Del().Key(series.key).Build()).Error()
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
