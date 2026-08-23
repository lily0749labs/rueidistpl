package rueidistpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/rueidis"
)

// ErrInvalidBatchResponse 表示 Rueidis 返回的批量结果数量或单 key 状态值不符合命令约定。
var ErrInvalidBatchResponse = errors.New("rueidistpl: invalid batch response")

// DeleteKeyResult 记录 DeleteEach 中一个逻辑 key 的执行结果。
type DeleteKeyResult struct {
	Key     string
	Deleted bool
	Err     error
}

// DeleteBatchResult 同时保留逐 key 状态、成功删除总数和聚合错误。
// 即使 Err 非空，Results 中其他成功项和 Deleted 仍然有效。
type DeleteBatchResult struct {
	Results []DeleteKeyResult
	Deleted int64
	Err     error
}

// ExistsKeyResult 记录 ExistsEach 中一个逻辑 key 的存在状态。
type ExistsKeyResult struct {
	Key    string
	Exists bool
	Err    error
}

// ExistsBatchResult 同时保留逐 key 状态、实际存在总数和聚合错误。
// 即使 Err 非空，Results 中其他成功项和 Existing 仍然有效。
type ExistsBatchResult struct {
	Results  []ExistsKeyResult
	Existing int64
	Err      error
}

type booleanKeyResult struct {
	key   string
	value bool
	err   error
}

// DeleteEach 逐 key 执行 DEL，返回与输入顺序一致的明细。
// 每个 key 使用独立命令，因此可以安全处理 Redis Cluster 中跨 Slot 的输入。
func (redis *Redis) DeleteEach(ctx context.Context, keys ...string) DeleteBatchResult {
	commands := make([]rueidis.Completed, len(keys))
	for index, key := range keys {
		commands[index] = redis.client.B().Del().Key(redis.Key(key)).Build()
	}
	if len(commands) == 0 {
		return DeleteBatchResult{Results: make([]DeleteKeyResult, 0)}
	}

	items, count, err := booleanBatchResults("DEL", keys, redis.client.DoMulti(ctx, commands...))
	results := make([]DeleteKeyResult, len(items))
	for index, item := range items {
		results[index] = DeleteKeyResult{Key: item.key, Deleted: item.value, Err: item.err}
	}
	return DeleteBatchResult{Results: results, Deleted: count, Err: err}
}

// ExistsEach 逐 key 执行 EXISTS，返回与输入顺序一致的明细。
// 每个 key 使用独立命令，因此可以安全处理 Redis Cluster 中跨 Slot 的输入。
func (redis *Redis) ExistsEach(ctx context.Context, keys ...string) ExistsBatchResult {
	commands := make([]rueidis.Completed, len(keys))
	for index, key := range keys {
		commands[index] = redis.client.B().Exists().Key(redis.Key(key)).Build()
	}
	if len(commands) == 0 {
		return ExistsBatchResult{Results: make([]ExistsKeyResult, 0)}
	}

	items, count, err := booleanBatchResults("EXISTS", keys, redis.client.DoMulti(ctx, commands...))
	results := make([]ExistsKeyResult, len(items))
	for index, item := range items {
		results[index] = ExistsKeyResult{Key: item.key, Exists: item.value, Err: item.err}
	}
	return ExistsBatchResult{Results: results, Existing: count, Err: err}
}

func booleanBatchResults(operation string, keys []string, redisResults []rueidis.RedisResult) ([]booleanKeyResult, int64, error) {
	results := make([]booleanKeyResult, len(keys))
	errorsByKey := make([]error, 0)
	var count int64
	for index, key := range keys {
		results[index].key = key
		if index >= len(redisResults) {
			itemErr := fmt.Errorf("%s key %q: %w: missing result", operation, key, ErrInvalidBatchResponse)
			results[index].err = itemErr
			errorsByKey = append(errorsByKey, itemErr)
			continue
		}

		value, err := redisResults[index].AsInt64()
		if err != nil {
			itemErr := fmt.Errorf("%s key %q: %w", operation, key, err)
			results[index].err = itemErr
			errorsByKey = append(errorsByKey, itemErr)
			continue
		}
		if value != 0 && value != 1 {
			itemErr := fmt.Errorf("%s key %q: %w: got %d, want 0 or 1", operation, key, ErrInvalidBatchResponse, value)
			results[index].err = itemErr
			errorsByKey = append(errorsByKey, itemErr)
			continue
		}

		results[index].value = value == 1
		count += value
	}
	if len(redisResults) > len(keys) {
		errorsByKey = append(errorsByKey, fmt.Errorf(
			"%s: %w: got %d results for %d keys",
			operation,
			ErrInvalidBatchResponse,
			len(redisResults),
			len(keys),
		))
	}
	return results, count, errors.Join(errorsByKey...)
}
