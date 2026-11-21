package rueidistpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/rueidis"
)

// ErrInvalidBatchResponse 表示 Rueidis 返回的批量结果数量或单 key 状态值不符合命令约定。
var ErrInvalidBatchResponse = errors.New("rueidistpl: invalid batch response")

// 限制每次 DoMulti 的命令数和临时内存；最终明细仍与全部输入一一对应。
const keyBatchSize = 512

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

// DeleteEach 逐 key 执行 DEL，返回与输入顺序一致的明细。
// 每个 key 使用独立命令，因此可以安全处理 Redis Cluster 中跨 Slot 的输入。
// 大批次会分块执行；某个 key 失败不会阻止后续 key，ctx 取消后不再发送后续分块。
func (redis *Redis) DeleteEach(ctx context.Context, keys ...string) DeleteBatchResult {
	results := make([]DeleteKeyResult, len(keys))
	count, err := redis.booleanKeyBatches(ctx, "DEL", keys, func(index int, value bool, err error) {
		results[index] = DeleteKeyResult{Key: keys[index], Deleted: value, Err: err}
	})
	return DeleteBatchResult{Results: results, Deleted: count, Err: err}
}

// ExistsEach 逐 key 执行 EXISTS，返回与输入顺序一致的明细。
// 每个 key 使用独立命令，因此可以安全处理 Redis Cluster 中跨 Slot 的输入。
// 大批次会分块执行；某个 key 失败不会阻止后续 key，ctx 取消后不再发送后续分块。
func (redis *Redis) ExistsEach(ctx context.Context, keys ...string) ExistsBatchResult {
	results := make([]ExistsKeyResult, len(keys))
	count, err := redis.booleanKeyBatches(ctx, "EXISTS", keys, func(index int, value bool, err error) {
		results[index] = ExistsKeyResult{Key: keys[index], Exists: value, Err: err}
	})
	return ExistsBatchResult{Results: results, Existing: count, Err: err}
}

func (redis *Redis) booleanKeyBatches(ctx context.Context, operation string, keys []string, setResult func(int, bool, error)) (int64, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	commands := make([]rueidis.Completed, 0, min(len(keys), keyBatchSize))
	var count int64
	var batchErrors []error
	for offset := 0; offset < len(keys); {
		if err := ctx.Err(); err != nil {
			for index := offset; index < len(keys); index++ {
				setResult(index, false, fmt.Errorf("%s key %q: %w", operation, keys[index], err))
			}
			batchErrors = append(batchErrors, err)
			break
		}
		end := offset + min(keyBatchSize, len(keys)-offset)
		commands = commands[:0]
		for _, key := range keys[offset:end] {
			if operation == "DEL" {
				commands = append(commands, redis.client.B().Del().Key(redis.Key(key)).Build())
			} else {
				commands = append(commands, redis.client.B().Exists().Key(redis.Key(key)).Build())
			}
		}
		batchCount, err := booleanBatchResults(operation, keys[offset:end], redis.client.DoMulti(ctx, commands...), func(index int, value bool, err error) {
			setResult(offset+index, value, err)
		})
		count += batchCount
		if err != nil {
			batchErrors = append(batchErrors, err)
		}
		offset = end
	}
	return count, errors.Join(batchErrors...)
}

func booleanBatchResults(operation string, keys []string, redisResults []rueidis.RedisResult, setResult func(int, bool, error)) (int64, error) {
	var errorsByKey []error
	var count int64
	for index, key := range keys {
		if index >= len(redisResults) {
			itemErr := fmt.Errorf("%s key %q: %w: missing result", operation, key, ErrInvalidBatchResponse)
			setResult(index, false, itemErr)
			errorsByKey = append(errorsByKey, itemErr)
			continue
		}

		value, err := redisResults[index].AsInt64()
		if err != nil {
			itemErr := fmt.Errorf("%s key %q: %w", operation, key, err)
			setResult(index, false, itemErr)
			errorsByKey = append(errorsByKey, itemErr)
			continue
		}
		if value != 0 && value != 1 {
			itemErr := fmt.Errorf("%s key %q: %w: got %d, want 0 or 1", operation, key, ErrInvalidBatchResponse, value)
			setResult(index, false, itemErr)
			errorsByKey = append(errorsByKey, itemErr)
			continue
		}

		setResult(index, value == 1, nil)
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
	return count, errors.Join(errorsByKey...)
}
