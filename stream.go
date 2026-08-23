package rueidistpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
)

const streamDataField = "data"

// ErrStreamDataMissing 表示消息不是由本封装写入，缺少约定的 data 字段。
var ErrStreamDataMissing = errors.New("rueidistpl: stream entry has no data field")

// ErrInvalidMinIdle 表示 XCLAIM 收到了负数最小空闲时间。
var ErrInvalidMinIdle = errors.New("rueidistpl: stream minimum idle time must not be negative")

// ErrInvalidStreamMaxLen 表示 Stream 最大长度不是正数。
var ErrInvalidStreamMaxLen = errors.New("rueidistpl: stream maximum length must be positive")

// StreamEntry 表示一条已经完成 JSON 解码的 Stream 消息。
type StreamEntry[T any] struct {
	// ID 是 Redis Stream 消息 ID。
	ID string
	// Value 是 data 字段中的业务对象。
	Value T
}

// PendingEntry 描述消费组中一条尚未 ACK 的消息。
type PendingEntry struct {
	// ID 是 Redis Stream 消息 ID。
	ID string
	// Consumer 是当前持有该 Pending 消息的消费者。
	Consumer string
	// Idle 是距离上次投递经过的时间。
	Idle time.Duration
	// Deliveries 是累计投递次数。
	Deliveries int64
}

// StreamGroupInfo 描述 XINFO GROUPS 返回的消费组运行状态。
type StreamGroupInfo struct {
	// Name 是消费组名称。
	Name string
	// Consumers 是该组已知消费者数。
	Consumers int64
	// Pending 是已投递但尚未 ACK 的消息数。
	Pending int64
	// LastDeliveredID 是最后一条已投递消息的 ID。
	LastDeliveredID string
	// EntriesRead 是 Redis 估算的已读取消息数。
	EntriesRead int64
	// EntriesReadKnown 表示 Redis 是否能给出 EntriesRead。
	EntriesReadKnown bool
	// Lag 是尚未投递给该组的消息数。
	Lag int64
	// LagKnown 表示 Redis 是否能给出 Lag。
	LagKnown bool
}

// StreamConsumerInfo 描述 XINFO CONSUMERS 返回的单个消费者状态。
type StreamConsumerInfo struct {
	// Name 是消费者名称。
	Name string
	// Pending 是归属该消费者且尚未 ACK 的消息数。
	Pending int64
	// Idle 是距离该消费者最后一次交互的时间。
	Idle time.Duration
	// Inactive 是距离该消费者最后一次成功投递的时间，Redis 7.2+ 提供。
	Inactive time.Duration
	// InactiveKnown 表示当前 Redis 响应是否包含 Inactive。
	InactiveKnown bool
}

// AutoClaimResult 是 XAUTOCLAIM 的强类型返回值。
type AutoClaimResult[T any] struct {
	// NextStart 是下一次扫描时应传入的起始 ID；返回 0-0 表示本轮扫描结束。
	NextStart string
	// Entries 是本次转移到新消费者名下的消息。
	Entries []StreamEntry[T]
	// DeletedIDs 是在 Pending 列表中但已从 Stream 删除的 ID；Redis 7.0 起才会返回。
	DeletedIDs []string
}

// Stream 使用单个 data field 保存 JSON 对象。
type Stream[T any] struct {
	client rueidis.Client
	key    string
}

// NewStream 创建将业务对象统一编码到 data 字段的类型化 Stream。
func NewStream[T any](redis *Redis, key string) *Stream[T] {
	return &Stream[T]{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (stream *Stream[T]) Key() string {
	return stream.key
}

// Add 使用 Redis 自动生成的 ID 添加消息，并返回消息 ID。
func (stream *Stream[T]) Add(ctx context.Context, value T) (string, error) {
	return stream.AddWithID(ctx, "*", value)
}

// AddMaxLen 使用 Redis 自动生成的 ID 添加消息，并以 MAXLEN ~ 近似裁剪 Stream。
// 近似裁剪通常比精确裁剪更高效，但长度可能暂时略大于 maxLen。
func (stream *Stream[T]) AddMaxLen(ctx context.Context, value T, maxLen int64) (string, error) {
	return stream.AddWithIDMaxLen(ctx, "*", value, maxLen)
}

// AddWithID 使用调用方指定的消息 ID 添加消息。
func (stream *Stream[T]) AddWithID(ctx context.Context, id string, value T) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	command := stream.client.B().Xadd().Key(stream.key).Id(id).FieldValue().FieldValue(streamDataField, string(payload)).Build()
	return stream.client.Do(ctx, command).ToString()
}

// AddWithIDMaxLen 使用指定 ID 添加消息，并以 MAXLEN ~ 近似裁剪 Stream。
func (stream *Stream[T]) AddWithIDMaxLen(ctx context.Context, id string, value T, maxLen int64) (string, error) {
	if maxLen <= 0 {
		return "", ErrInvalidStreamMaxLen
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	threshold := strconv.FormatInt(maxLen, 10)
	command := stream.client.B().Xadd().Key(stream.key).Maxlen().Almost().Threshold(threshold).
		Id(id).FieldValue().FieldValue(streamDataField, string(payload)).Build()
	return stream.client.Do(ctx, command).ToString()
}

// Range 按 ID 闭区间读取消息；count 非正表示不添加 COUNT 限制。
func (stream *Stream[T]) Range(ctx context.Context, start, end string, count int64) ([]StreamEntry[T], error) {
	var result rueidis.RedisResult
	command := stream.client.B().Xrange().Key(stream.key).Start(start).End(end)
	if count > 0 {
		result = stream.client.Do(ctx, command.Count(count).Build())
	} else {
		result = stream.client.Do(ctx, command.Build())
	}
	entries, err := result.AsXRange()
	if err != nil {
		return nil, err
	}
	return decodeStreamEntries[T](entries)
}

// Read 从 afterID 之后非阻塞读取消息；count 非正时默认读取 100 条。
func (stream *Stream[T]) Read(ctx context.Context, afterID string, count int64) ([]StreamEntry[T], error) {
	if count <= 0 {
		count = 100
	}
	command := stream.client.B().Xread().Count(count).Streams().Key(stream.key).Id(afterID).Build()
	return stream.decodeRead(stream.client.Do(ctx, command))
}

// ReadBlock 最多阻塞 block 等待消息；block 非正时退化为非阻塞 Read，小于 1ms 的正值提升为 1ms。
func (stream *Stream[T]) ReadBlock(ctx context.Context, afterID string, count int64, block time.Duration) ([]StreamEntry[T], error) {
	if count <= 0 {
		count = 100
	}
	if block <= 0 {
		return stream.Read(ctx, afterID, count)
	}
	block = atLeastMillisecond(block)
	command := stream.client.B().Xread().Count(count).Block(block.Milliseconds()).Streams().Key(stream.key).Id(afterID).Build()
	return stream.decodeRead(stream.client.Do(ctx, command))
}

// CreateGroup 创建消费组；组已存在时也返回 nil。
func (stream *Stream[T]) CreateGroup(ctx context.Context, group, startID string) error {
	command := stream.client.B().XgroupCreate().Key(stream.key).Group(group).Id(startID).Mkstream().Build()
	err := stream.client.Do(ctx, command).Error()
	if rueidis.IsRedisBusyGroup(err) {
		return nil
	}
	return err
}

// SetGroupID 修改消费组的最后投递 ID，可用于重放或跳过历史消息。
// 该操作不会清理已有 Pending 消息，调用前应确认业务重放语义。
func (stream *Stream[T]) SetGroupID(ctx context.Context, group, id string) error {
	command := stream.client.B().XgroupSetid().Key(stream.key).Group(group).Id(id).Build()
	return stream.client.Do(ctx, command).Error()
}

// Groups 返回 Stream 的全部消费组状态，包含 Pending 数和可用时的 lag。
func (stream *Stream[T]) Groups(ctx context.Context) ([]StreamGroupInfo, error) {
	command := stream.client.B().XinfoGroups().Key(stream.key).Build()
	rows, err := stream.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	groups := make([]StreamGroupInfo, len(rows))
	for index, row := range rows {
		fields, err := row.AsMap()
		if err != nil {
			return nil, fmt.Errorf("decode stream group %d: %w", index, err)
		}
		group := StreamGroupInfo{}
		group.Name, err = streamInfoString(fields, "name")
		if err == nil {
			group.Consumers, err = streamInfoInt64(fields, "consumers")
		}
		if err == nil {
			group.Pending, err = streamInfoInt64(fields, "pending")
		}
		if err == nil {
			group.LastDeliveredID, err = streamInfoString(fields, "last-delivered-id")
		}
		if err == nil {
			group.EntriesRead, group.EntriesReadKnown, err = streamInfoOptionalInt64(fields, "entries-read")
		}
		if err == nil {
			group.Lag, group.LagKnown, err = streamInfoOptionalInt64(fields, "lag")
		}
		if err != nil {
			return nil, fmt.Errorf("decode stream group %d: %w", index, err)
		}
		groups[index] = group
	}
	return groups, nil
}

// Consumers 返回指定消费组的全部消费者状态。
func (stream *Stream[T]) Consumers(ctx context.Context, group string) ([]StreamConsumerInfo, error) {
	command := stream.client.B().XinfoConsumers().Key(stream.key).Group(group).Build()
	rows, err := stream.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	consumers := make([]StreamConsumerInfo, len(rows))
	for index, row := range rows {
		fields, err := row.AsMap()
		if err != nil {
			return nil, fmt.Errorf("decode stream consumer %d: %w", index, err)
		}
		consumer := StreamConsumerInfo{}
		consumer.Name, err = streamInfoString(fields, "name")
		if err == nil {
			consumer.Pending, err = streamInfoInt64(fields, "pending")
		}
		var idleMilliseconds int64
		if err == nil {
			idleMilliseconds, err = streamInfoInt64(fields, "idle")
			consumer.Idle = time.Duration(idleMilliseconds) * time.Millisecond
		}
		var inactiveMilliseconds int64
		if err == nil {
			inactiveMilliseconds, consumer.InactiveKnown, err = streamInfoOptionalInt64(fields, "inactive")
			consumer.Inactive = time.Duration(inactiveMilliseconds) * time.Millisecond
		}
		if err != nil {
			return nil, fmt.Errorf("decode stream consumer %d: %w", index, err)
		}
		consumers[index] = consumer
	}
	return consumers, nil
}

// ReadGroup 读取尚未投递给其他消费者的新消息。
func (stream *Stream[T]) ReadGroup(ctx context.Context, group, consumer string, count int64) ([]StreamEntry[T], error) {
	return stream.ReadGroupFrom(ctx, group, consumer, ">", count, 0)
}

// ReadGroupFrom 可读取新消息（startID 为 ">"）或当前消费者的 Pending 消息。
// block 小于 1ms 的正值提升为 1ms，避免 Redis 将 BLOCK 0 解释为无限等待。
func (stream *Stream[T]) ReadGroupFrom(ctx context.Context, group, consumer, startID string, count int64, block time.Duration) ([]StreamEntry[T], error) {
	if count <= 0 {
		count = 100
	}
	var command rueidis.Completed
	if block > 0 {
		block = atLeastMillisecond(block)
		command = stream.client.B().Xreadgroup().Group(group, consumer).Count(count).Block(block.Milliseconds()).Streams().Key(stream.key).Id(startID).Build()
	} else {
		command = stream.client.B().Xreadgroup().Group(group, consumer).Count(count).Streams().Key(stream.key).Id(startID).Build()
	}
	return stream.decodeRead(stream.client.Do(ctx, command))
}

// Ack 确认消费成功，并返回从 Pending 列表中移除的消息数。
func (stream *Stream[T]) Ack(ctx context.Context, group string, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	command := stream.client.B().Xack().Key(stream.key).Group(group).Id(ids...).Build()
	return stream.client.Do(ctx, command).AsInt64()
}

// Pending 返回消费组中最多 count 条待确认消息及其消费者、空闲时间和投递次数。
func (stream *Stream[T]) Pending(ctx context.Context, group string, count int64) ([]PendingEntry, error) {
	if count <= 0 {
		return []PendingEntry{}, nil
	}
	command := stream.client.B().Xpending().Key(stream.key).Group(group).Start("-").End("+").Count(count).Build()
	rows, err := stream.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	entries := make([]PendingEntry, len(rows))
	for index, row := range rows {
		values, err := row.ToArray()
		if err != nil {
			return nil, fmt.Errorf("decode pending row %d: %w", index, err)
		}
		if len(values) != 4 {
			return nil, fmt.Errorf("decode pending row %d: got %d fields, want 4", index, len(values))
		}
		entries[index].ID, err = values[0].ToString()
		if err == nil {
			entries[index].Consumer, err = values[1].ToString()
		}
		var idle int64
		if err == nil {
			idle, err = values[2].AsInt64()
			entries[index].Idle = time.Duration(idle) * time.Millisecond
		}
		if err == nil {
			entries[index].Deliveries, err = values[3].AsInt64()
		}
		if err != nil {
			return nil, fmt.Errorf("decode pending row %d: %w", index, err)
		}
	}
	return entries, nil
}

// Claim 将空闲时间不少于 minIdle 的 Pending 消息转移给 consumer。
// minIdle 不能为负；小于 1ms 的正值提升为 1ms，零值表示立即允许转移。
func (stream *Stream[T]) Claim(ctx context.Context, group, consumer string, minIdle time.Duration, ids ...string) ([]StreamEntry[T], error) {
	if len(ids) == 0 {
		return []StreamEntry[T]{}, nil
	}
	if minIdle < 0 {
		return nil, ErrInvalidMinIdle
	}
	minIdle = atLeastMillisecond(minIdle)
	command := stream.client.B().Xclaim().Key(stream.key).Group(group).Consumer(consumer).
		MinIdleTime(strconv.FormatInt(minIdle.Milliseconds(), 10)).Id(ids...).Build()
	entries, err := stream.client.Do(ctx, command).AsXRange()
	if err != nil {
		return nil, err
	}
	return decodeStreamEntries[T](entries)
}

// AutoClaim 扫描 Pending 列表，将空闲时间不少于 minIdle 的消息自动转移给 consumer。
// start 为空时从 0-0 开始；count 非正时默认每次处理 100 条。该命令需要 Redis 6.2+。
func (stream *Stream[T]) AutoClaim(ctx context.Context, group, consumer string, minIdle time.Duration, start string, count int64) (AutoClaimResult[T], error) {
	var empty AutoClaimResult[T]
	if minIdle < 0 {
		return empty, ErrInvalidMinIdle
	}
	if start == "" {
		start = "0-0"
	}
	if count <= 0 {
		count = 100
	}
	minIdle = atLeastMillisecond(minIdle)
	command := stream.client.B().Xautoclaim().Key(stream.key).Group(group).Consumer(consumer).
		MinIdleTime(strconv.FormatInt(minIdle.Milliseconds(), 10)).Start(start).Count(count).Build()
	values, err := stream.client.Do(ctx, command).ToArray()
	if err != nil {
		return empty, err
	}
	if len(values) != 2 && len(values) != 3 {
		return empty, fmt.Errorf("decode XAUTOCLAIM: got %d fields, want 2 or 3", len(values))
	}
	nextStart, err := values[0].ToString()
	if err != nil {
		return empty, fmt.Errorf("decode XAUTOCLAIM next start: %w", err)
	}
	rawEntries, err := values[1].AsXRange()
	if err != nil {
		return empty, fmt.Errorf("decode XAUTOCLAIM entries: %w", err)
	}
	entries, err := decodeStreamEntries[T](rawEntries)
	if err != nil {
		return empty, err
	}
	deletedIDs := []string{}
	if len(values) == 3 {
		deletedIDs, err = values[2].AsStrSlice()
		if err != nil {
			return empty, fmt.Errorf("decode XAUTOCLAIM deleted IDs: %w", err)
		}
	}
	return AutoClaimResult[T]{NextStart: nextStart, Entries: entries, DeletedIDs: deletedIDs}, nil
}

// DeleteGroup 删除消费组及其 Pending 状态，返回该消费组是否存在并被删除。
func (stream *Stream[T]) DeleteGroup(ctx context.Context, group string) (bool, error) {
	command := stream.client.B().XgroupDestroy().Key(stream.key).Group(group).Build()
	return stream.client.Do(ctx, command).AsBool()
}

// DeleteConsumer 从消费组中删除消费者，返回被一并丢弃的 Pending 消息数。
// 调用前应先确认或转移该消费者的 Pending 消息，避免消息失去归属。
func (stream *Stream[T]) DeleteConsumer(ctx context.Context, group, consumer string) (int64, error) {
	command := stream.client.B().XgroupDelconsumer().Key(stream.key).Group(group).Consumername(consumer).Build()
	return stream.client.Do(ctx, command).AsInt64()
}

// TrimMaxLen 使用 XTRIM MAXLEN ~ 近似裁剪 Stream，返回被删除的消息数。
func (stream *Stream[T]) TrimMaxLen(ctx context.Context, maxLen int64) (int64, error) {
	if maxLen <= 0 {
		return 0, ErrInvalidStreamMaxLen
	}
	threshold := strconv.FormatInt(maxLen, 10)
	command := stream.client.B().Xtrim().Key(stream.key).Maxlen().Almost().Threshold(threshold).Build()
	return stream.client.Do(ctx, command).AsInt64()
}

// Delete 从 Stream 中永久删除消息，并返回实际删除数量。
func (stream *Stream[T]) Delete(ctx context.Context, ids ...string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return stream.client.Do(ctx, stream.client.B().Xdel().Key(stream.key).Id(ids...).Build()).AsInt64()
}

// Len 返回 Stream 中的消息数量。
func (stream *Stream[T]) Len(ctx context.Context) (int64, error) {
	return stream.client.Do(ctx, stream.client.B().Xlen().Key(stream.key).Build()).AsInt64()
}

// Clear 删除整个 Stream，包括其消费组状态。
func (stream *Stream[T]) Clear(ctx context.Context) error {
	return stream.client.Do(ctx, stream.client.B().Del().Key(stream.key).Build()).Error()
}

func (stream *Stream[T]) decodeRead(result rueidis.RedisResult) ([]StreamEntry[T], error) {
	streams, err := result.AsXRead()
	if rueidis.IsRedisNil(err) {
		return []StreamEntry[T]{}, nil
	}
	if err != nil {
		return nil, err
	}
	return decodeStreamEntries[T](streams[stream.key])
}

func decodeStreamEntries[T any](entries []rueidis.XRangeEntry) ([]StreamEntry[T], error) {
	result := make([]StreamEntry[T], len(entries))
	for index, entry := range entries {
		raw, exists := entry.FieldValues[streamDataField]
		if !exists {
			return nil, fmt.Errorf("%w: %s", ErrStreamDataMissing, entry.ID)
		}
		value, err := decodeJSON[T](raw)
		if err != nil {
			return nil, fmt.Errorf("decode stream entry %s: %w", entry.ID, err)
		}
		result[index] = StreamEntry[T]{ID: entry.ID, Value: value}
	}
	return result, nil
}

func streamInfoString(fields map[string]rueidis.RedisMessage, name string) (string, error) {
	message, exists := fields[name]
	if !exists {
		return "", fmt.Errorf("missing field %q", name)
	}
	value, err := message.ToString()
	if err != nil {
		return "", fmt.Errorf("field %q: %w", name, err)
	}
	return value, nil
}

func streamInfoInt64(fields map[string]rueidis.RedisMessage, name string) (int64, error) {
	message, exists := fields[name]
	if !exists {
		return 0, fmt.Errorf("missing field %q", name)
	}
	value, err := message.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("field %q: %w", name, err)
	}
	return value, nil
}

func streamInfoOptionalInt64(fields map[string]rueidis.RedisMessage, name string) (int64, bool, error) {
	message, exists := fields[name]
	if !exists {
		return 0, false, nil
	}
	value, err := message.AsInt64()
	if rueidis.IsRedisNil(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("field %q: %w", name, err)
	}
	return value, true, nil
}
