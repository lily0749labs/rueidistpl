package rueidistpl

import (
	"context"
	"strconv"

	"github.com/redis/rueidis"
)

// Counter 是使用 Redis String 实现的 int64 原子计数器。
type Counter struct {
	redis *Redis
	key   string
}

// Counter 创建计数器并自动应用全局 key 前缀。
func (redis *Redis) Counter(key string) *Counter {
	return &Counter{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (counter *Counter) Key() string {
	return counter.key
}

// Get 返回当前计数值。
func (counter *Counter) Get(ctx context.Context) (int64, error) {
	return counter.redis.client.Do(ctx, counter.redis.client.B().Get().Key(counter.key).Build()).AsInt64()
}

// Set 覆盖当前计数值。
func (counter *Counter) Set(ctx context.Context, value int64) error {
	return counter.redis.client.Do(
		ctx,
		counter.redis.client.B().Set().Key(counter.key).Value(strconv.FormatInt(value, 10)).Build(),
	).Error()
}

// Increment 原子加一并返回新值。
func (counter *Counter) Increment(ctx context.Context) (int64, error) {
	return counter.Add(ctx, 1)
}

// Add 原子增加 delta，并返回增加后的值；delta 可以为负数。
func (counter *Counter) Add(ctx context.Context, delta int64) (int64, error) {
	command := counter.redis.client.B().Incrby().Key(counter.key).Increment(delta).Build()
	return counter.redis.client.Do(ctx, command).AsInt64()
}

// Delete 删除计数器。
func (counter *Counter) Delete(ctx context.Context) error {
	return counter.redis.client.Do(ctx, counter.redis.client.B().Del().Key(counter.key).Build()).Error()
}

// StringList 是直接保存字符串的 Redis List。
type StringList struct {
	client rueidis.Client
	key    string
}

// List 是 StringList 的兼容简写。
func (redis *Redis) List(key string) *StringList {
	return redis.StringList(key)
}

// StringList 返回保存原始字符串的 List；对象列表使用 NewList[T]。
func (redis *Redis) StringList(key string) *StringList {
	return &StringList{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (list *StringList) Key() string {
	return list.key
}

// PushLeft 从左侧压入字符串，并返回列表新长度。
func (list *StringList) PushLeft(ctx context.Context, values ...string) (int64, error) {
	if len(values) == 0 {
		return list.Len(ctx)
	}
	return list.client.Do(ctx, list.client.B().Lpush().Key(list.key).Element(values...).Build()).AsInt64()
}

// PushRight 从右侧压入字符串，并返回列表新长度。
func (list *StringList) PushRight(ctx context.Context, values ...string) (int64, error) {
	if len(values) == 0 {
		return list.Len(ctx)
	}
	return list.client.Do(ctx, list.client.B().Rpush().Key(list.key).Element(values...).Build()).AsInt64()
}

// PopLeft 从左侧弹出一个字符串；空列表返回 Redis Nil 错误。
func (list *StringList) PopLeft(ctx context.Context) (string, error) {
	return list.client.Do(ctx, list.client.B().Lpop().Key(list.key).Build()).ToString()
}

// PopRight 从右侧弹出一个字符串；空列表返回 Redis Nil 错误。
func (list *StringList) PopRight(ctx context.Context) (string, error) {
	return list.client.Do(ctx, list.client.B().Rpop().Key(list.key).Build()).ToString()
}

// Range 按 Redis LRANGE 的闭区间下标返回元素，stop=-1 表示列表末尾。
func (list *StringList) Range(ctx context.Context, start, stop int64) ([]string, error) {
	return list.client.Do(ctx, list.client.B().Lrange().Key(list.key).Start(start).Stop(stop).Build()).AsStrSlice()
}

// Len 返回列表长度。
func (list *StringList) Len(ctx context.Context) (int64, error) {
	return list.client.Do(ctx, list.client.B().Llen().Key(list.key).Build()).AsInt64()
}

// Trim 只保留下标闭区间内的元素。
func (list *StringList) Trim(ctx context.Context, start, stop int64) error {
	return list.client.Do(ctx, list.client.B().Ltrim().Key(list.key).Start(start).Stop(stop).Build()).Error()
}

// Clear 删除整个列表 key。
func (list *StringList) Clear(ctx context.Context) error {
	return list.client.Do(ctx, list.client.B().Del().Key(list.key).Build()).Error()
}

// StringSet 是 member 为字符串的 Redis Set。
type StringSet struct {
	client rueidis.Client
	key    string
}

// StringSetScan 表示一次字符串 Set 游标扫描结果。
type StringSetScan struct {
	// Cursor 是下一次扫描使用的游标；返回 0 表示扫描完成。
	Cursor uint64
	// Members 是本批字符串成员。
	Members []string
}

// StringSet 创建字符串集合并自动应用全局 key 前缀。
func (redis *Redis) StringSet(key string) *StringSet {
	return &StringSet{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (set *StringSet) Key() string {
	return set.key
}

// Add 添加成员并返回实际新增数量。
func (set *StringSet) Add(ctx context.Context, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	return set.client.Do(ctx, set.client.B().Sadd().Key(set.key).Member(members...).Build()).AsInt64()
}

// Remove 删除成员并返回实际删除数量。
func (set *StringSet) Remove(ctx context.Context, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	return set.client.Do(ctx, set.client.B().Srem().Key(set.key).Member(members...).Build()).AsInt64()
}

// Members 返回全部成员；Redis Set 不保证返回顺序。
func (set *StringSet) Members(ctx context.Context) ([]string, error) {
	return set.client.Do(ctx, set.client.B().Smembers().Key(set.key).Build()).AsStrSlice()
}

// Scan 分批扫描字符串 Set。pattern 为空时不过滤，count 非正时默认为 100。
func (set *StringSet) Scan(ctx context.Context, cursor uint64, pattern string, count int64) (StringSetScan, error) {
	if count <= 0 {
		count = 100
	}
	command := set.client.B().Sscan().Key(set.key).Cursor(cursor)
	var (
		entry rueidis.ScanEntry
		err   error
	)
	if pattern == "" {
		entry, err = set.client.Do(ctx, command.Count(count).Build()).AsScanEntry()
	} else {
		entry, err = set.client.Do(ctx, command.Match(pattern).Count(count).Build()).AsScanEntry()
	}
	if err != nil {
		return StringSetScan{}, err
	}
	return StringSetScan{Cursor: entry.Cursor, Members: entry.Elements}, nil
}

// Contains 判断 member 是否存在。
func (set *StringSet) Contains(ctx context.Context, member string) (bool, error) {
	return set.client.Do(ctx, set.client.B().Sismember().Key(set.key).Member(member).Build()).AsBool()
}

// Len 返回集合成员数量。
func (set *StringSet) Len(ctx context.Context) (int64, error) {
	return set.client.Do(ctx, set.client.B().Scard().Key(set.key).Build()).AsInt64()
}

// Clear 删除整个集合 key。
func (set *StringSet) Clear(ctx context.Context) error {
	return set.client.Do(ctx, set.client.B().Del().Key(set.key).Build()).Error()
}
