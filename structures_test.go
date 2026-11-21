package rueidistpl

import (
	"context"
	"slices"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestCounterLifecycle 验证计数器的读取、覆盖、加一、按 delta 增加和删除，
// 并确认所有操作都使用应用全局 key 前缀。
func TestCounterLifecycle(t *testing.T) {
	// Arrange：按读取、覆盖、两次原子增加和删除的顺序声明 mock 命令。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	counter := redis.Counter("visits")
	realKey := counter.Key()
	if realKey != "app:visits" {
		t.Fatalf("Key value=%q", realKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("GET", "app:visits")).Return(rueidismock.Result(rueidismock.RedisBlobString("10"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:visits", "20")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("INCRBY", "app:visits", "1")).Return(rueidismock.Result(rueidismock.RedisInt64(21))),
		client.EXPECT().Do(ctx, rueidismock.Match("INCRBY", "app:visits", "4")).Return(rueidismock.Result(rueidismock.RedisInt64(25))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:visits")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)
	// Act/Assert：每一步既核对新计数值，也由 gomock 检查真实 key 和 delta。
	getValue, getErr := counter.Get(ctx)
	if getErr != nil || getValue != 10 {
		t.Fatalf("Get value=%d err=%v", getValue, getErr)
	}
	setErr := counter.Set(ctx, 20)
	if setErr != nil {
		t.Fatal(setErr)
	}
	incrementValue, incrementErr := counter.Increment(ctx)
	if incrementErr != nil || incrementValue != 21 {
		t.Fatalf("Increment value=%d err=%v", incrementValue, incrementErr)
	}
	addValue, addErr := counter.Add(ctx, 4)
	if addErr != nil || addValue != 25 {
		t.Fatalf("Add value=%d err=%v", addValue, addErr)
	}
	deleteErr := counter.Delete(ctx)
	if deleteErr != nil {
		t.Fatal(deleteErr)
	}
}

// TestStringListLifecycle 验证 List 兼容入口和 StringList 明确入口生成相同 key，
// 并覆盖左右压入/弹出、范围读取、长度、裁剪及清空的原始字符串语义。
func TestStringListLifecycle(t *testing.T) {
	// Arrange：List 是兼容入口，StringList 是明确入口，两者都必须应用相同全局前缀。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	list := redis.List("events")
	compatibilityKey := list.Key()
	explicitKey := redis.StringList("events").Key()
	if compatibilityKey != "app:events" || explicitKey != "app:events" {
		t.Fatalf("List keys compatibility=%q explicit=%q", compatibilityKey, explicitKey)
	}
	// 声明字符串列表完整生命周期，不经过 JSON 编解码。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("LPUSH", "app:events", "a", "b")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("RPUSH", "app:events", "c")).Return(rueidismock.Result(rueidismock.RedisInt64(3))),
		client.EXPECT().Do(ctx, rueidismock.Match("LPOP", "app:events")).Return(rueidismock.Result(rueidismock.RedisBlobString("b"))),
		client.EXPECT().Do(ctx, rueidismock.Match("RPOP", "app:events")).Return(rueidismock.Result(rueidismock.RedisBlobString("c"))),
		client.EXPECT().Do(ctx, rueidismock.Match("LRANGE", "app:events", "0", "-1")).Return(rueidismock.Result(redisStrings("a", "b"))),
		client.EXPECT().Do(ctx, rueidismock.Match("LLEN", "app:events")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("LTRIM", "app:events", "0", "9")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:events")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)
	// 执行左右压入/弹出并核对 Redis 的元素顺序语义。
	leftLength, pushLeftErr := list.PushLeft(ctx, "a", "b")
	if pushLeftErr != nil || leftLength != 2 {
		t.Fatalf("PushLeft length=%d err=%v", leftLength, pushLeftErr)
	}
	rightLength, pushRightErr := list.PushRight(ctx, "c")
	if pushRightErr != nil || rightLength != 3 {
		t.Fatalf("PushRight length=%d err=%v", rightLength, pushRightErr)
	}
	popLeftValue, popLeftErr := list.PopLeft(ctx)
	if popLeftErr != nil || popLeftValue != "b" {
		t.Fatalf("PopLeft value=%q err=%v", popLeftValue, popLeftErr)
	}
	popRightValue, popRightErr := list.PopRight(ctx)
	if popRightErr != nil || popRightValue != "c" {
		t.Fatalf("PopRight value=%q err=%v", popRightValue, popRightErr)
	}
	// 最后验证范围、长度、裁剪和清空等维护操作。
	rangeValues, rangeErr := list.Range(ctx, 0, -1)
	if rangeErr != nil || !slices.Equal(rangeValues, []string{"a", "b"}) {
		t.Fatalf("Range values=%v err=%v", rangeValues, rangeErr)
	}
	collectionLength, lenErr := list.Len(ctx)
	if lenErr != nil || collectionLength != 2 {
		t.Fatalf("Len value=%d err=%v", collectionLength, lenErr)
	}
	trimErr := list.Trim(ctx, 0, 9)
	if trimErr != nil {
		t.Fatal(trimErr)
	}
	clearErr := list.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestStringListEmptyPush 验证不传元素时不会发送空 LPUSH/RPUSH，
// 而是通过 LLEN 返回当前列表长度。
func TestStringListEmptyPush(t *testing.T) {
	// Arrange：只允许发生两次 LLEN，用它约束空 Push 不得发送 LPUSH/RPUSH。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	list := redis.StringList("events")
	client.EXPECT().Do(ctx, rueidismock.Match("LLEN", "app:events")).Return(rueidismock.Result(rueidismock.RedisInt64(2))).Times(2)
	// 左右入口都应返回当前长度 2，且不修改列表内容。
	leftLength, pushLeftErr := list.PushLeft(ctx)
	if pushLeftErr != nil || leftLength != 2 {
		t.Fatalf("PushLeft length=%d err=%v", leftLength, pushLeftErr)
	}
	rightLength, pushRightErr := list.PushRight(ctx)
	if pushRightErr != nil || rightLength != 2 {
		t.Fatalf("PushRight length=%d err=%v", rightLength, pushRightErr)
	}
}

// TestStringSetLifecycle 覆盖字符串集合的新增、删除、全量成员、成员判断、计数和清空；
// 空成员切片必须直接返回 0，不访问 Redis。
func TestStringSetLifecycle(t *testing.T) {
	// Arrange：准备新增、移除、成员读取、包含判断、计数和清空的顺序响应。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	set := redis.StringSet("tags")
	realKey := set.Key()
	if realKey != "app:tags" {
		t.Fatalf("Key value=%q", realKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("SADD", "app:tags", "go", "redis")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("SREM", "app:tags", "redis")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("SMEMBERS", "app:tags")).Return(rueidismock.Result(redisStrings("go"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SSCAN", "app:tags", "0", "COUNT", "100")).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString("7"),
			redisStrings("go", "redis"),
		))),
		client.EXPECT().Do(ctx, rueidismock.Match("SSCAN", "app:tags", "7", "MATCH", "r*", "COUNT", "5")).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString("0"),
			redisStrings("redis"),
		))),
		client.EXPECT().Do(ctx, rueidismock.Match("SISMEMBER", "app:tags", "go")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("SCARD", "app:tags")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:tags")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)
	// Act/Assert：核对实际新增/删除数量以及成员读取结果。
	addedCount, addErr := set.Add(ctx, "go", "redis")
	if addErr != nil || addedCount != 2 {
		t.Fatalf("Add value=%d err=%v", addedCount, addErr)
	}
	removedCount, removeErr := set.Remove(ctx, "redis")
	if removeErr != nil || removedCount != 1 {
		t.Fatalf("Remove value=%d err=%v", removedCount, removeErr)
	}
	members, membersErr := set.Members(ctx)
	if membersErr != nil || !slices.Equal(members, []string{"go"}) {
		t.Fatalf("Members values=%v err=%v", members, membersErr)
	}
	// 游标扫描先验证默认 COUNT，再带上 MATCH 继续上一批游标。
	firstScan, firstScanErr := set.Scan(ctx, 0, "", 0)
	if firstScanErr != nil || firstScan.Cursor != 7 || !slices.Equal(firstScan.Members, []string{"go", "redis"}) {
		t.Fatalf("first Scan value=%+v err=%v", firstScan, firstScanErr)
	}
	secondScan, secondScanErr := set.Scan(ctx, firstScan.Cursor, "r*", 5)
	if secondScanErr != nil || secondScan.Cursor != 0 || !slices.Equal(secondScan.Members, []string{"redis"}) {
		t.Fatalf("second Scan value=%+v err=%v", secondScan, secondScanErr)
	}
	contains, containsErr := set.Contains(ctx, "go")
	if containsErr != nil || !contains {
		t.Fatalf("Contains value=%t err=%v", contains, containsErr)
	}
	collectionLength, lenErr := set.Len(ctx)
	if lenErr != nil || collectionLength != 1 {
		t.Fatalf("Len value=%d err=%v", collectionLength, lenErr)
	}
	clearErr := set.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
	// 空 Add/Remove 必须本地返回 0；mock 没有额外预期，可防止意外访问 Redis。
	emptyAddedCount, addErr2 := set.Add(ctx)
	if addErr2 != nil || emptyAddedCount != 0 {
		t.Fatalf("empty Add value=%d err=%v", emptyAddedCount, addErr2)
	}
	emptyRemovedCount, removeErr2 := set.Remove(ctx)
	if removeErr2 != nil || emptyRemovedCount != 0 {
		t.Fatalf("empty Remove value=%d err=%v", emptyRemovedCount, removeErr2)
	}
}
