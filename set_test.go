package rueidistpl

import (
	"context"
	"slices"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestGenericSetLifecycle 验证泛型 Set 的完整生命周期：
// 对象以稳定 JSON 字符串去重，成员判断、全量读取、计数、删除和清空都保留 Redis Set 原始语义。
func TestGenericSetLifecycle(t *testing.T) {
	// Arrange：准备两个可比较对象及对应 JSON，同时验证前缀后的真实 key。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	set := NewSet[unitUser](redis, "online-users")
	alice := unitUser{ID: 1, Name: "Alice"}
	bob := unitUser{ID: 2, Name: "Bob"}
	aliceJSON := `{"id":1,"name":"Alice"}`
	bobJSON := `{"id":2,"name":"Bob"}`
	actualKey := set.Key()
	if actualKey != "app:online-users" {
		t.Fatalf("Key value=%q", actualKey)
	}

	// 按写入、查询、删除和清理顺序声明命令，确认 Go 对象不会以 fmt 字符串意外入库。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("SADD", "app:online-users", aliceJSON, bobJSON)).Return(
			rueidismock.Result(rueidismock.RedisInt64(2)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("SISMEMBER", "app:online-users", aliceJSON)).Return(
			rueidismock.Result(rueidismock.RedisInt64(1)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("SMEMBERS", "app:online-users")).Return(
			rueidismock.Result(redisStrings(bobJSON, aliceJSON)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("SSCAN", "app:online-users", "0", "COUNT", "100")).Return(
			rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("8"), redisStrings(aliceJSON, bobJSON))),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("SSCAN", "app:online-users", "8", "MATCH", "*Alice*", "COUNT", "5")).Return(
			rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("0"), redisStrings(aliceJSON))),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("SCARD", "app:online-users")).Return(
			rueidismock.Result(rueidismock.RedisInt64(2)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("SREM", "app:online-users", bobJSON)).Return(
			rueidismock.Result(rueidismock.RedisInt64(1)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:online-users")).Return(
			rueidismock.Result(rueidismock.RedisInt64(1)),
		),
	)

	// Act/Assert：每个返回值都先保存在显式变量中，方便打断点观察新增数、成员值和强类型切片。
	addedCount, addErr := set.Add(ctx, alice, bob)
	if addErr != nil || addedCount != 2 {
		t.Fatalf("Add count=%d err=%v", addedCount, addErr)
	}
	containsAlice, containsErr := set.Contains(ctx, alice)
	if containsErr != nil || !containsAlice {
		t.Fatalf("Contains value=%t err=%v", containsAlice, containsErr)
	}
	members, membersErr := set.Members(ctx)
	if membersErr != nil || !slices.Equal(members, []unitUser{bob, alice}) {
		t.Fatalf("Members values=%+v err=%v", members, membersErr)
	}
	// 分批读取先使用默认 COUNT，再使用上一批游标和 MATCH 继续扫描。
	firstScan, firstScanErr := set.Scan(ctx, 0, "", 0)
	if firstScanErr != nil || firstScan.Cursor != 8 || !slices.Equal(firstScan.Values, []unitUser{alice, bob}) {
		t.Fatalf("first Scan value=%+v err=%v", firstScan, firstScanErr)
	}
	secondScan, secondScanErr := set.Scan(ctx, firstScan.Cursor, "*Alice*", 5)
	if secondScanErr != nil || secondScan.Cursor != 0 || !slices.Equal(secondScan.Values, []unitUser{alice}) {
		t.Fatalf("second Scan value=%+v err=%v", secondScan, secondScanErr)
	}
	setLength, lenErr := set.Len(ctx)
	if lenErr != nil || setLength != 2 {
		t.Fatalf("Len value=%d err=%v", setLength, lenErr)
	}
	removedCount, removeErr := set.Remove(ctx, bob)
	if removeErr != nil || removedCount != 1 {
		t.Fatalf("Remove count=%d err=%v", removedCount, removeErr)
	}
	clearErr := set.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestGenericSetEmptyAndJSONErrors 验证空批次不会生成非法 Redis 命令，
// 无法编码的成员在写入前失败，Redis 中混入的脏 JSON 不会被当作类型零值返回。
func TestGenericSetEmptyAndJSONErrors(t *testing.T) {
	// Arrange/Act/Assert：使用无客户端 Set 验证空 Add/Remove 在本地返回 0。
	ctx := context.Background()
	emptySet := &Set[unitUser]{}
	addedCount, addErr := emptySet.Add(ctx)
	if addErr != nil || addedCount != 0 {
		t.Fatalf("empty Add count=%d err=%v", addedCount, addErr)
	}
	removedCount, removeErr := emptySet.Remove(ctx)
	if removeErr != nil || removedCount != 0 {
		t.Fatalf("empty Remove count=%d err=%v", removedCount, removeErr)
	}

	// chan 无法表示为 JSON，Add、Remove 和 Contains 都必须在访问 Redis 前返回编码错误。
	badSet := &Set[chan int]{}
	badValue := make(chan int)
	badAddedCount, badAddErr := badSet.Add(ctx, badValue)
	if badAddErr == nil || badAddedCount != 0 {
		t.Fatalf("bad Add count=%d err=%v", badAddedCount, badAddErr)
	}
	badRemovedCount, badRemoveErr := badSet.Remove(ctx, badValue)
	if badRemoveErr == nil || badRemovedCount != 0 {
		t.Fatalf("bad Remove count=%d err=%v", badRemovedCount, badRemoveErr)
	}
	badContains, badContainsErr := badSet.Contains(ctx, badValue)
	if badContainsErr == nil || badContains {
		t.Fatalf("bad Contains value=%t err=%v", badContains, badContainsErr)
	}

	// 最后注入一条脏成员，确认 Members 返回 nil 而非部分解码结果。
	redis, client := newMockRedis(t)
	set := NewSet[unitUser](redis, "online-users")
	client.EXPECT().Do(ctx, rueidismock.Match("SMEMBERS", "app:online-users")).Return(
		rueidismock.Result(redisStrings(`{"id":1,"name":"Alice"}`, `not-json`)),
	)
	client.EXPECT().Do(ctx, rueidismock.Match("SSCAN", "app:online-users", "0", "COUNT", "100")).Return(
		rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("0"), redisStrings(`not-json`))),
	)
	members, membersErr := set.Members(ctx)
	if members != nil || membersErr == nil {
		t.Fatalf("dirty Members values=%+v err=%v", members, membersErr)
	}
	// SSCAN 中的脏 JSON 同样不得返回部分页结果，游标也应归零。
	dirtyScan, dirtyScanErr := set.Scan(ctx, 0, "", 0)
	if dirtyScan.Cursor != 0 || dirtyScan.Values != nil || dirtyScanErr == nil {
		t.Fatalf("dirty Scan value=%+v err=%v", dirtyScan, dirtyScanErr)
	}
}
