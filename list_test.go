package rueidistpl

import (
	"context"
	"slices"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestGenericListLifecycle 验证泛型 List 的完整正常路径：
// 所有对象必须先编码为 JSON 再进入 Redis，读取、按下标查询和弹出时恢复为原类型；
// 同时精确检查左右压入/弹出、覆盖、删除、裁剪和清空对应的 Redis 命令。
func TestGenericListLifecycle(t *testing.T) {
	// Arrange：准备两个对象和对应 JSON，用于同时校验命令载荷与读取结果。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	list := NewList[unitUser](redis, "users")
	alice := unitUser{ID: 1, Name: "Alice"}
	bob := unitUser{ID: 2, Name: "Bob"}
	aliceJSON := `{"id":1,"name":"Alice"}`
	bobJSON := `{"id":2,"name":"Bob"}`

	if list.Key() != "app:users" {
		t.Fatalf("unexpected key %q", list.Key())
	}

	// 按完整生命周期声明 Redis 响应，所有写命令必须携带 JSON 而不是 Go 格式字符串。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("LPUSH", "app:users", aliceJSON)).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("RPUSH", "app:users", aliceJSON, bobJSON)).Return(rueidismock.Result(rueidismock.RedisInt64(3))),
		client.EXPECT().Do(ctx, rueidismock.Match("LRANGE", "app:users", "0", "-1")).Return(rueidismock.Result(redisStrings(aliceJSON, bobJSON))),
		client.EXPECT().Do(ctx, rueidismock.Match("LINDEX", "app:users", "1")).Return(rueidismock.Result(rueidismock.RedisBlobString(bobJSON))),
		client.EXPECT().Do(ctx, rueidismock.Match("LSET", "app:users", "1", aliceJSON)).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("LREM", "app:users", "0", bobJSON)).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("LPOP", "app:users")).Return(rueidismock.Result(rueidismock.RedisBlobString(aliceJSON))),
		client.EXPECT().Do(ctx, rueidismock.Match("RPOP", "app:users")).Return(rueidismock.Result(rueidismock.RedisBlobString(bobJSON))),
		client.EXPECT().Do(ctx, rueidismock.Match("LLEN", "app:users")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("LTRIM", "app:users", "0", "9")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:users")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 写入与读取：验证左右压入、范围解码和按下标查询。
	leftLength, pushLeftErr := list.PushLeft(ctx, alice)
	if pushLeftErr != nil || leftLength != 1 {
		t.Fatalf("PushLeft length=%d err=%v", leftLength, pushLeftErr)
	}
	rightLength, pushRightErr := list.PushRight(ctx, alice, bob)
	if pushRightErr != nil || rightLength != 3 {
		t.Fatalf("PushRight length=%d err=%v", rightLength, pushRightErr)
	}
	listValues, rangeErr := list.Range(ctx, 0, -1)
	if rangeErr != nil || !slices.Equal(listValues, []unitUser{alice, bob}) {
		t.Fatalf("Range values=%+v err=%v", listValues, rangeErr)
	}
	indexedValue, indexErr := list.Index(ctx, 1)
	if indexErr != nil || indexedValue != bob {
		t.Fatalf("Index value=%+v err=%v", indexedValue, indexErr)
	}
	// 修改与删除：LSET/LREM 必须使用同一 JSON 表示，保证结构体值匹配稳定。
	setErr := list.Set(ctx, 1, alice)
	if setErr != nil {
		t.Fatal(setErr)
	}
	removedCount, removeErr := list.Remove(ctx, 0, bob)
	if removeErr != nil || removedCount != 1 {
		t.Fatalf("Remove value=%d err=%v", removedCount, removeErr)
	}
	// 弹出与维护：返回值恢复为强类型，最后验证长度、裁剪和清空。
	leftValue, popLeftErr := list.PopLeft(ctx)
	if popLeftErr != nil || leftValue != alice {
		t.Fatalf("PopLeft value=%+v err=%v", leftValue, popLeftErr)
	}
	rightValue, popRightErr := list.PopRight(ctx)
	if popRightErr != nil || rightValue != bob {
		t.Fatalf("PopRight value=%+v err=%v", rightValue, popRightErr)
	}
	listLength, lenErr := list.Len(ctx)
	if lenErr != nil || listLength != 2 {
		t.Fatalf("Len value=%d err=%v", listLength, lenErr)
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

// TestGenericListEmptyPushAndJSONErrors 验证两个重要边界：
// 空 Push 不发送非法 LPUSH/RPUSH，而是返回现有长度；无法编码的对象和列表中的脏 JSON 必须报错。
func TestGenericListEmptyPushAndJSONErrors(t *testing.T) {
	// Arrange：空 Push 的正确行为是查询现有长度，因此只允许两次 LLEN。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	list := NewList[unitUser](redis, "users")
	client.EXPECT().Do(ctx, rueidismock.Match("LLEN", "app:users")).Return(rueidismock.Result(rueidismock.RedisInt64(4))).Times(2)
	// 左右两个空 Push 都不能生成 Redis 不接受的无 element 命令。
	leftLength, pushLeftErr := list.PushLeft(ctx)
	if pushLeftErr != nil || leftLength != 4 {
		t.Fatalf("empty PushLeft length=%d err=%v", leftLength, pushLeftErr)
	}
	rightLength, pushRightErr := list.PushRight(ctx)
	if pushRightErr != nil || rightLength != 4 {
		t.Fatalf("empty PushRight length=%d err=%v", rightLength, pushRightErr)
	}

	// chan 无法编码为 JSON，Push 和 Remove 都应在接触客户端前失败。
	badList := NewList[chan int](&Redis{}, "bad")
	_, pushRightErr2 := badList.PushRight(ctx, make(chan int))
	if pushRightErr2 == nil {
		t.Fatal("PushRight should reject values unsupported by JSON")
	}
	_, removeErr := badList.Remove(ctx, 0, make(chan int))
	if removeErr == nil {
		t.Fatal("Remove should reject values unsupported by JSON")
	}

	// 模拟列表中混入非 JSON 字符串，Range 必须报告具体解码错误。
	client.EXPECT().Do(ctx, rueidismock.Match("LRANGE", "app:users", "0", "-1")).Return(rueidismock.Result(redisStrings("invalid")))
	_, rangeErr := list.Range(ctx, 0, -1)
	if rangeErr == nil {
		t.Fatal("Range should report invalid JSON")
	}

	// 单元素解码同样可能先写入部分字段；Index 必须丢弃该对象并返回 T 的零值。
	client.EXPECT().Do(ctx, rueidismock.Match("LINDEX", "app:users", "0")).Return(rueidismock.Result(rueidismock.RedisBlobString(`{"id":99,"name":[]}`)))
	invalidValue, indexErr := list.Index(ctx, 0)
	if indexErr == nil || invalidValue != (unitUser{}) {
		t.Fatalf("Index value=%+v err=%v; want zero value and decode error", invalidValue, indexErr)
	}
}
