package rueidistpl

import (
	"context"
	"errors"
	"testing"
	"time"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestValueLifecycle 覆盖单 String JSON 对象的完整生命周期：
// 永久和带 TTL 写入、SET NX 成功/冲突、读取、原子读取删除、存在性、续期、PTTL 和删除；
// 同时验证毫秒以下正 TTL 会提升为 Redis 可表达的 1ms。
func TestValueLifecycle(t *testing.T) {
	// Arrange：固定对象与 JSON，并使用官方 mock 检查每条 String 命令。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	value := NewValue[unitUser](redis, "profile")
	alice := unitUser{ID: 1, Name: "Alice"}
	payload := `{"id":1,"name":"Alice"}`

	if value.Key() != "app:profile" {
		t.Fatalf("unexpected key %q", value.Key())
	}

	// 预期覆盖永久 SET、毫秒 TTL、NX 成功/冲突及完整读取删除链路。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:profile", payload)).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:profile", payload, "PX", "1")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:profile", payload, "NX", "PX", "1000")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:profile", payload, "NX")).Return(rueidismock.Result(rueidismock.RedisNil())),
		client.EXPECT().Do(ctx, rueidismock.Match("GET", "app:profile")).Return(rueidismock.Result(rueidismock.RedisBlobString(payload))),
		client.EXPECT().Do(ctx, nonRetryable(rueidismock.Match("GETDEL", "app:profile"))).Return(rueidismock.Result(rueidismock.RedisBlobString(payload))),
		client.EXPECT().Do(ctx, rueidismock.Match("EXISTS", "app:profile")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("PEXPIRE", "app:profile", "1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("PTTL", "app:profile")).Return(rueidismock.Result(rueidismock.RedisInt64(1500))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:profile")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 写入阶段：先永久写入，再验证纳秒 TTL 被提升为 PX 1。
	setErr := value.Set(ctx, alice)
	if setErr != nil {
		t.Fatal(setErr)
	}
	setErr2 := value.Set(ctx, alice, time.Nanosecond)
	if setErr2 != nil {
		t.Fatal(setErr2)
	}
	// 条件写入阶段：Redis OK 转成 true，Redis Nil 转成无错误的 false。
	setNXStored, setNXErr := value.SetNX(ctx, alice, time.Second)
	if setNXErr != nil || !setNXStored {
		t.Fatalf("SetNX stored=%t err=%v", setNXStored, setNXErr)
	}
	setNXStored2, setNXErr2 := value.SetNX(ctx, alice, 0)
	if setNXErr2 != nil || setNXStored2 {
		t.Fatalf("duplicate SetNX stored=%t err=%v", setNXStored2, setNXErr2)
	}
	// 查询与清理阶段：验证强类型解码、存在性、TTL 单位和删除布尔值。
	storedValue, getErr := value.Get(ctx)
	if getErr != nil || storedValue != alice {
		t.Fatalf("Get value=%+v err=%v", storedValue, getErr)
	}
	// GETDEL 将读取与删除合并为单条命令，返回对象应与普通 Get 完全一致。
	deletedValue, getDeleteErr := value.GetDelete(ctx)
	if getDeleteErr != nil || deletedValue != alice {
		t.Fatalf("GetDelete value=%+v err=%v", deletedValue, getDeleteErr)
	}
	exists, existsErr := value.Exists(ctx)
	if existsErr != nil || !exists {
		t.Fatalf("Exists value=%t err=%v", exists, existsErr)
	}
	expireChanged, expireErr := value.Expire(ctx, time.Nanosecond)
	if expireErr != nil || !expireChanged {
		t.Fatalf("Expire value=%t err=%v", expireChanged, expireErr)
	}
	ttlValue, ttlErr := value.TTL(ctx)
	if ttlErr != nil || ttlValue != 1500*time.Millisecond {
		t.Fatalf("TTL value=%s err=%v", ttlValue, ttlErr)
	}
	deleted, deleteErr := value.Delete(ctx)
	if deleteErr != nil || !deleted {
		t.Fatalf("Delete value=%t err=%v", deleted, deleteErr)
	}
}

// TestValueValidationAndEncodingErrors 验证无法 JSON 编码的对象、非正 Expire TTL、
// 以及 Redis 中的非法 JSON 都会返回错误且不会被当作有效零值。
func TestValueValidationAndEncodingErrors(t *testing.T) {
	// 第一部分不配置客户端，用 chan 字段验证编码和 TTL 参数会在本地失败。
	ctx := context.Background()
	invalid := NewValue[struct {
		Channel chan int `json:"channel"`
	}](&Redis{}, "invalid")
	data := struct {
		Channel chan int `json:"channel"`
	}{Channel: make(chan int)}

	setErr := invalid.Set(ctx, data)
	if setErr == nil {
		t.Fatal("Set should reject values unsupported by JSON")
	}
	_, setNXErr := invalid.SetNX(ctx, data, time.Second)
	if setNXErr == nil {
		t.Fatal("SetNX should reject values unsupported by JSON")
	}
	_, expireErr := invalid.Expire(ctx, 0)
	if !errors.Is(expireErr, ErrInvalidExpiration) {
		t.Fatalf("got %v want %v", expireErr, ErrInvalidExpiration)
	}

	// 第二部分让 JSON 先写入 ID、再因 Name 类型错误而失败，确认 Get/GetDelete 丢弃部分赋值对象。
	redis, client := newMockRedis(t)
	value := NewValue[unitUser](redis, "profile")
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("GET", "app:profile")).Return(rueidismock.Result(rueidismock.RedisBlobString(`{"id":99,"name":[]}`))),
		client.EXPECT().Do(ctx, nonRetryable(rueidismock.Match("GETDEL", "app:profile"))).Return(rueidismock.Result(rueidismock.RedisBlobString(`{"id":99,"name":[]}`))),
	)
	invalidValue, getErr := value.Get(ctx)
	if getErr == nil || invalidValue != (unitUser{}) {
		t.Fatalf("Get value=%+v err=%v; want zero value and decode error", invalidValue, getErr)
	}
	invalidDeletedValue, getDeleteErr := value.GetDelete(ctx)
	if getDeleteErr == nil || invalidDeletedValue != (unitUser{}) {
		t.Fatalf("GetDelete value=%+v err=%v; want zero value and decode error", invalidDeletedValue, getDeleteErr)
	}
}
