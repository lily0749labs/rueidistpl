package rueidistpl

import (
	"context"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestJSONDocumentLifecycle 验证 JSON 根文档、条件写入、JSONPath 局部操作和清理的完整流程。
func TestJSONDocumentLifecycle(t *testing.T) {
	// Arrange：JSONDocument 的所有命令都应使用添加 app 前缀后的真实 key。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	document := NewJSONDocument[unitUser](redis, "profiles:1")
	actualKey := document.Key()
	if actualKey != "app:profiles:1" {
		t.Fatalf("Key value=%q", actualKey)
	}
	alice := unitUser{ID: 1, Name: "alice"}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("JSON.SET", "app:profiles:1", "$", `{"id":1,"name":"alice"}`)).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("JSON.SET", "app:profiles:1", "$", `{"id":1,"name":"alice"}`, "NX")).Return(rueidismock.Result(rueidismock.RedisNil())),
		client.EXPECT().Do(ctx, rueidismock.Match("JSON.SET", "app:profiles:1", "$", `{"id":1,"name":"alice"}`, "XX")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("JSON.GET", "app:profiles:1")).Return(rueidismock.Result(rueidismock.RedisBlobString(`{"id":1,"name":"alice"}`))),
		client.EXPECT().Do(ctx, rueidismock.Match("JSON.SET", "app:profiles:1", "$.name", `"Alice"`)).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("JSON.GET", "app:profiles:1", "$.name")).Return(rueidismock.Result(rueidismock.RedisBlobString(`["Alice"]`))),
		client.EXPECT().Do(ctx, rueidismock.Match("JSON.MERGE", "app:profiles:1", "$", `{"name":"A"}`)).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("JSON.DEL", "app:profiles:1", "$.name")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("EXISTS", "app:profiles:1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:profiles:1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 根写入使用强类型值；NX 未执行不是异常，XX 真正覆盖时返回 true。
	setErr := document.Set(ctx, alice)
	nxApplied, nxErr := document.SetNX(ctx, alice)
	xxApplied, xxErr := document.SetXX(ctx, alice)
	if setErr != nil || nxErr != nil || nxApplied || xxErr != nil || !xxApplied {
		t.Fatalf("Set err=%v; SetNX applied=%t err=%v; SetXX applied=%t err=%v", setErr, nxApplied, nxErr, xxApplied, xxErr)
	}

	// Get 将完整 JSON 解码成 unitUser，变量保留到断点作用域，便于直接检查字段值。
	loaded, getErr := document.Get(ctx)
	if getErr != nil || loaded != alice {
		t.Fatalf("Get value=%+v err=%v", loaded, getErr)
	}

	// JSONPath 读取保留 RedisJSON 的匹配数组，避免封装层擅自丢失多匹配结果。
	pathSetErr := document.SetPath(ctx, "$.name", "Alice")
	rawName, pathGetErr := document.GetPath(ctx, "$.name")
	if pathSetErr != nil || pathGetErr != nil || string(rawName) != `["Alice"]` {
		t.Fatalf("SetPath err=%v; GetPath raw=%s err=%v", pathSetErr, rawName, pathGetErr)
	}

	// 合并、路径删除、存在检查和整 key 清理分别核对返回值，确保调用方无需接触原生命令。
	mergeErr := document.MergePath(ctx, "$", map[string]string{"name": "A"})
	deleted, deleteErr := document.DeletePath(ctx, "$.name")
	exists, existsErr := document.Exists(ctx)
	clearErr := document.Clear(ctx)
	if mergeErr != nil || deleteErr != nil || deleted != 1 || existsErr != nil || !exists || clearErr != nil {
		t.Fatalf("Merge err=%v; Delete=%d/%v; Exists=%t/%v; Clear=%v", mergeErr, deleted, deleteErr, exists, existsErr, clearErr)
	}
}

// TestJSONDocumentErrors 验证编码失败、文档缺失及 JSON 解码失败时不会泄漏部分值。
func TestJSONDocumentErrors(t *testing.T) {
	// 无客户端门面用于证明 SetPath/MergePath 的 JSON 编码错误会在发送命令之前返回。
	ctx := context.Background()
	emptyDocument := NewJSONDocument[unitUser](&Redis{}, "profile")
	setPathErr := emptyDocument.SetPath(ctx, "$", func() {})
	mergePathErr := emptyDocument.MergePath(ctx, "$", make(chan int))
	if setPathErr == nil || mergePathErr == nil {
		t.Fatalf("SetPath err=%v; MergePath err=%v", setPathErr, mergePathErr)
	}

	// 缺失时返回 unitUser 零值和 Redis Nil；坏 JSON 也必须返回零值而非部分解码结果。
	redis, client := newMockRedis(t)
	document := NewJSONDocument[unitUser](redis, "profile")
	command := rueidismock.Match("JSON.GET", "app:profile")
	gomock.InOrder(
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisNil())),
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisBlobString(`{"id":1,"name":`))),
	)
	missingValue, missingErr := document.Get(ctx)
	invalidValue, invalidErr := document.Get(ctx)
	if missingValue != (unitUser{}) || !IsNotFound(missingErr) {
		t.Fatalf("missing value=%+v err=%v", missingValue, missingErr)
	}
	if invalidValue != (unitUser{}) || invalidErr == nil {
		t.Fatalf("invalid value=%+v err=%v", invalidValue, invalidErr)
	}
}
