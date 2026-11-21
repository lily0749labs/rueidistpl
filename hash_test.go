package rueidistpl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestHashLifecycle 用官方 Rueidis mock 覆盖 Hash 的完整便利接口：
// 写入、条件写入、批量读取、全量读取、游标扫描、数值递增、field TTL、删除和清空。
// 每一步同时核对 Redis 命令和 JSON 结果，避免只验证返回值而漏掉 key/field 参数错误。
func TestHashLifecycle(t *testing.T) {
	// Arrange：准备两个可比较对象及其稳定 JSON，后续命令和返回结果都复用这些值。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hash := NewHash[unitUser](redis, "users")
	alice := unitUser{ID: 1, Name: "Alice"}
	bob := unitUser{ID: 2, Name: "Bob"}
	aliceJSON := `{"id":1,"name":"Alice"}`
	bobJSON := `{"id":2,"name":"Bob"}`

	if hash.Key() != "app:users" {
		t.Fatalf("unexpected key %q", hash.Key())
	}

	// 按调用顺序声明完整 Redis 命令，既验证方法结果，也验证 Builder 参数组合。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("HSET", "app:users", "1", aliceJSON)).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("HSETNX", "app:users", "2", bobJSON)).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("HSET", "app:users", "2", bobJSON)).Return(rueidismock.Result(rueidismock.RedisInt64(0))),
		client.EXPECT().Do(ctx, rueidismock.Match("HGET", "app:users", "1")).Return(rueidismock.Result(rueidismock.RedisBlobString(aliceJSON))),
		client.EXPECT().Do(ctx, rueidismock.Match("HMGET", "app:users", "1", "missing")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString(aliceJSON), rueidismock.RedisNil()))),
		client.EXPECT().Do(ctx, rueidismock.Match("HGETALL", "app:users")).Return(rueidismock.Result(rueidismock.RedisMap(map[string]rueidis.RedisMessage{
			"1": rueidismock.RedisBlobString(aliceJSON),
			"2": rueidismock.RedisBlobString(bobJSON),
		}))),
		client.EXPECT().Do(ctx, rueidismock.Match("HKEYS", "app:users")).Return(rueidismock.Result(redisStrings("1", "2"))),
		client.EXPECT().Do(ctx, rueidismock.Match("HVALS", "app:users")).Return(rueidismock.Result(redisStrings(aliceJSON, bobJSON))),
		client.EXPECT().Do(ctx, rueidismock.Match("HSCAN", "app:users", "0", "COUNT", "100")).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString("0"),
			redisStrings("1", aliceJSON),
		))),
		client.EXPECT().Do(ctx, rueidismock.Match("HINCRBY", "app:users", "number", "2")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("HINCRBYFLOAT", "app:users", "ratio", "1.5")).Return(rueidismock.Result(rueidismock.RedisBlobString("1.5"))),
		client.EXPECT().Do(ctx, rueidismock.Match("HPEXPIRE", "app:users", "1", "FIELDS", "1", "1")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1)))),
		client.EXPECT().Do(ctx, rueidismock.Match("HPTTL", "app:users", "FIELDS", "1", "1")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1500)))),
		client.EXPECT().Do(ctx, rueidismock.Match("HPERSIST", "app:users", "FIELDS", "1", "1")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1)))),
		client.EXPECT().Do(ctx, rueidismock.Match("HEXISTS", "app:users", "1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("HDEL", "app:users", "1", "2")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("HLEN", "app:users")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:users")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 写入阶段：覆盖 Set、SetNX 和 SetMany 三种入口。
	setErr := hash.Set(ctx, "1", alice)
	if setErr != nil {
		t.Fatal(setErr)
	}
	setNXStored, setNXErr := hash.SetNX(ctx, "2", bob)
	if setNXErr != nil || !setNXStored {
		t.Fatalf("SetNX stored=%t err=%v", setNXStored, setNXErr)
	}
	setManyErr := hash.SetMany(ctx, map[string]unitUser{"2": bob})
	if setManyErr != nil {
		t.Fatal(setManyErr)
	}
	// 读取阶段：验证单 field、批量 field、全量 key/value 及 HSCAN 解码。
	getValue, getErr := hash.Get(ctx, "1")
	if getErr != nil || getValue != alice {
		t.Fatalf("Get value=%+v err=%v", getValue, getErr)
	}
	getManyValues, getManyErr := hash.GetMany(ctx, "1", "missing")
	if getManyErr != nil || len(getManyValues) != 1 || getManyValues["1"] != alice {
		t.Fatalf("GetMany values=%+v err=%v", getManyValues, getManyErr)
	}
	allValues, allErr := hash.All(ctx)
	if allErr != nil || len(allValues) != 2 || allValues["2"] != bob {
		t.Fatalf("All values=%+v err=%v", allValues, allErr)
	}
	hashKeys, keysErr := hash.Keys(ctx)
	if keysErr != nil || len(hashKeys) != 2 {
		t.Fatalf("Keys values=%+v err=%v", hashKeys, keysErr)
	}
	hashValues, valuesErr := hash.Values(ctx)
	if valuesErr != nil || len(hashValues) != 2 || hashValues[0] != alice {
		t.Fatalf("Values values=%+v err=%v", hashValues, valuesErr)
	}
	scanResult, scanErr := hash.Scan(ctx, 0, "", 0)
	if scanErr != nil || scanResult.Cursor != 0 || scanResult.Values["1"] != alice {
		t.Fatalf("Scan value=%+v err=%v", scanResult, scanErr)
	}
	// 原子数值阶段：整数与浮点数分别使用 Redis 原生命令，不能走 JSON 读改写。
	integerValue, incrementIntErr := hash.IncrementInt(ctx, "number", 2)
	if incrementIntErr != nil || integerValue != 2 {
		t.Fatalf("IncrementInt value=%d err=%v", integerValue, incrementIntErr)
	}
	floatValue, incrementFloatErr := hash.IncrementFloat(ctx, "ratio", 1.5)
	if incrementFloatErr != nil || floatValue != 1.5 {
		t.Fatalf("IncrementFloat value=%f err=%v", floatValue, incrementFloatErr)
	}
	// field TTL 阶段：验证最小毫秒精度、剩余 TTL 解析和持久化取消过期。
	fieldExpired, expireFieldErr := hash.ExpireField(ctx, "1", time.Nanosecond)
	if expireFieldErr != nil || !fieldExpired {
		t.Fatalf("ExpireField value=%t err=%v", fieldExpired, expireFieldErr)
	}
	ttlFieldValue, ttlFieldErr := hash.TTLField(ctx, "1")
	if ttlFieldErr != nil || ttlFieldValue != 1500*time.Millisecond {
		t.Fatalf("TTLField value=%s err=%v", ttlFieldValue, ttlFieldErr)
	}
	fieldPersisted, persistFieldErr := hash.PersistField(ctx, "1")
	if persistFieldErr != nil || !fieldPersisted {
		t.Fatalf("PersistField value=%t err=%v", fieldPersisted, persistFieldErr)
	}
	// 清理阶段：验证存在性、批量 field 删除、长度和整个 Hash 删除。
	exists, existsErr := hash.Exists(ctx, "1")
	if existsErr != nil || !exists {
		t.Fatalf("Exists value=%t err=%v", exists, existsErr)
	}
	deletedCount, deleteErr := hash.Delete(ctx, "1", "2")
	if deleteErr != nil || deletedCount != 2 {
		t.Fatalf("Delete value=%d err=%v", deletedCount, deleteErr)
	}
	hashLength, lenErr := hash.Len(ctx)
	if lenErr != nil || hashLength != 2 {
		t.Fatalf("Len value=%d err=%v", hashLength, lenErr)
	}
	clearErr := hash.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestHashScanPattern 验证非零游标、MATCH 和 COUNT 会按正确顺序生成 HSCAN，
// 并正确识别返回游标 0 所表示的扫描结束状态。
func TestHashScanPattern(t *testing.T) {
	// Arrange：模拟从游标 4 继续扫描且本批没有匹配结果。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hash := NewHash[unitUser](redis, "users")
	client.EXPECT().Do(ctx, rueidismock.Match("HSCAN", "app:users", "4", "MATCH", "user:*", "COUNT", "5")).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisBlobString("0"),
		redisStrings(),
	)))
	// Act/Assert：必须带上 MATCH/COUNT，并把返回游标 0 解释为扫描完成。
	scanResult, scanErr := hash.Scan(ctx, 4, "user:*", 5)
	if scanErr != nil || scanResult.Cursor != 0 || len(scanResult.Values) != 0 {
		t.Fatalf("Scan value=%+v err=%v", scanResult, scanErr)
	}
}

// TestHashSetManyUsesStableFieldOrder 验证 map 输入会按 field 字典序生成 HSET 参数，
// 使日志、链路追踪和严格命令测试不会随着 Go map 的随机遍历顺序发生变化。
func TestHashSetManyUsesStableFieldOrder(t *testing.T) {
	// Arrange：故意按 z、a 的可读顺序构造 map，期望命令始终输出 a、z。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hash := NewHash[unitUser](redis, "users")
	alice := unitUser{ID: 1, Name: "Alice"}
	bob := unitUser{ID: 2, Name: "Bob"}
	client.EXPECT().Do(ctx, rueidismock.Match(
		"HSET", "app:users",
		"a", `{"id":1,"name":"Alice"}`,
		"z", `{"id":2,"name":"Bob"}`,
	)).Return(rueidismock.Result(rueidismock.RedisInt64(2)))

	// Act/Assert：一次批量写入即可完成，gomock 会精确验证排序后的 field/value 顺序。
	err := hash.SetMany(ctx, map[string]unitUser{"z": bob, "a": alice})
	if err != nil {
		t.Fatal(err)
	}
}

// TestHashEmptyAndValidationBranches 验证空批量操作不会访问 Redis，
// 同时检查非法 TTL 和三种写入入口的 JSON 编码失败都能尽早返回。
func TestHashEmptyAndValidationBranches(t *testing.T) {
	// 空批量 API 使用没有客户端的 Hash，证明这些路径不会触发 Redis 调用。
	hash := &Hash[unitUser]{}
	ctx := context.Background()
	setManyErr := hash.SetMany(ctx, nil)
	if setManyErr != nil {
		t.Fatal(setManyErr)
	}
	getManyValues, getManyErr := hash.GetMany(ctx)
	if getManyErr != nil || len(getManyValues) != 0 {
		t.Fatalf("GetMany values=%+v err=%v", getManyValues, getManyErr)
	}
	deletedCount, deleteErr := hash.Delete(ctx)
	if deleteErr != nil || deletedCount != 0 {
		t.Fatalf("Delete value=%d err=%v", deletedCount, deleteErr)
	}
	// field TTL 非正时应在构造 HPEXPIRE 前返回参数错误。
	_, expireFieldErr := hash.ExpireField(ctx, "field", 0)
	if !errors.Is(expireFieldErr, ErrInvalidExpiration) {
		t.Fatalf("got %v want %v", expireFieldErr, ErrInvalidExpiration)
	}

	// 使用 chan 作为无法序列化的值，覆盖单写、条件写和批量写的编码错误。
	badRedis, badClient := newMockRedis(t)
	badHash := NewHash[chan int](badRedis, "bad")
	setErr := badHash.Set(ctx, "field", make(chan int))
	if setErr == nil {
		t.Fatal("Set should reject values unsupported by JSON")
	}
	_, setNXErr := badHash.SetNX(ctx, "field", make(chan int))
	if setNXErr == nil {
		t.Fatal("SetNX should reject values unsupported by JSON")
	}
	setManyErr2 := badHash.SetMany(ctx, map[string]chan int{"field": make(chan int)})
	if setManyErr2 == nil {
		t.Fatal("SetMany should reject values unsupported by JSON")
	}

	// HGET 的 JSON 即使已经写入前置字段，只要后续字段解码失败，也必须返回完整零值。
	dirtyHash := NewHash[unitUser](badRedis, "dirty")
	badClient.EXPECT().Do(ctx, rueidismock.Match("HGET", "app:dirty", "field")).Return(rueidismock.Result(rueidismock.RedisBlobString(`{"id":99,"name":[]}`)))
	invalidValue, getErr := dirtyHash.Get(ctx, "field")
	if getErr == nil || invalidValue != (unitUser{}) {
		t.Fatalf("Get value=%+v err=%v; want zero value and decode error", invalidValue, getErr)
	}
}

// TestSingleIntegerResultRejectsUnexpectedLength 验证 HPTTL/HPEXPIRE 等单 field 命令
// 若返回值数量异常会显式报错，防止把协议或版本异常误判成 false。
func TestSingleIntegerResultRejectsUnexpectedLength(t *testing.T) {
	// 构造空数组模拟服务端没有返回约定的单 field 状态码。
	_, singleIntegerResultErr := singleIntegerResult(rueidismock.Result(rueidismock.RedisArray()))
	if singleIntegerResultErr == nil {
		t.Fatal("expected response length error")
	}
}
