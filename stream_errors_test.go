package rueidistpl

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
)

// TestStreamInfoFieldParsersRejectMalformedValues 验证 XINFO 的必填和可选字段解析器
// 能区分缺少字段、错误 RESP 类型、Redis Nil 和合法整数。
func TestStreamInfoFieldParsersRejectMalformedValues(t *testing.T) {
	// Arrange：构造缺失字段、数组类型字符串、非法整数字符串、Nil 和合法整数五类输入。
	emptyFields := map[string]rueidis.RedisMessage{}
	invalidStringFields := map[string]rueidis.RedisMessage{"name": rueidismock.RedisError("ERR invalid name")}
	invalidIntegerFields := map[string]rueidis.RedisMessage{"pending": rueidismock.RedisBlobString("not-a-number")}
	nilFields := map[string]rueidis.RedisMessage{"lag": rueidismock.RedisNil()}
	validFields := map[string]rueidis.RedisMessage{"lag": rueidismock.RedisInt64(7)}

	// Act：分别保存各解析结果和错误，断点时能直接查看字段的三态返回。
	missingString, missingStringErr := streamInfoString(emptyFields, "name")
	invalidString, invalidStringErr := streamInfoString(invalidStringFields, "name")
	missingInteger, missingIntegerErr := streamInfoInt64(emptyFields, "pending")
	invalidInteger, invalidIntegerErr := streamInfoInt64(invalidIntegerFields, "pending")
	nilValue, nilKnown, nilErr := streamInfoOptionalInt64(nilFields, "lag")
	validValue, validKnown, validErr := streamInfoOptionalInt64(validFields, "lag")
	invalidOptionalValue, invalidOptionalKnown, invalidOptionalErr := streamInfoOptionalInt64(invalidIntegerFields, "pending")

	// Assert：必填字段缺失或类型错误必须失败，返回数据保持零值。
	if missingString != "" || missingStringErr == nil || invalidString != "" || invalidStringErr == nil {
		t.Fatalf("string missing=(%q,%v) invalid=(%q,%v)", missingString, missingStringErr, invalidString, invalidStringErr)
	}
	if missingInteger != 0 || missingIntegerErr == nil || invalidInteger != 0 || invalidIntegerErr == nil {
		t.Fatalf("integer missing=(%d,%v) invalid=(%d,%v)", missingInteger, missingIntegerErr, invalidInteger, invalidIntegerErr)
	}
	// 可选字段的 Nil 表示 unknown，不是错误；合法整数则必须同时返回 known=true。
	if nilValue != 0 || nilKnown || nilErr != nil {
		t.Fatalf("optional nil value=%d known=%t err=%v", nilValue, nilKnown, nilErr)
	}
	if validValue != 7 || !validKnown || validErr != nil {
		t.Fatalf("optional valid value=%d known=%t err=%v", validValue, validKnown, validErr)
	}
	if invalidOptionalValue != 0 || invalidOptionalKnown || invalidOptionalErr == nil {
		t.Fatalf("optional invalid value=%d known=%t err=%v", invalidOptionalValue, invalidOptionalKnown, invalidOptionalErr)
	}
}

// TestStreamReadRedisError 验证 XREAD 的普通 Redis 错误不会被误当成“暂时没有消息”的 Nil。
func TestStreamReadRedisError(t *testing.T) {
	// Arrange：让 XREAD 返回可识别的节点错误。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	wantErr := errors.New("stream node failed")
	client.EXPECT().Do(ctx, rueidismock.Match("XREAD", "COUNT", "100", "STREAMS", "app:events", "0")).Return(rueidismock.ErrorResult(wantErr))

	// Act：执行默认 count 的非阻塞读取。
	entries, err := stream.Read(ctx, "0", 0)

	// Assert：错误时切片为 nil，底层错误仍可用 errors.Is 判断。
	if entries != nil || !errors.Is(err, wantErr) {
		t.Fatalf("Read entries=%+v err=%v", entries, err)
	}
}

// TestStreamAutoClaimDecodeErrors 验证 XAUTOCLAIM 每一层异常响应都会返回空结果：
// 顶层数组、下一游标、消息数组、消息 JSON 和 Redis 7 删除 ID 数组均不能静默失败。
func TestStreamAutoClaimDecodeErrors(t *testing.T) {
	// Arrange：五次相同命令依次在不同解析层级失败，便于定位具体协议分支。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	wantErr := errors.New("autoclaim failed")
	command := rueidismock.Match("XAUTOCLAIM", "app:events", "workers", "worker-1", "0", "0-0", "COUNT", "10")
	client.EXPECT().Do(ctx, command).Return(rueidismock.ErrorResult(wantErr))
	client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisError("ERR invalid cursor"),
		rueidismock.RedisArray(),
	)))
	client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisBlobString("0-0"),
		rueidismock.RedisError("ERR invalid entries"),
	)))
	client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisBlobString("0-0"),
		rueidismock.RedisArray(redisStreamEntry("1-0", `{"id":99,"name":[]}`)),
	)))
	client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisBlobString("0-0"),
		rueidismock.RedisArray(),
		rueidismock.RedisError("ERR invalid deleted ids"),
	)))

	// Act：逐次执行并将结果与错误放入明确命名的变量，方便断点定位失败层级。
	transportResult, transportErr := stream.AutoClaim(ctx, "workers", "worker-1", 0, "0-0", 10)
	cursorResult, cursorErr := stream.AutoClaim(ctx, "workers", "worker-1", 0, "0-0", 10)
	entriesResult, entriesErr := stream.AutoClaim(ctx, "workers", "worker-1", 0, "0-0", 10)
	jsonResult, jsonErr := stream.AutoClaim(ctx, "workers", "worker-1", 0, "0-0", 10)
	deletedIDsResult, deletedIDsErr := stream.AutoClaim(ctx, "workers", "worker-1", 0, "0-0", 10)

	// Assert：顶层传输错误必须保留原错误，后续四种协议错误都必须清空半成品。
	if transportResult.NextStart != "" || transportResult.Entries != nil || transportResult.DeletedIDs != nil || !errors.Is(transportErr, wantErr) {
		t.Fatalf("transport result=%+v err=%v", transportResult, transportErr)
	}
	if cursorResult.NextStart != "" || cursorResult.Entries != nil || cursorResult.DeletedIDs != nil || cursorErr == nil {
		t.Fatalf("cursor result=%+v err=%v", cursorResult, cursorErr)
	}
	if entriesResult.NextStart != "" || entriesResult.Entries != nil || entriesResult.DeletedIDs != nil || entriesErr == nil {
		t.Fatalf("entries result=%+v err=%v", entriesResult, entriesErr)
	}
	if jsonResult.NextStart != "" || jsonResult.Entries != nil || jsonResult.DeletedIDs != nil || jsonErr == nil {
		t.Fatalf("json result=%+v err=%v", jsonResult, jsonErr)
	}
	if deletedIDsResult.NextStart != "" || deletedIDsResult.Entries != nil || deletedIDsResult.DeletedIDs != nil || deletedIDsErr == nil {
		t.Fatalf("deleted IDs result=%+v err=%v", deletedIDsResult, deletedIDsErr)
	}
}

// TestPendingDecodeErrors 验证 XPENDING 的顶层错误、非数组行、字段数量异常和字段类型错误
// 都返回 nil 结果，避免消费组恢复逻辑使用半解析的 Pending 数据。
func TestPendingDecodeErrors(t *testing.T) {
	// Arrange：四次相同 XPENDING 依次模拟不同层级的异常响应。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	wantErr := errors.New("pending failed")
	command := rueidismock.Match("XPENDING", "app:events", "workers", "-", "+", "10")
	client.EXPECT().Do(ctx, command).Return(rueidismock.ErrorResult(wantErr))
	client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("not-a-row"))))
	client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisArray(
		rueidismock.RedisBlobString("1-0"),
	))))
	client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisArray(
		rueidismock.RedisError("ERR invalid id"),
		rueidismock.RedisBlobString("worker-1"),
		rueidismock.RedisInt64(10),
		rueidismock.RedisInt64(1),
	))))

	// Act：保存每一次调用的结果和错误，确保错误路径没有被后续调用覆盖。
	transportEntries, transportErr := stream.Pending(ctx, "workers", 10)
	rowEntries, rowErr := stream.Pending(ctx, "workers", 10)
	lengthEntries, lengthErr := stream.Pending(ctx, "workers", 10)
	typeEntries, typeErr := stream.Pending(ctx, "workers", 10)

	// Assert：四类错误都必须返回 nil 切片；传输错误另外保留 errors.Is 能力。
	if transportEntries != nil || !errors.Is(transportErr, wantErr) {
		t.Fatalf("transport entries=%+v err=%v", transportEntries, transportErr)
	}
	if rowEntries != nil || rowErr == nil {
		t.Fatalf("row entries=%+v err=%v", rowEntries, rowErr)
	}
	if lengthEntries != nil || lengthErr == nil {
		t.Fatalf("length entries=%+v err=%v", lengthEntries, lengthErr)
	}
	if typeEntries != nil || typeErr == nil {
		t.Fatalf("type entries=%+v err=%v", typeEntries, typeErr)
	}
}
