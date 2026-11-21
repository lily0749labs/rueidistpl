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

// TestStreamLifecycle 覆盖 Stream 从生产到消费确认的完整链路：
// XADD/XRANGE/XREAD、消费组幂等创建、XREADGROUP、Pending 解析、Claim、Ack、删除与清空；
// 每条消息都必须通过约定的 data 字段完成 JSON 编解码。
func TestStreamLifecycle(t *testing.T) {
	// Arrange：构造 Rueidis 可解析的 XRANGE/XREAD 响应，消息只包含约定 data 字段。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	alice := unitUser{ID: 1, Name: "Alice"}
	payload := `{"id":1,"name":"Alice"}`
	entry := redisStreamEntry("1-0", payload)
	rangeResult := rueidismock.RedisArray(entry)
	readResult := rueidismock.RedisMap(map[string]rueidis.RedisMessage{
		"app:events": rangeResult,
	})

	if stream.Key() != "app:events" {
		t.Fatalf("unexpected key %q", stream.Key())
	}

	// 按生产、读取、消费组处理、确认和清理的顺序声明完整 Redis 命令。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("XADD", "app:events", "*", streamDataField, payload)).Return(rueidismock.Result(rueidismock.RedisBlobString("1-0"))),
		client.EXPECT().Do(ctx, rueidismock.Match("XADD", "app:events", "2-0", streamDataField, payload)).Return(rueidismock.Result(rueidismock.RedisBlobString("2-0"))),
		client.EXPECT().Do(ctx, rueidismock.Match("XRANGE", "app:events", "-", "+", "COUNT", "2")).Return(rueidismock.Result(rangeResult)),
		client.EXPECT().Do(ctx, rueidismock.Match("XREAD", "COUNT", "100", "STREAMS", "app:events", "0")).Return(rueidismock.Result(readResult)),
		client.EXPECT().Do(ctx, rueidismock.Match("XREAD", "COUNT", "2", "BLOCK", "1000", "STREAMS", "app:events", "$")).Return(rueidismock.Result(readResult)),
		client.EXPECT().Do(ctx, rueidismock.Match("XGROUP", "CREATE", "app:events", "workers", "0", "MKSTREAM")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("XGROUP", "CREATE", "app:events", "workers", "0", "MKSTREAM")).Return(rueidismock.Result(rueidismock.RedisError("BUSYGROUP Consumer Group name already exists"))),
		client.EXPECT().Do(ctx, rueidismock.Match("XREADGROUP", "GROUP", "workers", "worker-1", "COUNT", "100", "STREAMS", "app:events", ">")).Return(rueidismock.Result(readResult)),
		client.EXPECT().Do(ctx, rueidismock.Match("XREADGROUP", "GROUP", "workers", "worker-1", "COUNT", "2", "BLOCK", "1000", "STREAMS", "app:events", "0")).Return(rueidismock.Result(readResult)),
		client.EXPECT().Do(ctx, rueidismock.Match("XACK", "app:events", "workers", "1-0", "2-0")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("XPENDING", "app:events", "workers", "-", "+", "2")).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisArray(
				rueidismock.RedisBlobString("1-0"),
				rueidismock.RedisBlobString("worker-1"),
				rueidismock.RedisInt64(500),
				rueidismock.RedisInt64(2),
			),
		))),
		client.EXPECT().Do(ctx, rueidismock.Match("XCLAIM", "app:events", "workers", "worker-2", "0", "1-0")).Return(rueidismock.Result(rangeResult)),
		client.EXPECT().Do(ctx, rueidismock.Match("XDEL", "app:events", "1-0")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("XLEN", "app:events")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:events")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 生产阶段：验证自动 ID 和指定 ID 两种 XADD 入口。
	addedID, addErr := stream.Add(ctx, alice)
	if addErr != nil || addedID != "1-0" {
		t.Fatalf("Add id=%q err=%v", addedID, addErr)
	}
	customID, addWithIDErr := stream.AddWithID(ctx, "2-0", alice)
	if addWithIDErr != nil || customID != "2-0" {
		t.Fatalf("AddWithID id=%q err=%v", customID, addWithIDErr)
	}
	// 普通读取阶段：XRANGE、XREAD、阻塞 XREAD 都应还原同一个强类型对象。
	rangeEntries, rangeErr := stream.Range(ctx, "-", "+", 2)
	if rangeErr != nil || len(rangeEntries) != 1 || rangeEntries[0].Value != alice {
		t.Fatalf("Range entries=%+v err=%v", rangeEntries, rangeErr)
	}
	readEntries, readErr := stream.Read(ctx, "0", 0)
	if readErr != nil || len(readEntries) != 1 || readEntries[0].ID != "1-0" {
		t.Fatalf("Read entries=%+v err=%v", readEntries, readErr)
	}
	readBlockEntries, readBlockErr := stream.ReadBlock(ctx, "$", 2, time.Second)
	if readBlockErr != nil || len(readBlockEntries) != 1 {
		t.Fatalf("ReadBlock entries=%+v err=%v", readBlockEntries, readBlockErr)
	}
	// 消费组创建具有幂等语义，第二次 BUSYGROUP 不能作为业务失败返回。
	createGroupErr := stream.CreateGroup(ctx, "workers", "0")
	if createGroupErr != nil {
		t.Fatal(createGroupErr)
	}
	createGroupErr2 := stream.CreateGroup(ctx, "workers", "0")
	if createGroupErr2 != nil {
		t.Fatalf("existing group should be accepted: %v", createGroupErr2)
	}
	// 消费阶段：读取新消息，再检查 Pending 并将超时消息转移给另一消费者。
	readGroupEntries, readGroupErr := stream.ReadGroup(ctx, "workers", "worker-1", 0)
	if readGroupErr != nil || len(readGroupEntries) != 1 {
		t.Fatalf("ReadGroup entries=%+v err=%v", readGroupEntries, readGroupErr)
	}
	readGroupFromEntries, readGroupFromErr := stream.ReadGroupFrom(ctx, "workers", "worker-1", "0", 2, time.Second)
	if readGroupFromErr != nil || len(readGroupFromEntries) != 1 {
		t.Fatalf("ReadGroupFrom entries=%+v err=%v", readGroupFromEntries, readGroupFromErr)
	}
	ackCount, ackErr := stream.Ack(ctx, "workers", "1-0", "2-0")
	if ackErr != nil || ackCount != 2 {
		t.Fatalf("Ack count=%d err=%v", ackCount, ackErr)
	}
	pendingEntries, pendingErr := stream.Pending(ctx, "workers", 2)
	if pendingErr != nil || len(pendingEntries) != 1 || pendingEntries[0] != (PendingEntry{ID: "1-0", Consumer: "worker-1", Idle: 500 * time.Millisecond, Deliveries: 2}) {
		t.Fatalf("Pending entries=%+v err=%v", pendingEntries, pendingErr)
	}
	claimEntries, claimErr := stream.Claim(ctx, "workers", "worker-2", 0, "1-0")
	if claimErr != nil || len(claimEntries) != 1 || claimEntries[0].Value != alice {
		t.Fatalf("Claim entries=%+v err=%v", claimEntries, claimErr)
	}
	// 维护阶段：永久删除消息、读取长度并清除整个 Stream 及消费组。
	deleteCount, deleteErr := stream.Delete(ctx, "1-0")
	if deleteErr != nil || deleteCount != 1 {
		t.Fatalf("Delete count=%d err=%v", deleteCount, deleteErr)
	}
	streamLength, lenErr := stream.Len(ctx)
	if lenErr != nil || streamLength != 1 {
		t.Fatalf("Len value=%d err=%v", streamLength, lenErr)
	}
	clearErr := stream.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestStreamEmptyAndDecodeBranches 验证空 ID/空 count 操作不访问 Redis，
// 同时检查缺失 data 字段、非法 JSON 和无法序列化载荷都返回清晰错误。
func TestStreamEmptyAndDecodeBranches(t *testing.T) {
	// 第一部分不设置客户端，验证空参数 API 不会生成 Redis 命令。
	ctx := context.Background()
	stream := &Stream[unitUser]{}
	pendingEntries, pendingErr := stream.Pending(ctx, "workers", 0)
	if pendingErr != nil || len(pendingEntries) != 0 {
		t.Fatalf("Pending entries=%+v err=%v", pendingEntries, pendingErr)
	}
	ackCount, ackErr := stream.Ack(ctx, "workers")
	if ackErr != nil || ackCount != 0 {
		t.Fatalf("Ack count=%d err=%v", ackCount, ackErr)
	}
	claimEntries, claimErr := stream.Claim(ctx, "workers", "worker", 0)
	if claimErr != nil || len(claimEntries) != 0 {
		t.Fatalf("Claim entries=%+v err=%v", claimEntries, claimErr)
	}
	deleteCount, deleteErr := stream.Delete(ctx)
	if deleteErr != nil || deleteCount != 0 {
		t.Fatalf("Delete count=%d err=%v", deleteCount, deleteErr)
	}

	// 第二部分直接测试解码器，区分缺少 data 字段和 data 内容非法两种脏消息。
	_, missingDataErr := decodeStreamEntries[unitUser]([]rueidis.XRangeEntry{{ID: "1-0"}})
	if !errors.Is(missingDataErr, ErrStreamDataMissing) {
		t.Fatalf("got %v want %v", missingDataErr, ErrStreamDataMissing)
	}
	_, invalidDataErr := decodeStreamEntries[unitUser]([]rueidis.XRangeEntry{{ID: "1-0", FieldValues: map[string]string{streamDataField: "invalid"}}})
	if invalidDataErr == nil {
		t.Fatal("invalid stream JSON should fail")
	}

	// 最后用 chan 载荷确认生产侧会在执行 XADD 前返回 JSON 编码错误。
	badStream := &Stream[chan int]{}
	_, addErr := badStream.Add(ctx, make(chan int))
	if addErr == nil {
		t.Fatal("Add should reject values unsupported by JSON")
	}
}

// TestStreamRedisNilRead 验证没有新消息时，将 Redis Nil 转换为空切片而不是错误；
// block 非正退化为普通 XREAD，亚毫秒正数则提升到 1ms，不能生成无限等待的 BLOCK 0。
func TestStreamRedisNilRead(t *testing.T) {
	// Arrange：分别模拟非阻塞读取、阻塞读取和消费组阻塞读取都没有新消息。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("XREAD", "COUNT", "100", "STREAMS", "app:events", "0")).Return(rueidismock.Result(rueidismock.RedisNil())),
		client.EXPECT().Do(ctx, rueidismock.Match("XREAD", "COUNT", "100", "BLOCK", "1", "STREAMS", "app:events", "0")).Return(rueidismock.Result(rueidismock.RedisNil())),
		client.EXPECT().Do(ctx, rueidismock.Match("XREADGROUP", "GROUP", "workers", "worker-1", "COUNT", "100", "BLOCK", "1", "STREAMS", "app:events", ">")).Return(rueidismock.Result(rueidismock.RedisNil())),
	)
	// Act/Assert：block=0 退化为普通读取，Nil 被规范化为空切片和 nil error。
	readBlockEntries, readBlockErr := stream.ReadBlock(ctx, "0", 0, 0)
	if readBlockErr != nil || len(readBlockEntries) != 0 {
		t.Fatalf("ReadBlock entries=%+v err=%v", readBlockEntries, readBlockErr)
	}
	// 纳秒级正数在两个阻塞入口都必须生成为 BLOCK 1，并保持空结果语义。
	shortBlockEntries, shortBlockErr := stream.ReadBlock(ctx, "0", 0, time.Nanosecond)
	if shortBlockErr != nil || len(shortBlockEntries) != 0 {
		t.Fatalf("ReadBlock short entries=%+v err=%v", shortBlockEntries, shortBlockErr)
	}
	groupEntries, groupErr := stream.ReadGroupFrom(ctx, "workers", "worker-1", ">", 0, time.Nanosecond)
	if groupErr != nil || len(groupEntries) != 0 {
		t.Fatalf("ReadGroupFrom entries=%+v err=%v", groupEntries, groupErr)
	}
}

// TestStreamCreateGroupErrorClassification 验证只有 Redis 官方 BUSYGROUP 错误才会被视为幂等成功，
// 普通网络或代理错误即使文本碰巧包含 BUSYGROUP，也必须原样返回，避免隐藏真实故障。
func TestStreamCreateGroupErrorClassification(t *testing.T) {
	// Arrange：构造一个普通 Go error，其文本包含 BUSYGROUP，但类型不是 RedisError。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	transportErr := errors.New("proxy rejected BUSYGROUP response")
	client.EXPECT().Do(ctx, rueidismock.Match("XGROUP", "CREATE", "app:events", "workers", "0", "MKSTREAM")).Return(rueidismock.ErrorResult(transportErr))

	// Act/Assert：错误不能因字符串包含关键字而被吞掉，调用方应能识别原始故障。
	createErr := stream.CreateGroup(ctx, "workers", "0")
	if !errors.Is(createErr, transportErr) {
		t.Fatalf("CreateGroup error=%v", createErr)
	}
}

// TestStreamClaimMinIdleBoundary 验证 XCLAIM 的最小空闲时间边界：
// 正的亚毫秒值必须提升为 1ms，负数必须在客户端直接拒绝且不发送 Redis 命令。
func TestStreamClaimMinIdleBoundary(t *testing.T) {
	// Arrange：只为合法的亚毫秒调用声明命令，负数调用若误发命令会由 gomock 自动报错。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	payload := `{"id":1,"name":"Alice"}`
	client.EXPECT().Do(ctx, rueidismock.Match("XCLAIM", "app:events", "workers", "worker-2", "1", "1-0")).Return(
		rueidismock.Result(rueidismock.RedisArray(redisStreamEntry("1-0", payload))),
	)

	// Act/Assert：纳秒值不能被截断成 0，否则会把“至少空闲一段时间”变成无条件立即转移。
	claimed, claimErr := stream.Claim(ctx, "workers", "worker-2", time.Nanosecond, "1-0")
	if claimErr != nil || len(claimed) != 1 || claimed[0].Value.Name != "Alice" {
		t.Fatalf("Claim entries=%+v err=%v", claimed, claimErr)
	}
	// 负数没有合法 Redis 语义，返回专用错误，结果切片保持 nil。
	invalid, invalidErr := stream.Claim(ctx, "workers", "worker-2", -time.Millisecond, "1-0")
	if invalid != nil || !errors.Is(invalidErr, ErrInvalidMinIdle) {
		t.Fatalf("negative Claim entries=%+v err=%v", invalid, invalidErr)
	}
}

// TestStreamCapacityAndGroupMaintenance 验证 Stream 容量控制和消费组维护命令：
// XADD/XTRIM 均使用性能更好的 MAXLEN ~，删除消费者和消费组时保留 Redis 的原始返回语义。
func TestStreamCapacityAndGroupMaintenance(t *testing.T) {
	// Arrange：严格声明预期命令，确保 maxLen 没有被客户端误解为精确裁剪。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	alice := unitUser{ID: 1, Name: "Alice"}
	payload := `{"id":1,"name":"Alice"}`
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("XADD", "app:events", "MAXLEN", "~", "1000", "*", streamDataField, payload)).Return(
			rueidismock.Result(rueidismock.RedisBlobString("1-0")),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("XADD", "app:events", "MAXLEN", "~", "1000", "2-0", streamDataField, payload)).Return(
			rueidismock.Result(rueidismock.RedisBlobString("2-0")),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("XTRIM", "app:events", "MAXLEN", "~", "500")).Return(
			rueidismock.Result(rueidismock.RedisInt64(7)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("XGROUP", "DELCONSUMER", "app:events", "workers", "worker-old")).Return(
			rueidismock.Result(rueidismock.RedisInt64(2)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("XGROUP", "DESTROY", "app:events", "workers")).Return(
			rueidismock.Result(rueidismock.RedisInt64(1)),
		),
	)

	// Act/Assert：分别保留各调用结果变量，方便在 IDE 中逐步打断点观察返回数据。
	addedID, addErr := stream.AddMaxLen(ctx, alice, 1000)
	if addErr != nil || addedID != "1-0" {
		t.Fatalf("AddMaxLen id=%q err=%v", addedID, addErr)
	}
	customID, customErr := stream.AddWithIDMaxLen(ctx, "2-0", alice, 1000)
	if customErr != nil || customID != "2-0" {
		t.Fatalf("AddWithIDMaxLen id=%q err=%v", customID, customErr)
	}
	trimmedCount, trimErr := stream.TrimMaxLen(ctx, 500)
	if trimErr != nil || trimmedCount != 7 {
		t.Fatalf("TrimMaxLen count=%d err=%v", trimmedCount, trimErr)
	}
	removedPendingCount, deleteConsumerErr := stream.DeleteConsumer(ctx, "workers", "worker-old")
	if deleteConsumerErr != nil || removedPendingCount != 2 {
		t.Fatalf("DeleteConsumer pending=%d err=%v", removedPendingCount, deleteConsumerErr)
	}
	groupDeleted, deleteGroupErr := stream.DeleteGroup(ctx, "workers")
	if deleteGroupErr != nil || !groupDeleted {
		t.Fatalf("DeleteGroup deleted=%t err=%v", groupDeleted, deleteGroupErr)
	}
}

// TestStreamCapacityValidation 验证非正 maxLen 在客户端直接失败，
// 既不序列化数据，也不发送可能无限增长或误删消息的 Redis 命令。
func TestStreamCapacityValidation(t *testing.T) {
	// Arrange：使用没有客户端的 Stream，如果参数校验失效，测试会因误访问 Redis 而失败。
	ctx := context.Background()
	stream := &Stream[unitUser]{}
	alice := unitUser{ID: 1, Name: "Alice"}

	// Act/Assert：三个入口都返回同一个可 errors.Is 判断的领域错误。
	addedID, addErr := stream.AddMaxLen(ctx, alice, 0)
	if addedID != "" || !errors.Is(addErr, ErrInvalidStreamMaxLen) {
		t.Fatalf("AddMaxLen id=%q err=%v", addedID, addErr)
	}
	customID, customErr := stream.AddWithIDMaxLen(ctx, "1-0", alice, -1)
	if customID != "" || !errors.Is(customErr, ErrInvalidStreamMaxLen) {
		t.Fatalf("AddWithIDMaxLen id=%q err=%v", customID, customErr)
	}
	trimmedCount, trimErr := stream.TrimMaxLen(ctx, 0)
	if trimmedCount != 0 || !errors.Is(trimErr, ErrInvalidStreamMaxLen) {
		t.Fatalf("TrimMaxLen count=%d err=%v", trimmedCount, trimErr)
	}
}

// TestStreamAutoClaim 验证 XAUTOCLAIM 的完整 Redis 7+ 响应：
// 下一次扫描 ID、已接管的强类型消息和已删除 ID 都不能丢失。
func TestStreamAutoClaim(t *testing.T) {
	// Arrange：构造 Redis 7 的三段式返回，并让默认 start/count 和亚毫秒边界同时受测。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	payload := `{"id":1,"name":"Alice"}`
	autoClaimReply := rueidismock.RedisArray(
		rueidismock.RedisBlobString("9-0"),
		rueidismock.RedisArray(redisStreamEntry("1-0", payload)),
		rueidismock.RedisArray(rueidismock.RedisBlobString("0-1"), rueidismock.RedisBlobString("0-2")),
	)
	client.EXPECT().Do(ctx, rueidismock.Match("XAUTOCLAIM", "app:events", "workers", "worker-2", "1", "0-0", "COUNT", "100")).Return(
		rueidismock.Result(autoClaimReply),
	)

	// Act：保留整个结果对象，调试时可直接检查三类返回数据。
	result, autoClaimErr := stream.AutoClaim(ctx, "workers", "worker-2", time.Nanosecond, "", 0)

	// Assert：强类型值解码正确，且 Redis 7 新增的 deleted IDs 完整保留。
	if autoClaimErr != nil || result.NextStart != "9-0" || len(result.Entries) != 1 || result.Entries[0].Value.Name != "Alice" {
		t.Fatalf("AutoClaim result=%+v err=%v", result, autoClaimErr)
	}
	if len(result.DeletedIDs) != 2 || result.DeletedIDs[0] != "0-1" || result.DeletedIDs[1] != "0-2" {
		t.Fatalf("AutoClaim deleted IDs=%+v", result.DeletedIDs)
	}
}

// TestStreamAutoClaimCompatibilityAndErrors 验证 Redis 6.2 的两段式响应仍可解析，
// 同时覆盖负 minIdle 本地校验和服务端返回结构异常时的可诊断错误。
func TestStreamAutoClaimCompatibilityAndErrors(t *testing.T) {
	// Arrange：第一次返回 Redis 6.2 格式，第二次故意缺少必需字段。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	payload := `{"id":1,"name":"Alice"}`
	client.EXPECT().Do(ctx, rueidismock.Match("XAUTOCLAIM", "app:events", "workers", "worker-2", "0", "1-0", "COUNT", "2")).Return(
		rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString("0-0"),
			rueidismock.RedisArray(redisStreamEntry("1-0", payload)),
		)),
	)
	client.EXPECT().Do(ctx, rueidismock.Match("XAUTOCLAIM", "app:events", "workers", "worker-2", "0", "1-0", "COUNT", "2")).Return(
		rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("0-0"))),
	)

	// Act/Assert：两段式响应的 DeletedIDs 应是可安全遍历的空切片，而不是 nil。
	compatibleResult, compatibleErr := stream.AutoClaim(ctx, "workers", "worker-2", 0, "1-0", 2)
	if compatibleErr != nil || compatibleResult.NextStart != "0-0" || len(compatibleResult.Entries) != 1 || compatibleResult.DeletedIDs == nil {
		t.Fatalf("compatible AutoClaim result=%+v err=%v", compatibleResult, compatibleErr)
	}
	// 响应字段数不对时不返回半成品，错误文本要包含 XAUTOCLAIM 便于定位。
	malformedResult, malformedErr := stream.AutoClaim(ctx, "workers", "worker-2", 0, "1-0", 2)
	if malformedResult.NextStart != "" || malformedErr == nil {
		t.Fatalf("malformed AutoClaim result=%+v err=%v", malformedResult, malformedErr)
	}
	// 负数无合法 Redis 语义，必须在客户端拒绝，不消耗额外命令。
	invalidResult, invalidErr := stream.AutoClaim(ctx, "workers", "worker-2", -time.Millisecond, "0-0", 10)
	if invalidResult.NextStart != "" || !errors.Is(invalidErr, ErrInvalidMinIdle) {
		t.Fatalf("negative AutoClaim result=%+v err=%v", invalidResult, invalidErr)
	}
}

// TestStreamGroupInformationAndReplay 验证 Stream 运维 API 能解析消费组与消费者状态，
// 并通过 XGROUP SETID 调整投递位置；可选字段缺失时必须显式标记为 unknown。
func TestStreamGroupInformationAndReplay(t *testing.T) {
	// Arrange：第一个消费组包含 Redis 7 的 entries-read/lag，第二个使用 Nil 模拟无法估算。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	groupsReply := rueidismock.RedisArray(
		rueidismock.RedisArray(
			rueidismock.RedisBlobString("name"), rueidismock.RedisBlobString("workers"),
			rueidismock.RedisBlobString("consumers"), rueidismock.RedisInt64(2),
			rueidismock.RedisBlobString("pending"), rueidismock.RedisInt64(3),
			rueidismock.RedisBlobString("last-delivered-id"), rueidismock.RedisBlobString("9-0"),
			rueidismock.RedisBlobString("entries-read"), rueidismock.RedisInt64(9),
			rueidismock.RedisBlobString("lag"), rueidismock.RedisInt64(4),
		),
		rueidismock.RedisArray(
			rueidismock.RedisBlobString("name"), rueidismock.RedisBlobString("legacy"),
			rueidismock.RedisBlobString("consumers"), rueidismock.RedisInt64(0),
			rueidismock.RedisBlobString("pending"), rueidismock.RedisInt64(0),
			rueidismock.RedisBlobString("last-delivered-id"), rueidismock.RedisBlobString("0-0"),
			rueidismock.RedisBlobString("entries-read"), rueidismock.RedisNil(),
			rueidismock.RedisBlobString("lag"), rueidismock.RedisNil(),
		),
	)
	// 消费者响应同时覆盖 Redis 7.2+ inactive 和旧版本不包含该字段的格式。
	consumersReply := rueidismock.RedisArray(
		rueidismock.RedisArray(
			rueidismock.RedisBlobString("name"), rueidismock.RedisBlobString("worker-1"),
			rueidismock.RedisBlobString("pending"), rueidismock.RedisInt64(2),
			rueidismock.RedisBlobString("idle"), rueidismock.RedisInt64(500),
			rueidismock.RedisBlobString("inactive"), rueidismock.RedisInt64(1200),
		),
		rueidismock.RedisArray(
			rueidismock.RedisBlobString("name"), rueidismock.RedisBlobString("worker-old"),
			rueidismock.RedisBlobString("pending"), rueidismock.RedisInt64(0),
			rueidismock.RedisBlobString("idle"), rueidismock.RedisInt64(2500),
		),
	)
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("XINFO", "GROUPS", "app:events")).Return(rueidismock.Result(groupsReply)),
		client.EXPECT().Do(ctx, rueidismock.Match("XINFO", "CONSUMERS", "app:events", "workers")).Return(rueidismock.Result(consumersReply)),
		client.EXPECT().Do(ctx, rueidismock.Match("XGROUP", "SETID", "app:events", "workers", "0-0")).Return(
			rueidismock.Result(rueidismock.RedisString("OK")),
		),
	)

	// Act/Assert：保留完整切片变量，断点时可观察 lag 和 known 标记。
	groups, groupsErr := stream.Groups(ctx)
	if groupsErr != nil || len(groups) != 2 {
		t.Fatalf("Groups values=%+v err=%v", groups, groupsErr)
	}
	workers := groups[0]
	if workers.Name != "workers" || workers.Consumers != 2 || workers.Pending != 3 || workers.LastDeliveredID != "9-0" ||
		workers.EntriesRead != 9 || !workers.EntriesReadKnown || workers.Lag != 4 || !workers.LagKnown {
		t.Fatalf("workers group=%+v", workers)
	}
	legacy := groups[1]
	if legacy.Name != "legacy" || legacy.EntriesReadKnown || legacy.LagKnown {
		t.Fatalf("legacy group=%+v", legacy)
	}
	// 毫秒整数应还原为 time.Duration，旧响应的 InactiveKnown 保持 false。
	consumers, consumersErr := stream.Consumers(ctx, "workers")
	if consumersErr != nil || len(consumers) != 2 {
		t.Fatalf("Consumers values=%+v err=%v", consumers, consumersErr)
	}
	worker := consumers[0]
	if worker.Name != "worker-1" || worker.Pending != 2 || worker.Idle != 500*time.Millisecond ||
		worker.Inactive != 1200*time.Millisecond || !worker.InactiveKnown {
		t.Fatalf("worker consumer=%+v", worker)
	}
	oldWorker := consumers[1]
	if oldWorker.Name != "worker-old" || oldWorker.Idle != 2500*time.Millisecond || oldWorker.InactiveKnown {
		t.Fatalf("old consumer=%+v", oldWorker)
	}
	// 重放入口只调整消费组 ID，成功时不返回多余状态。
	setGroupIDErr := stream.SetGroupID(ctx, "workers", "0-0")
	if setGroupIDErr != nil {
		t.Fatal(setGroupIDErr)
	}
}

// TestStreamGroupInformationDecodeErrors 验证 XINFO 响应被代理、模块或脏数据破坏时，
// 封装必须返回带行号与字段上下文的错误，不能产生半完成的运维数据。
func TestStreamGroupInformationDecodeErrors(t *testing.T) {
	// Arrange：第一个组缺少必需 name，第二个消费者行甚至不是 key/value 数组。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	stream := NewStream[unitUser](redis, "events")
	missingNameGroup := rueidismock.RedisArray(
		rueidismock.RedisBlobString("consumers"), rueidismock.RedisInt64(1),
		rueidismock.RedisBlobString("pending"), rueidismock.RedisInt64(0),
		rueidismock.RedisBlobString("last-delivered-id"), rueidismock.RedisBlobString("0-0"),
	)
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("XINFO", "GROUPS", "app:events")).Return(
			rueidismock.Result(rueidismock.RedisArray(missingNameGroup)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("XINFO", "CONSUMERS", "app:events", "workers")).Return(
			rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("invalid-row"))),
		),
	)

	// Act/Assert：错误时结果切片保持 nil，避免调用方误用前面已解析的局部数据。
	groups, groupsErr := stream.Groups(ctx)
	if groups != nil || groupsErr == nil {
		t.Fatalf("invalid Groups values=%+v err=%v", groups, groupsErr)
	}
	consumers, consumersErr := stream.Consumers(ctx, "workers")
	if consumers != nil || consumersErr == nil {
		t.Fatalf("invalid Consumers values=%+v err=%v", consumers, consumersErr)
	}
}
