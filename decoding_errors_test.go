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

// TestValueRedisErrorsReturnZeroValues 验证 GET/GETDEL 的 Redis 异常不会泄漏泛型对象的部分值，
// 同时覆盖 Value.Expiration 的正常结构化返回路径。
func TestValueRedisErrorsReturnZeroValues(t *testing.T) {
	// Arrange：前两条读取返回同一个可识别故障，最后一条 PTTL 返回永久状态。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	value := NewValue[unitUser](redis, "profile")
	wantErr := errors.New("redis read failed")
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("GET", "app:profile")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, nonRetryable(rueidismock.Match("GETDEL", "app:profile"))).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("PTTL", "app:profile")).Return(rueidismock.Result(rueidismock.RedisInt64(-1))),
	)

	// Act：保留三个调用各自的值和错误，断点时可以直接观察错误路径的真实返回值。
	getValue, getErr := value.Get(ctx)
	deletedValue, getDeleteErr := value.GetDelete(ctx)
	expiration, expirationErr := value.Expiration(ctx)

	// Assert：读取错误时必须返回 T 的完整零值，且包装前后的错误仍能通过 errors.Is 识别。
	if getValue != (unitUser{}) || !errors.Is(getErr, wantErr) {
		t.Fatalf("Get value=%+v err=%v", getValue, getErr)
	}
	if deletedValue != (unitUser{}) || !errors.Is(getDeleteErr, wantErr) {
		t.Fatalf("GetDelete value=%+v err=%v", deletedValue, getDeleteErr)
	}
	// 永久状态由两个布尔字段表达，调用方不再比较 TTL == -1ms。
	wantExpiration := ExpirationInfo{Exists: true, Persistent: true}
	if expiration != wantExpiration || expirationErr != nil {
		t.Fatalf("Expiration info=%+v err=%v", expiration, expirationErr)
	}
}

// TestHashReadErrorBranches 验证 Hash 读取 API 对传输错误、响应长度异常、
// RESP 类型异常和脏 JSON 都会返回 nil/零值，而不是半完成的结果。
func TestHashReadErrorBranches(t *testing.T) {
	// Arrange：按调用顺序为 HGET、四次 HMGET、两次 HGETALL 和两次 HVALS 准备异常响应。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hash := NewHash[unitUser](redis, "users")
	wantErr := errors.New("redis read failed")
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("HGET", "app:users", "1")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("HMGET", "app:users", "1")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("HMGET", "app:users", "1", "2")).Return(rueidismock.Result(redisStrings(`{"id":1,"name":"Alice"}`))),
		client.EXPECT().Do(ctx, rueidismock.Match("HMGET", "app:users", "1")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisArray()))),
		client.EXPECT().Do(ctx, rueidismock.Match("HMGET", "app:users", "1")).Return(rueidismock.Result(redisStrings(`{"id":99,"name":[]}`))),
		client.EXPECT().Do(ctx, rueidismock.Match("HGETALL", "app:users")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("HGETALL", "app:users")).Return(rueidismock.Result(rueidismock.RedisMap(map[string]rueidis.RedisMessage{
			"1": rueidismock.RedisBlobString(`{"id":99,"name":[]}`),
		}))),
		client.EXPECT().Do(ctx, rueidismock.Match("HVALS", "app:users")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("HVALS", "app:users")).Return(rueidismock.Result(redisStrings(`{"id":99,"name":[]}`))),
	)

	// Act/Assert：HGET 传输失败时泛型结果必须是完整零值。
	getValue, getErr := hash.Get(ctx, "1")
	if getValue != (unitUser{}) || !errors.Is(getErr, wantErr) {
		t.Fatalf("Get value=%+v err=%v", getValue, getErr)
	}

	// HMGET 的四种异常分别验证顶层错误、字段数量、字段 RESP 类型和 JSON 解码。
	transportValues, transportErr := hash.GetMany(ctx, "1")
	if transportValues != nil || !errors.Is(transportErr, wantErr) {
		t.Fatalf("GetMany transport values=%+v err=%v", transportValues, transportErr)
	}
	lengthValues, lengthErr := hash.GetMany(ctx, "1", "2")
	if lengthValues != nil || lengthErr == nil {
		t.Fatalf("GetMany length values=%+v err=%v", lengthValues, lengthErr)
	}
	typeValues, typeErr := hash.GetMany(ctx, "1")
	if typeValues != nil || typeErr == nil {
		t.Fatalf("GetMany type values=%+v err=%v", typeValues, typeErr)
	}
	decodeValues, decodeErr := hash.GetMany(ctx, "1")
	if decodeValues != nil || decodeErr == nil {
		t.Fatalf("GetMany decode values=%+v err=%v", decodeValues, decodeErr)
	}

	// HGETALL 和 HVALS 也必须在任何错误时丢弃已经解析的局部集合。
	allTransportValues, allTransportErr := hash.All(ctx)
	if allTransportValues != nil || !errors.Is(allTransportErr, wantErr) {
		t.Fatalf("All transport values=%+v err=%v", allTransportValues, allTransportErr)
	}
	allDecodeValues, allDecodeErr := hash.All(ctx)
	if allDecodeValues != nil || allDecodeErr == nil {
		t.Fatalf("All decode values=%+v err=%v", allDecodeValues, allDecodeErr)
	}
	valuesTransport, valuesTransportErr := hash.Values(ctx)
	if valuesTransport != nil || !errors.Is(valuesTransportErr, wantErr) {
		t.Fatalf("Values transport values=%+v err=%v", valuesTransport, valuesTransportErr)
	}
	valuesDecode, valuesDecodeErr := hash.Values(ctx)
	if valuesDecode != nil || valuesDecodeErr == nil {
		t.Fatalf("Values decode values=%+v err=%v", valuesDecode, valuesDecodeErr)
	}
}

// TestHashScanErrorBranches 验证 HSCAN 对顶层 Redis 错误、奇数 field/value 数量和脏 JSON
// 都返回 HashScan 零值，调用方不会误用尚未完整解析的游标或 map。
func TestHashScanErrorBranches(t *testing.T) {
	// Arrange：三次相同扫描依次返回传输错误、奇数元素和无法解码的对象。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hash := NewHash[unitUser](redis, "users")
	wantErr := errors.New("scan failed")
	command := rueidismock.Match("HSCAN", "app:users", "0", "COUNT", "100")
	gomock.InOrder(
		client.EXPECT().Do(ctx, command).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString("7"),
			redisStrings("field-only"),
		))),
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString("7"),
			redisStrings("1", `{"id":99,"name":[]}`),
		))),
	)

	// Act：逐次保存三个扫描结果，便于确认每条错误路径都清除了 cursor 和 values。
	transportResult, transportErr := hash.Scan(ctx, 0, "", 0)
	oddResult, oddErr := hash.Scan(ctx, 0, "", 0)
	decodeResult, decodeErr := hash.Scan(ctx, 0, "", 0)

	// Assert：所有异常都只能返回 HashScan 的零值，不能暴露响应中的游标 7。
	if transportResult.Cursor != 0 || transportResult.Values != nil || !errors.Is(transportErr, wantErr) {
		t.Fatalf("transport result=%+v err=%v", transportResult, transportErr)
	}
	if oddResult.Cursor != 0 || oddResult.Values != nil || oddErr == nil {
		t.Fatalf("odd result=%+v err=%v", oddResult, oddErr)
	}
	if decodeResult.Cursor != 0 || decodeResult.Values != nil || decodeErr == nil {
		t.Fatalf("decode result=%+v err=%v", decodeResult, decodeErr)
	}
}

// TestListErrorBranches 验证 List 的编码、Redis 读取和单元素解码失败都返回明确错误，
// 并保持泛型返回值和切片为零值。
func TestListErrorBranches(t *testing.T) {
	// Arrange：合法 List 的 LRANGE 返回传输错误；泛型 chan List 用于触发本地 JSON 编码失败。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	list := NewList[unitUser](redis, "users")
	wantErr := errors.New("list read failed")
	client.EXPECT().Do(ctx, rueidismock.Match("LRANGE", "app:users", "0", "-1")).Return(rueidismock.ErrorResult(wantErr))
	badList := NewList[chan int](&Redis{}, "bad")

	// Act：分别触发 PushLeft、Set 的编码失败，Range 的 Redis 失败和单元素 Redis 失败。
	pushLength, pushErr := badList.PushLeft(ctx, make(chan int))
	setErr := badList.Set(ctx, 0, make(chan int))
	rangeValues, rangeErr := list.Range(ctx, 0, -1)
	decodedValue, decodeErr := decodeListResult[unitUser](rueidismock.ErrorResult(wantErr))

	// Assert：本地编码失败不能产生长度或 Redis 调用，读取失败不能保留集合或对象。
	if pushLength != 0 || pushErr == nil {
		t.Fatalf("PushLeft length=%d err=%v", pushLength, pushErr)
	}
	if setErr == nil {
		t.Fatal("Set 应拒绝 JSON 不支持的 chan 字段")
	}
	if rangeValues != nil || !errors.Is(rangeErr, wantErr) {
		t.Fatalf("Range values=%+v err=%v", rangeValues, rangeErr)
	}
	if decodedValue != (unitUser{}) || !errors.Is(decodeErr, wantErr) {
		t.Fatalf("decodeListResult value=%+v err=%v", decodedValue, decodeErr)
	}
}

// TestTTLFieldRedisError 验证旧 TTLField 兼容 API 遇到异常响应时仍返回零 TTL，
// 使新增结构化 API 不会改变原有错误语义。
func TestTTLFieldRedisError(t *testing.T) {
	// Arrange：HPTTL 顶层返回 Redis 错误，无法解析为单元素整数数组。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hash := NewHash[unitUser](redis, "users")
	wantErr := errors.New("hpttl failed")
	client.EXPECT().Do(ctx, rueidismock.Match("HPTTL", "app:users", "FIELDS", "1", "1")).Return(rueidismock.ErrorResult(wantErr))

	// Act：调用为兼容保留的原始 TTLField API。
	ttl, err := hash.TTLField(ctx, "1")

	// Assert：错误链保持不变，TTL 不能残留任何看似有效的时间。
	if ttl != time.Duration(0) || !errors.Is(err, wantErr) {
		t.Fatalf("TTLField ttl=%s err=%v", ttl, err)
	}
}
