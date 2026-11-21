package rueidistpl

import (
	"context"
	"errors"
	"testing"
	"time"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestExpirationInfoStates 验证 Redis PTTL/HPTTL 的四类返回值都能被明确解释，
// 业务代码不再需要记忆 -1 和 -2 两个特殊状态码。
func TestExpirationInfoStates(t *testing.T) {
	// Arrange：覆盖正常剩余时间、永久对象、不存在对象，以及协议约定之外的负数响应。
	testCases := []struct {
		name         string
		milliseconds int64
		want         ExpirationInfo
		wantErr      bool
	}{
		{name: "有限有效期", milliseconds: 1500, want: ExpirationInfo{Exists: true, TTL: 1500 * time.Millisecond}},
		{name: "永久对象", milliseconds: -1, want: ExpirationInfo{Exists: true, Persistent: true}},
		{name: "对象不存在", milliseconds: -2, want: ExpirationInfo{}},
		{name: "异常状态码", milliseconds: -3, want: ExpirationInfo{}, wantErr: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// Act：只调用状态转换函数，排除 Redis mock 对特殊值解释的干扰。
			actual, err := expirationInfo(testCase.milliseconds)

			// Assert：错误分支必须返回完整零值，成功分支则精确比较三个公开字段。
			if actual != testCase.want {
				t.Fatalf("ExpirationInfo=%+v，期望 %+v", actual, testCase.want)
			}
			if testCase.wantErr && !errors.Is(err, ErrInvalidExpirationResponse) {
				t.Fatalf("err=%v，期望 %v", err, ErrInvalidExpirationResponse)
			}
			if !testCase.wantErr && err != nil {
				t.Fatalf("不应返回错误：%v", err)
			}
		})
	}
}

// TestRedisExpiration 验证 Redis 门面会应用全局 key 前缀，并把 PTTL 转换成结构化结果。
func TestRedisExpiration(t *testing.T) {
	// Arrange：准备一个带前缀的逻辑 key，并让 Redis 返回 2.5 秒剩余时间。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	client.EXPECT().Do(ctx, rueidismock.Match("PTTL", "app:session")).Return(
		rueidismock.Result(rueidismock.RedisInt64(2500)),
	)

	// Act：通过统一 Redis 门面查询，而不是直接调用底层 Rueidis。
	info, err := redis.Expiration(ctx, "session")

	// Assert：存在、非永久和 TTL 三个字段必须同时正确，避免只判断 TTL 大于零。
	want := ExpirationInfo{Exists: true, TTL: 2500 * time.Millisecond}
	if err != nil || info != want {
		t.Fatalf("ExpirationInfo=%+v err=%v，期望 %+v", info, err, want)
	}
}

// TestExpirationMethodsDiscardResultsOnRedisError 验证 Value 和 Redis 查询失败时，
// 不会把不完整数据伪装成有效的结构化过期状态。
func TestExpirationMethodsDiscardResultsOnRedisError(t *testing.T) {
	// Arrange：同一个可识别错误分别由 Value.PTTL 和 Redis.PTTL 返回。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	wantErr := errors.New("redis unavailable")
	value := NewValue[unitUser](redis, "profile")
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("PTTL", "app:profile")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("PTTL", "app:profile")).Return(rueidismock.ErrorResult(wantErr)),
	)

	// Act：分别从强类型 Value 和通用 Redis 门面触发错误路径。
	valueInfo, valueErr := value.Expiration(ctx)
	redisInfo, redisErr := redis.Expiration(ctx, "profile")

	// Assert：两个入口均返回零值，并保留底层错误链供 errors.Is 判断。
	if valueInfo != (ExpirationInfo{}) || !errors.Is(valueErr, wantErr) {
		t.Fatalf("Value.Expiration info=%+v err=%v", valueInfo, valueErr)
	}
	if redisInfo != (ExpirationInfo{}) || !errors.Is(redisErr, wantErr) {
		t.Fatalf("Redis.Expiration info=%+v err=%v", redisInfo, redisErr)
	}
}

// TestHashExpirationField 验证 Hash field 的数组响应能够转换成结构化永久状态。
func TestHashExpirationField(t *testing.T) {
	// Arrange：Redis 7.4+ 的 HPTTL 单 field 响应是只包含一个状态码的数组。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hash := NewHash[unitUser](redis, "users")
	client.EXPECT().Do(ctx, rueidismock.Match("HPTTL", "app:users", "FIELDS", "1", "1")).Return(
		rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(-1))),
	)

	// Act：查询 field 的过期状态。
	info, err := hash.ExpirationField(ctx, "1")

	// Assert：-1 必须解释为“存在且永久”，TTL 本身保持零值。
	want := ExpirationInfo{Exists: true, Persistent: true}
	if err != nil || info != want {
		t.Fatalf("ExpirationField info=%+v err=%v，期望 %+v", info, err, want)
	}
}

// TestHashExpirationFieldRejectsMalformedResponse 验证 HPTTL 数组元素不是整数时，
// 结构化 API 会返回零值与解析错误，而不会误判 field 不存在。
func TestHashExpirationFieldRejectsMalformedResponse(t *testing.T) {
	// Arrange：使用字符串元素模拟代理、模块或未来协议返回了非预期类型。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hash := NewHash[unitUser](redis, "users")
	client.EXPECT().Do(ctx, rueidismock.Match("HPTTL", "app:users", "FIELDS", "1", "1")).Return(
		rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("invalid"))),
	)

	// Act：执行 field 过期状态查询并保留结果变量，方便断点检查。
	info, err := hash.ExpirationField(ctx, "1")

	// Assert：解析失败不能携带 Exists/Persistent/TTL 等看似有效的数据。
	if err == nil || info != (ExpirationInfo{}) {
		t.Fatalf("ExpirationField info=%+v err=%v，期望零值和解析错误", info, err)
	}
}
