package rueidistpl

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
)

// TestDeleteEachReturnsPerKeyResults 验证跨 Slot 友好的逐 key 删除会保留输入顺序、
// 每个 key 的删除状态和成功总数，同时仍自动应用全局 key 前缀。
func TestDeleteEachReturnsPerKeyResults(t *testing.T) {
	// Arrange：三个逻辑 key 分别模拟“已删除、不存在、已删除”，并精确匹配三条独立 DEL。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	client.EXPECT().DoMulti(
		ctx,
		rueidismock.Match("DEL", "app:{account-1}:profile"),
		rueidismock.Match("DEL", "app:{account-2}:profile"),
		rueidismock.Match("DEL", "app:{account-3}:profile"),
	).Return([]rueidis.RedisResult{
		rueidismock.Result(rueidismock.RedisInt64(1)),
		rueidismock.Result(rueidismock.RedisInt64(0)),
		rueidismock.Result(rueidismock.RedisInt64(1)),
	})

	// Act：这些 key 带有不同 Cluster hash tag，证明门面没有拼成一条跨 Slot DEL。
	result := redis.DeleteEach(ctx, "{account-1}:profile", "{account-2}:profile", "{account-3}:profile")

	// Assert：汇总数字和三个明细都可直接在断点中查看，不依赖 err 是否为空来保留数据。
	if result.Err != nil || result.Deleted != 2 || len(result.Results) != 3 {
		t.Fatalf("DeleteEach result=%+v", result)
	}
	if result.Results[0].Key != "{account-1}:profile" || !result.Results[0].Deleted || result.Results[0].Err != nil {
		t.Fatalf("第一个 key 结果错误：%+v", result.Results[0])
	}
	if result.Results[1].Key != "{account-2}:profile" || result.Results[1].Deleted || result.Results[1].Err != nil {
		t.Fatalf("第二个 key 结果错误：%+v", result.Results[1])
	}
	if result.Results[2].Key != "{account-3}:profile" || !result.Results[2].Deleted || result.Results[2].Err != nil {
		t.Fatalf("第三个 key 结果错误：%+v", result.Results[2])
	}
}

// TestExistsEachPreservesPartialSuccess 验证中间 key 查询失败时，前后成功项、
// 实际存在总数、单项错误和聚合错误都会同时保留下来。
func TestExistsEachPreservesPartialSuccess(t *testing.T) {
	// Arrange：第二条命令返回 Redis 错误，其余两条分别返回存在和不存在。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	wantErr := errors.New("cluster node unavailable")
	client.EXPECT().DoMulti(
		ctx,
		rueidismock.Match("EXISTS", "app:a"),
		rueidismock.Match("EXISTS", "app:b"),
		rueidismock.Match("EXISTS", "app:c"),
	).Return([]rueidis.RedisResult{
		rueidismock.Result(rueidismock.RedisInt64(1)),
		rueidismock.ErrorResult(wantErr),
		rueidismock.Result(rueidismock.RedisInt64(0)),
	})

	// Act：获取一个承载全部明细的结果结构，避免多返回值在错误时丢失可用数据。
	result := redis.ExistsEach(ctx, "a", "b", "c")

	// Assert：聚合错误仍可通过 errors.Is 识别，成功计数只包含已确认存在的 key。
	if !errors.Is(result.Err, wantErr) || result.Existing != 1 || len(result.Results) != 3 {
		t.Fatalf("ExistsEach result=%+v", result)
	}
	if !result.Results[0].Exists || result.Results[0].Err != nil {
		t.Fatalf("成功项 a 被破坏：%+v", result.Results[0])
	}
	if result.Results[1].Exists || !errors.Is(result.Results[1].Err, wantErr) {
		t.Fatalf("失败项 b 没有保留原始错误：%+v", result.Results[1])
	}
	if result.Results[2].Exists || result.Results[2].Err != nil {
		t.Fatalf("不存在项 c 被误判：%+v", result.Results[2])
	}
}

// TestLegacyDeleteReturnsPartialCount 验证原有 Delete API 在部分失败时也返回已经确认的删除数量，
// 老调用方无需迁移即可获得比“错误时固定返回零”更准确的信息。
func TestLegacyDeleteReturnsPartialCount(t *testing.T) {
	// Arrange：第一条 DEL 成功，第二条失败；错误用于验证包装后仍可识别。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	wantErr := errors.New("timeout")
	client.EXPECT().DoMulti(ctx, rueidismock.Match("DEL", "app:a"), rueidismock.Match("DEL", "app:b")).Return(
		[]rueidis.RedisResult{
			rueidismock.Result(rueidismock.RedisInt64(1)),
			rueidismock.ErrorResult(wantErr),
		},
	)

	// Act：调用兼容入口，而不是新的 DeleteEach。
	deleted, err := redis.Delete(ctx, "a", "b")

	// Assert：deleted=1 明确表达部分成功，err 则提醒调用方仍需处理失败项。
	if deleted != 1 || !errors.Is(err, wantErr) {
		t.Fatalf("Delete deleted=%d err=%v", deleted, err)
	}
}

// TestBatchOperationsWithNoKeys 验证空输入立即返回非 nil 的空明细，
// 调用方可以直接遍历结果，并且不会向 Redis 发送无效命令。
func TestBatchOperationsWithNoKeys(t *testing.T) {
	// Arrange：使用没有底层客户端的门面，任何意外 Redis 调用都会触发 panic。
	redis := &Redis{}
	ctx := context.Background()

	// Act：分别执行两个逐 key 批量入口。
	deleteResult := redis.DeleteEach(ctx)
	existsResult := redis.ExistsEach(ctx)

	// Assert：空切片必须已初始化，汇总值和聚合错误保持零值。
	if deleteResult.Results == nil || len(deleteResult.Results) != 0 || deleteResult.Deleted != 0 || deleteResult.Err != nil {
		t.Fatalf("DeleteEach 空结果错误：%+v", deleteResult)
	}
	if existsResult.Results == nil || len(existsResult.Results) != 0 || existsResult.Existing != 0 || existsResult.Err != nil {
		t.Fatalf("ExistsEach 空结果错误：%+v", existsResult)
	}
}

// TestBooleanBatchResultsRejectsMalformedResponses 验证批量协议异常不会被当作业务上的 false，
// 并覆盖结果缺失、结果过多和单 key 返回值越界三种低概率错误分支。
func TestBooleanBatchResultsRejectsMalformedResponses(t *testing.T) {
	// Arrange：两个 key 只收到一个结果，且该结果值为 2；这样可同时触发越界与缺失分支。
	redisResults := []rueidis.RedisResult{
		rueidismock.Result(rueidismock.RedisInt64(2)),
	}

	// Act：直接解析协议层结果，保留明细、计数和聚合错误三个变量便于断点查看。
	items := make([]ExistsKeyResult, 2)
	count, err := booleanBatchResults("EXISTS", []string{"a", "b"}, redisResults, func(index int, value bool, err error) {
		items[index] = ExistsKeyResult{Exists: value, Err: err}
	})

	// Assert：两个 key 都必须标记错误，异常值不能进入成功计数。
	if count != 0 || len(items) != 2 || !errors.Is(err, ErrInvalidBatchResponse) {
		t.Fatalf("items=%+v count=%d err=%v", items, count, err)
	}
	if !errors.Is(items[0].Err, ErrInvalidBatchResponse) || !errors.Is(items[1].Err, ErrInvalidBatchResponse) {
		t.Fatalf("逐 key 协议错误缺失：%+v", items)
	}

	// Arrange/Act：一个 key 收到两个结果，额外结果必须生成批次级错误。
	extraItems := make([]DeleteKeyResult, 1)
	extraCount, extraErr := booleanBatchResults("DEL", []string{"a"}, []rueidis.RedisResult{
		rueidismock.Result(rueidismock.RedisInt64(1)),
		rueidismock.Result(rueidismock.RedisInt64(0)),
	}, func(index int, value bool, err error) {
		extraItems[index] = DeleteKeyResult{Deleted: value, Err: err}
	})

	// Assert：有效的第一个结果仍应保留，同时用 ErrInvalidBatchResponse 暴露数量不一致。
	if extraCount != 1 || len(extraItems) != 1 || !extraItems[0].Deleted || !errors.Is(extraErr, ErrInvalidBatchResponse) {
		t.Fatalf("extra items=%+v count=%d err=%v", extraItems, extraCount, extraErr)
	}
}
