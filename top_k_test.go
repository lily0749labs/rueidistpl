package rueidistpl

import (
	"context"
	"errors"
	"slices"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestTopKLifecycle 验证默认/完整初始化、挤出结果、成员判断、计数、列表和清理。
func TestTopKLifecycle(t *testing.T) {
	// Arrange：TOPK.ADD 的 Redis Nil 表示未挤出元素，字符串则保留被挤出元素名称。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	top := redis.TopK("hot-products")
	actualKey := top.Key()
	if actualKey != "app:hot-products" {
		t.Fatalf("Key value=%q", actualKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("TOPK.RESERVE", "app:hot-products", "2")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("TOPK.RESERVE", "app:hot-products", "2", "20", "7", "0.9")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("TOPK.ADD", "app:hot-products", "p1", "p2")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisNil(), rueidismock.RedisBlobString("old")))),
		client.EXPECT().Do(ctx, rueidismock.Match("TOPK.QUERY", "app:hot-products", "p1", "missing")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1), rueidismock.RedisInt64(0)))),
		client.EXPECT().Do(ctx, rueidismock.Match("TOPK.COUNT", "app:hot-products", "p1", "p2")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(10), rueidismock.RedisInt64(8)))),
		client.EXPECT().Do(ctx, rueidismock.Match("TOPK.LIST", "app:hot-products", "WITHCOUNT")).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString("p1"), rueidismock.RedisInt64(10), rueidismock.RedisBlobString("p2"), rueidismock.RedisInt64(8),
		))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:hot-products")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 默认和完整参数初始化都独立暴露，调用方无需记住 TOPK.RESERVE 的可选参数顺序。
	reserveErr := top.Reserve(ctx, 2)
	reserveOptionsErr := top.ReserveWithOptions(ctx, 2, TopKOptions{Width: 20, Depth: 7, Decay: 0.9})
	if reserveErr != nil || reserveOptionsErr != nil {
		t.Fatalf("Reserve=%v ReserveWithOptions=%v", reserveErr, reserveOptionsErr)
	}

	// 每个输入元素都有一个结构化结果，HasKicked 明确区分 Redis Nil 和空字符串元素。
	addResults, addErr := top.Add(ctx, "p1", "p2")
	wantAddResults := []TopKAddResult{{Item: "p1"}, {Item: "p2", Kicked: "old", HasKicked: true}}
	if addErr != nil || !slices.Equal(addResults, wantAddResults) {
		t.Fatalf("Add values=%+v err=%v", addResults, addErr)
	}

	// Query、Count 和 List 分别覆盖归属、按输入查询计数及当前完整榜单三种高频需求。
	queryValues, queryErr := top.Query(ctx, "p1", "missing")
	counts, countErr := top.Count(ctx, "p1", "p2")
	items, listErr := top.List(ctx)
	wantItems := []TopKItem{{Item: "p1", Count: 10}, {Item: "p2", Count: 8}}
	clearErr := top.Clear(ctx)
	if queryErr != nil || !slices.Equal(queryValues, []bool{true, false}) || countErr != nil || !slices.Equal(counts, []int64{10, 8}) || listErr != nil || !slices.Equal(items, wantItems) || clearErr != nil {
		t.Fatalf("Query=%+v/%v Count=%+v/%v List=%+v/%v Clear=%v", queryValues, queryErr, counts, countErr, items, listErr, clearErr)
	}
}

// TestTopKValidationAndDecodeErrors 验证初始化参数、空输入和异常数组响应。
func TestTopKValidationAndDecodeErrors(t *testing.T) {
	// 本地分支使用空门面验证，非法 size/精度配置和空批量都不需要 Redis。
	ctx := context.Background()
	top := (&Redis{}).TopK("hot")
	sizeErr := top.Reserve(ctx, 0)
	optionsErr := top.ReserveWithOptions(ctx, 2, TopKOptions{Width: 0, Depth: 7, Decay: 0.9})
	emptyAdded, emptyAddErr := top.Add(ctx)
	emptyQuery, emptyQueryErr := top.Query(ctx)
	emptyCount, emptyCountErr := top.Count(ctx)
	if !errors.Is(sizeErr, ErrInvalidTopK) || !errors.Is(optionsErr, ErrInvalidTopKOptions) {
		t.Fatalf("size err=%v options err=%v", sizeErr, optionsErr)
	}
	if emptyAddErr != nil || len(emptyAdded) != 0 || emptyQueryErr != nil || len(emptyQuery) != 0 || emptyCountErr != nil || len(emptyCount) != 0 {
		t.Fatalf("Add=%+v/%v Query=%+v/%v Count=%+v/%v", emptyAdded, emptyAddErr, emptyQuery, emptyQueryErr, emptyCount, emptyCountErr)
	}

	// ADD 长度必须与输入一致，LIST 必须是 item/count 偶数布局，防止错位数据进入业务层。
	redis, client := newMockRedis(t)
	badTop := redis.TopK("hot")
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("TOPK.ADD", "app:hot", "p1")).Return(rueidismock.Result(rueidismock.RedisArray())),
		client.EXPECT().Do(ctx, rueidismock.Match("TOPK.LIST", "app:hot", "WITHCOUNT")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("p1")))),
	)
	badAddResults, badAddErr := badTop.Add(ctx, "p1")
	badListResults, badListErr := badTop.List(ctx)
	if badAddResults != nil || badAddErr == nil || badListResults != nil || badListErr == nil {
		t.Fatalf("Add=%+v/%v List=%+v/%v", badAddResults, badAddErr, badListResults, badListErr)
	}
}
