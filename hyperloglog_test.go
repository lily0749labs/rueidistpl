package rueidistpl

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestHyperLogLogLifecycle 验证近似去重计数的添加、联合计数、合并和清理命令。
func TestHyperLogLogLifecycle(t *testing.T) {
	// Arrange：CountWith/MergeFrom 中的其他逻辑 key 都必须应用 app 前缀，并保持输入顺序。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	hll := redis.HyperLogLog("uv:{site}")
	actualKey := hll.Key()
	if actualKey != "app:uv:{site}" {
		t.Fatalf("Key value=%q", actualKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("PFADD", "app:uv:{site}", "u1", "u2")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("PFCOUNT", "app:uv:{site}")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("PFCOUNT", "app:uv:{site}", "app:mobile:{site}")).Return(rueidismock.Result(rueidismock.RedisInt64(3))),
		client.EXPECT().Do(ctx, rueidismock.Match("PFMERGE", "app:uv:{site}", "app:web:{site}", "app:mobile:{site}")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:uv:{site}")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// PFADD 的布尔返回表示内部寄存器是否变化，不等同于输入元素个数。
	changed, addErr := hll.Add(ctx, "u1", "u2")
	count, countErr := hll.Count(ctx)
	unionCount, unionErr := hll.CountWith(ctx, "mobile:{site}")
	mergeErr := hll.MergeFrom(ctx, "web:{site}", "mobile:{site}")
	clearErr := hll.Clear(ctx)
	if addErr != nil || !changed || countErr != nil || count != 2 || unionErr != nil || unionCount != 3 || mergeErr != nil || clearErr != nil {
		t.Fatalf("Add=%t/%v Count=%d/%v Union=%d/%v Merge=%v Clear=%v", changed, addErr, count, countErr, unionCount, unionErr, mergeErr, clearErr)
	}
}

// TestHyperLogLogEmptyInputs 验证空添加是本地 no-op，空合并会返回明确参数错误。
func TestHyperLogLogEmptyInputs(t *testing.T) {
	// 不配置客户端，确保两个边界分支都不会构造 Redis 命令。
	ctx := context.Background()
	hll := (&Redis{}).HyperLogLog("uv")
	changed, addErr := hll.Add(ctx)
	mergeErr := hll.MergeFrom(ctx)
	if changed || addErr != nil {
		t.Fatalf("empty Add changed=%t err=%v", changed, addErr)
	}
	if !errors.Is(mergeErr, ErrHyperLogLogSourcesRequired) {
		t.Fatalf("empty Merge err=%v", mergeErr)
	}

	// 显式引用 Rueidis Nil 类型，确认 Geo Distance 等单值缺失仍可使用项目统一判定。
	if !IsNotFound(rueidis.Nil) {
		t.Fatal("Rueidis Nil should be recognized as not found")
	}
}
