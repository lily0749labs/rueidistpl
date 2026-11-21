package rueidistpl

import (
	"context"
	"errors"
	"slices"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestBloomFilterLifecycle 验证 Bloom Filter 的容量预留、单批量添加、单批量判断、计数和清理。
func TestBloomFilterLifecycle(t *testing.T) {
	// Arrange：预留参数和全部元素顺序必须原样进入 Redis，逻辑 key 自动添加 app 前缀。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	filter := redis.BloomFilter("seen-users")
	actualKey := filter.Key()
	if actualKey != "app:seen-users" {
		t.Fatalf("Key value=%q", actualKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("BF.RESERVE", "app:seen-users", "0.01", "1000")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("BF.ADD", "app:seen-users", "u1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("BF.MADD", "app:seen-users", "u1", "u2")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(0), rueidismock.RedisInt64(1)))),
		client.EXPECT().Do(ctx, rueidismock.Match("BF.EXISTS", "app:seen-users", "u1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("BF.MEXISTS", "app:seen-users", "u1", "missing")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1), rueidismock.RedisInt64(0)))),
		client.EXPECT().Do(ctx, rueidismock.Match("BF.CARD", "app:seen-users")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:seen-users")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// Reserve 先固定容量和误判率，Add 的 true 只表示过滤器位发生变化，不表示绝对唯一。
	reserveErr := filter.Reserve(ctx, 0.01, 1000)
	changed, addErr := filter.Add(ctx, "u1")
	batchChanged, batchAddErr := filter.AddMany(ctx, "u1", "u2")
	if reserveErr != nil || addErr != nil || !changed || batchAddErr != nil || !slices.Equal(batchChanged, []bool{false, true}) {
		t.Fatalf("Reserve=%v Add=%t/%v AddMany=%+v/%v", reserveErr, changed, addErr, batchChanged, batchAddErr)
	}

	// true 只表示“可能存在”，批量结果与输入位置严格对应；Card 是近似唯一元素数。
	exists, existsErr := filter.Exists(ctx, "u1")
	batchExists, batchExistsErr := filter.ExistsMany(ctx, "u1", "missing")
	cardinality, cardErr := filter.Card(ctx)
	clearErr := filter.Clear(ctx)
	if existsErr != nil || !exists || batchExistsErr != nil || !slices.Equal(batchExists, []bool{true, false}) || cardErr != nil || cardinality != 2 || clearErr != nil {
		t.Fatalf("Exists=%t/%v ExistsMany=%+v/%v Card=%d/%v Clear=%v", exists, existsErr, batchExists, batchExistsErr, cardinality, cardErr, clearErr)
	}
}

// TestBloomFilterValidationAndEmptyInputs 验证错误概率、错误容量和空批量均在本地处理。
func TestBloomFilterValidationAndEmptyInputs(t *testing.T) {
	// 不提供客户端，确保校验失败和空输入不会意外发送 Redis 命令。
	ctx := context.Background()
	filter := (&Redis{}).BloomFilter("seen")
	rateErr := filter.Reserve(ctx, 1, 100)
	capacityErr := filter.Reserve(ctx, 0.01, 0)
	emptyAdded, emptyAddErr := filter.AddMany(ctx)
	emptyExists, emptyExistsErr := filter.ExistsMany(ctx)
	if !errors.Is(rateErr, ErrInvalidBloomErrorRate) || !errors.Is(capacityErr, ErrInvalidBloomCapacity) {
		t.Fatalf("rate err=%v capacity err=%v", rateErr, capacityErr)
	}
	if emptyAddErr != nil || len(emptyAdded) != 0 || emptyExistsErr != nil || len(emptyExists) != 0 {
		t.Fatalf("empty AddMany=%+v/%v ExistsMany=%+v/%v", emptyAdded, emptyAddErr, emptyExists, emptyExistsErr)
	}
}
