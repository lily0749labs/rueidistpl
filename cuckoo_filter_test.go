package rueidistpl

import (
	"context"
	"errors"
	"slices"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestCuckooFilterLifecycle 验证 Cuckoo Filter 可添加、去重添加、判断、删除和计数。
func TestCuckooFilterLifecycle(t *testing.T) {
	// Arrange：Cuckoo Filter 支持真正的删除，因此删除和重复次数都要单独验证。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	filter := redis.CuckooFilter("sessions")
	actualKey := filter.Key()
	if actualKey != "app:sessions" {
		t.Fatalf("Key value=%q", actualKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("CF.RESERVE", "app:sessions", "1000")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("CF.ADD", "app:sessions", "s1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("CF.ADDNX", "app:sessions", "s1")).Return(rueidismock.Result(rueidismock.RedisInt64(0))),
		client.EXPECT().Do(ctx, rueidismock.Match("CF.EXISTS", "app:sessions", "s1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("CF.MEXISTS", "app:sessions", "s1", "missing")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1), rueidismock.RedisInt64(0)))),
		client.EXPECT().Do(ctx, rueidismock.Match("CF.COUNT", "app:sessions", "s1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("CF.DEL", "app:sessions", "s1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:sessions")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// AddNX 对“可能已存在”的元素返回 false；这仍受概率结构假阳性的影响。
	reserveErr := filter.Reserve(ctx, 1000)
	added, addErr := filter.Add(ctx, "s1")
	addedNX, addNXErr := filter.AddNX(ctx, "s1")
	if reserveErr != nil || addErr != nil || !added || addNXErr != nil || addedNX {
		t.Fatalf("Reserve=%v Add=%t/%v AddNX=%t/%v", reserveErr, added, addErr, addedNX, addNXErr)
	}

	// 单批量存在性、重复计数和一次删除分别保留明确返回值，方便业务层判断。
	exists, existsErr := filter.Exists(ctx, "s1")
	batchExists, batchErr := filter.ExistsMany(ctx, "s1", "missing")
	count, countErr := filter.Count(ctx, "s1")
	deleted, deleteErr := filter.Delete(ctx, "s1")
	clearErr := filter.Clear(ctx)
	if existsErr != nil || !exists || batchErr != nil || !slices.Equal(batchExists, []bool{true, false}) || countErr != nil || count != 1 || deleteErr != nil || !deleted || clearErr != nil {
		t.Fatalf("Exists=%t/%v Batch=%+v/%v Count=%d/%v Delete=%t/%v Clear=%v", exists, existsErr, batchExists, batchErr, count, countErr, deleted, deleteErr, clearErr)
	}
}

// TestCuckooFilterValidationAndEmptyInputs 验证非正容量和空批量不会访问 Redis。
func TestCuckooFilterValidationAndEmptyInputs(t *testing.T) {
	// 无 client 的门面使“本地处理”成为可执行断言，而不只是代码注释。
	ctx := context.Background()
	filter := (&Redis{}).CuckooFilter("sessions")
	reserveErr := filter.Reserve(ctx, 0)
	emptyValues, emptyErr := filter.ExistsMany(ctx)
	if !errors.Is(reserveErr, ErrInvalidCuckooCapacity) || emptyErr != nil || len(emptyValues) != 0 {
		t.Fatalf("Reserve err=%v ExistsMany=%+v/%v", reserveErr, emptyValues, emptyErr)
	}
}
