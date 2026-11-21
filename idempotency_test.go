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

// TestIdempotencyLifecycle 验证 SET NX PX 的三项核心语义：
// 毫秒以下正 TTL 自动提升为 1ms、首次 ID 获取成功、重复 ID 返回 false；
// 同时验证 Exists 与 Forget 使用同一命名空间 key。
func TestIdempotencyLifecycle(t *testing.T) {
	// Arrange：同一个业务 ID 连续执行两次，mock 分别返回 SET 成功和 NX 冲突。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	idempotency := redis.Idempotency("requests")

	// 精确核对 TTL 下限、幂等 key 命名空间以及 Exists/Forget 后续命令。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:requests:event-1", "1", "NX", "PX", "1")).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:requests:event-1", "1", "NX", "PX", "1000")).Return(rueidismock.Result(rueidismock.RedisNil())),
		client.EXPECT().Do(ctx, rueidismock.Match("EXISTS", "app:requests:event-1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:requests:event-1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 第一次必须获得执行权，纳秒级正 TTL 应以 PX 1 写入。
	tryAcquired, tryErr := idempotency.Try(ctx, "event-1", time.Nanosecond)
	if tryErr != nil || !tryAcquired {
		t.Fatalf("Try acquired=%t err=%v", tryAcquired, tryErr)
	}
	// 第二次 Redis Nil 代表标记已存在，应转换成 acquired=false 且没有错误。
	tryAcquired2, tryErr2 := idempotency.Try(ctx, "event-1", time.Second)
	if tryErr2 != nil || tryAcquired2 {
		t.Fatalf("duplicate Try acquired=%t err=%v", tryAcquired2, tryErr2)
	}
	// 最后验证标记查询和主动遗忘都使用同一个真实 key。
	exists, existsErr := idempotency.Exists(ctx, "event-1")
	if existsErr != nil || !exists {
		t.Fatalf("Exists value=%t err=%v", exists, existsErr)
	}
	forgetErr := idempotency.Forget(ctx, "event-1")
	if forgetErr != nil {
		t.Fatal(forgetErr)
	}
}

// TestIdempotencyRun 验证业务函数只在首次获得标记时执行：
// 成功任务保留标记，重复任务跳过 handler，nil handler 合法；
// 失败任务只删除属于本次执行的令牌，不能误删后来请求重新取得的标记。
func TestIdempotencyRun(t *testing.T) {
	// Arrange：依次准备成功、重复、nil handler 和业务失败四种执行结果。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	idempotency := redis.Idempotency("requests")
	workErr := errors.New("work failed")
	var successToken string
	var failedToken string

	gomock.InOrder(
		client.EXPECT().Do(ctx, matchIdempotencySet("app:requests:success", "1000", &successToken)).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(gomock.Any(), retryable(matchLuaArgs([]string{"app:requests:success"}, func(args []string) bool {
			return len(args) == 2 && args[0] == successToken && args[1] == "1000"
		}))).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, matchIdempotencySet("app:requests:duplicate", "1000", nil)).Return(rueidismock.Result(rueidismock.RedisNil())),
		client.EXPECT().Do(ctx, matchIdempotencySet("app:requests:nil-handler", "1000", nil)).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(ctx, matchIdempotencySet("app:requests:failed", "1000", &failedToken)).Return(rueidismock.Result(rueidismock.RedisString("OK"))),
		client.EXPECT().Do(gomock.Any(), retryable(matchLuaArgs([]string{"app:requests:failed"}, func(args []string) bool {
			return len(args) == 1 && args[0] == failedToken
		}))).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 成功场景应执行 handler 一次并保留幂等标记。
	called := 0
	ran, err := idempotency.Run(ctx, "success", time.Second, func(context.Context) error {
		called++
		return nil
	})
	if err != nil || !ran || called != 1 {
		t.Fatalf("success ran=%t called=%d err=%v", ran, called, err)
	}
	// 重复场景不得再次进入 handler，called 必须保持不变。
	ran, err = idempotency.Run(ctx, "duplicate", time.Second, func(context.Context) error {
		called++
		return nil
	})
	if err != nil || ran || called != 1 {
		t.Fatalf("duplicate ran=%t called=%d err=%v", ran, called, err)
	}
	// nil handler 允许只占用幂等标记，用于调用方自行控制后续流程。
	ran, err = idempotency.Run(ctx, "nil-handler", time.Second, nil)
	if err != nil || !ran {
		t.Fatalf("nil handler ran=%t err=%v", ran, err)
	}
	// 业务失败后应使用独立清理 context 删除标记，让下一次请求可以重试。
	ran, err = idempotency.Run(ctx, "failed", time.Second, func(context.Context) error { return workErr })
	if !ran || !errors.Is(err, workErr) {
		t.Fatalf("failed run ran=%t err=%v", ran, err)
	}
}

// TestIdempotencyRunRenewsLease 验证长任务会在 ttl/3 时按所有权 token 续租，
// 并在成功后重新获得完整 TTL 作为去重窗口。
func TestIdempotencyRunRenewsLease(t *testing.T) {
	redis, client := newMockRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	idempotency := redis.Idempotency("requests")
	var token string
	renewed := make(chan struct{})

	client.EXPECT().Do(ctx, matchIdempotencySet("app:requests:slow", "30", &token)).Return(rueidismock.Result(rueidismock.RedisString("OK")))
	client.EXPECT().Do(gomock.Any(), retryable(matchLuaArgs([]string{"app:requests:slow"}, func(args []string) bool {
		return len(args) == 2 && args[0] == token && args[1] == "30"
	}))).DoAndReturn(func(context.Context, rueidis.Completed) rueidis.RedisResult {
		close(renewed)
		return rueidismock.Result(rueidismock.RedisInt64(1))
	})
	client.EXPECT().Do(gomock.Any(), retryable(matchLuaArgs([]string{"app:requests:slow"}, func(args []string) bool {
		return len(args) == 2 && args[0] == token && args[1] == "30"
	}))).Return(rueidismock.Result(rueidismock.RedisInt64(1)))

	ran, err := idempotency.Run(ctx, "slow", 30*time.Millisecond, func(context.Context) error {
		select {
		case <-renewed:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil || !ran {
		t.Fatalf("ran=%t err=%v", ran, err)
	}
}

// TestIdempotencyRunCancelsOnLeaseLoss 验证所有权已被替换时停止业务 context，
// 返回 ErrIdempotencyLeaseLost，并且清理脚本仍只尝试删除旧 token。
func TestIdempotencyRunCancelsOnLeaseLoss(t *testing.T) {
	redis, client := newMockRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	idempotency := redis.Idempotency("requests")
	var token string

	client.EXPECT().Do(ctx, matchIdempotencySet("app:requests:lost", "30", &token)).Return(rueidismock.Result(rueidismock.RedisString("OK")))
	client.EXPECT().Do(gomock.Any(), retryable(matchLuaArgs([]string{"app:requests:lost"}, func(args []string) bool {
		return len(args) == 2 && args[0] == token && args[1] == "30"
	}))).Return(rueidismock.Result(rueidismock.RedisInt64(0)))
	client.EXPECT().Do(gomock.Any(), retryable(matchLuaArgs([]string{"app:requests:lost"}, func(args []string) bool {
		return len(args) == 1 && args[0] == token
	}))).Return(rueidismock.Result(rueidismock.RedisInt64(0)))

	ran, err := idempotency.Run(ctx, "lost", 30*time.Millisecond, func(handlerCtx context.Context) error {
		<-handlerCtx.Done()
		return context.Cause(handlerCtx)
	})
	if !ran || !errors.Is(err, ErrIdempotencyLeaseLost) {
		t.Fatalf("ran=%t err=%v", ran, err)
	}
}

// TestIdempotencyRunJoinsCleanupError 验证业务失败且按所有权清理幂等标记也失败时，
// 返回值同时保留两个错误，调用方既能识别原始业务异常，也能发现清理失败风险。
func TestIdempotencyRunJoinsCleanupError(t *testing.T) {
	// Arrange：SET NX 成功，但业务处理和回滚 DEL 分别返回不同错误。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	idempotency := redis.Idempotency("requests")
	workErr := errors.New("work failed")
	cleanupErr := errors.New("cleanup failed")
	var failedToken string
	client.EXPECT().Do(ctx, matchIdempotencySet("app:requests:failed", "1000", &failedToken)).Return(rueidismock.Result(rueidismock.RedisString("OK")))
	client.EXPECT().Do(gomock.Any(), retryable(matchLuaArgs([]string{"app:requests:failed"}, func(args []string) bool {
		return len(args) == 1 && args[0] == failedToken
	}))).Return(rueidismock.ErrorResult(cleanupErr))

	// Act/Assert：errors.Join 后两个错误都必须能被 errors.Is 单独识别。
	_, err := idempotency.Run(ctx, "failed", time.Second, func(context.Context) error { return workErr })
	if !errors.Is(err, workErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("joined error=%v", err)
	}
}
