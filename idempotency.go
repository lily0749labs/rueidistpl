package rueidistpl

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

const idempotencyOperationTimeout = 5 * time.Second

// ErrIdempotencyLeaseLost 表示 Run 执行期间幂等标记已过期、被替换或无法续租。
// 收到该错误时不能假设业务未执行，调用方应依靠下游唯一约束或业务状态进行核对。
var ErrIdempotencyLeaseLost = errors.New("rueidistpl: idempotency lease lost")

var idempotencyForgetOwnedScript = rueidis.NewLuaScriptRetryable(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call("DEL", KEYS[1])`)

var idempotencyRefreshOwnedScript = rueidis.NewLuaScriptRetryable(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call("PEXPIRE", KEYS[1], ARGV[2])`)

// Idempotency 使用 SET NX PX 记录已接收的业务 ID。
type Idempotency struct {
	client rueidis.Client
	prefix string
}

// Idempotency 创建带独立 key 命名空间的幂等控制器。
func (redis *Redis) Idempotency(namespace string) *Idempotency {
	return &Idempotency{client: redis.client, prefix: redis.Key(namespace)}
}

// Try 首次写入返回 true；ID 已存在返回 false。
func (idempotency *Idempotency) Try(ctx context.Context, id string, ttl time.Duration) (bool, error) {
	return idempotency.try(ctx, id, ttl, "1")
}

func (idempotency *Idempotency) try(ctx context.Context, id string, ttl time.Duration, token string) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidExpiration
	}
	ttl = atLeastMillisecond(ttl)
	command := idempotency.client.B().Set().Key(idempotency.key(id)).Value(token).Nx().Px(ttl).Build()
	err := idempotency.client.Do(ctx, command).Error()
	if rueidis.IsRedisNil(err) {
		return false, nil
	}
	return err == nil, err
}

// Run 仅为首次出现的 ID 执行 fn，并在执行期间按 ttl/3 自动续租。
// fn 必须使用收到的 context；续租失败时该 context 会以 ErrIdempotencyLeaseLost 为 cause 取消。
// fn 成功后会把标记有效期重新设置为完整 ttl；fn 失败时只删除本次持有的标记以允许重试。
// Redis 幂等标记不能替代数据库唯一约束，也不提供跨 Redis 与外部系统的恰好一次事务。
func (idempotency *Idempotency) Run(ctx context.Context, id string, ttl time.Duration, fn func(context.Context) error) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidExpiration
	}
	ttl = atLeastMillisecond(ttl)
	token, err := newOwnershipToken()
	if err != nil {
		return false, err
	}
	acquired, err := idempotency.try(ctx, id, ttl, token)
	if err != nil || !acquired {
		return acquired, err
	}
	if fn == nil {
		return true, nil
	}

	workCtx, cancelWork := context.WithCancelCause(ctx)
	defer cancelWork(nil)
	stopRenewal := make(chan struct{})
	renewalDone := make(chan error, 1)
	go idempotency.renewOwnedUntilStopped(workCtx, cancelWork, stopRenewal, renewalDone, id, token, ttl)

	workErr := fn(workCtx)
	close(stopRenewal)
	renewalErr := <-renewalDone
	cancelWork(nil)
	if workErr != nil || renewalErr != nil {
		forgetErr := idempotency.forgetOwnedWithTimeout(ctx, id, token, ttl)
		return true, errors.Join(workErr, renewalErr, forgetErr)
	}

	finalizeCtx, cancelFinalize := context.WithTimeout(context.WithoutCancel(ctx), idempotencyCommandTimeout(ttl))
	defer cancelFinalize()
	refreshed, err := idempotency.refreshOwned(finalizeCtx, id, token, ttl)
	if err != nil {
		return true, errors.Join(ErrIdempotencyLeaseLost, fmt.Errorf("finalize idempotency marker: %w", err))
	}
	if !refreshed {
		return true, ErrIdempotencyLeaseLost
	}
	return true, nil
}

func (idempotency *Idempotency) renewOwnedUntilStopped(
	ctx context.Context,
	cancelWork context.CancelCauseFunc,
	stop <-chan struct{},
	done chan<- error,
	id, token string,
	ttl time.Duration,
) {
	interval := ttl / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-stop:
			done <- nil
			return
		case <-ctx.Done():
			done <- nil
			return
		case <-timer.C:
			renewCtx, cancelRenew := context.WithTimeout(context.WithoutCancel(ctx), idempotencyCommandTimeout(ttl))
			refreshed, err := idempotency.refreshOwned(renewCtx, id, token, ttl)
			cancelRenew()
			if err != nil {
				leaseErr := errors.Join(ErrIdempotencyLeaseLost, fmt.Errorf("renew idempotency marker: %w", err))
				cancelWork(leaseErr)
				done <- leaseErr
				return
			}
			if !refreshed {
				cancelWork(ErrIdempotencyLeaseLost)
				done <- ErrIdempotencyLeaseLost
				return
			}
			timer.Reset(interval)
		}
	}
}

func (idempotency *Idempotency) refreshOwned(ctx context.Context, id, token string, ttl time.Duration) (bool, error) {
	refreshed, err := idempotencyRefreshOwnedScript.Exec(
		ctx,
		idempotency.client,
		[]string{idempotency.key(id)},
		[]string{token, fmt.Sprintf("%d", ttl.Milliseconds())},
	).AsInt64()
	return refreshed == 1, err
}

func (idempotency *Idempotency) forgetOwnedWithTimeout(ctx context.Context, id, token string, ttl time.Duration) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), idempotencyCommandTimeout(ttl))
	defer cancel()
	return idempotency.forgetOwned(cleanupCtx, id, token)
}

func idempotencyCommandTimeout(ttl time.Duration) time.Duration {
	timeout := ttl / 3
	if timeout < time.Millisecond {
		timeout = time.Millisecond
	}
	if timeout > idempotencyOperationTimeout {
		timeout = idempotencyOperationTimeout
	}
	return timeout
}

// Exists 判断业务 ID 的幂等标记是否仍然有效。
func (idempotency *Idempotency) Exists(ctx context.Context, id string) (bool, error) {
	count, err := idempotency.client.Do(ctx, idempotency.client.B().Exists().Key(idempotency.key(id)).Build()).AsInt64()
	return count > 0, err
}

// Forget 主动删除幂等标记，使相同 ID 可以再次执行。
func (idempotency *Idempotency) Forget(ctx context.Context, id string) error {
	return idempotency.client.Do(ctx, idempotency.client.B().Del().Key(idempotency.key(id)).Build()).Error()
}

func (idempotency *Idempotency) forgetOwned(ctx context.Context, id, token string) error {
	return idempotencyForgetOwnedScript.Exec(
		ctx,
		idempotency.client,
		[]string{idempotency.key(id)},
		[]string{token},
	).Error()
}

func (idempotency *Idempotency) key(id string) string {
	return joinKey(idempotency.prefix, id)
}
