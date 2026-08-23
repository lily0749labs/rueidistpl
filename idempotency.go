package rueidistpl

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/rueidis"
)

const idempotencyCleanupTimeout = 5 * time.Second

var idempotencyForgetOwnedScript = rueidis.NewLuaScriptRetryable(`
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call("DEL", KEYS[1])`)

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

// Run 仅为首次出现的 ID 执行 fn；fn 失败时只删除本次执行持有的标记以允许重试。
// 即使执行期间 TTL 到期且其他请求重新取得标记，旧请求也不会误删新标记。
func (idempotency *Idempotency) Run(ctx context.Context, id string, ttl time.Duration, fn func(context.Context) error) (bool, error) {
	if ttl <= 0 {
		return false, ErrInvalidExpiration
	}
	token, err := newIdempotencyToken()
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
	if err := fn(ctx); err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), idempotencyCleanupTimeout)
		defer cancel()
		forgetErr := idempotency.forgetOwned(cleanupCtx, id, token)
		return true, errors.Join(err, forgetErr)
	}
	return true, nil
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

func newIdempotencyToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create idempotency ownership token: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
