package rueidistpl

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrInvalidExpirationResponse 表示 Redis 返回了 PTTL/HPTTL 约定之外的负数状态码。
var ErrInvalidExpirationResponse = errors.New("rueidistpl: invalid expiration response")

// ExpirationInfo 是已经解释过 Redis 特殊 TTL 状态的结构化结果。
//
// Exists=false 表示 key 或 field 不存在；Persistent=true 表示对象存在但没有过期时间；
// 只有 Exists=true 且 Persistent=false 时，TTL 才表示剩余有效期。
type ExpirationInfo struct {
	Exists     bool
	Persistent bool
	TTL        time.Duration
}

// Expiration 返回逻辑 key 的结构化过期状态，调用方无需再解释 PTTL 的 -1ms/-2ms。
func (redis *Redis) Expiration(ctx context.Context, key string) (ExpirationInfo, error) {
	milliseconds, err := redis.client.Do(ctx, redis.client.B().Pttl().Key(redis.Key(key)).Build()).AsInt64()
	if err != nil {
		return ExpirationInfo{}, err
	}
	return expirationInfo(milliseconds)
}

func expirationInfo(milliseconds int64) (ExpirationInfo, error) {
	switch {
	case milliseconds >= 0:
		return ExpirationInfo{Exists: true, TTL: time.Duration(milliseconds) * time.Millisecond}, nil
	case milliseconds == -1:
		return ExpirationInfo{Exists: true, Persistent: true}, nil
	case milliseconds == -2:
		return ExpirationInfo{}, nil
	default:
		return ExpirationInfo{}, fmt.Errorf("%w: %dms", ErrInvalidExpirationResponse, milliseconds)
	}
}
