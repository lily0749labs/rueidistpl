package rueidistpl

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eko/gocache/lib/v4/store"
	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

func TestCacheGetWithTTL(t *testing.T) {
	type profile struct {
		Name string `json:"name"`
	}
	transportErr := errors.New("connection lost")
	tests := []struct {
		name      string
		response  rueidis.RedisResult
		wantTTL   time.Duration
		wantErr   error
		wantMiss  bool
		wantError bool
	}{
		{name: "server TTL", response: rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString(`{"name":"Ada"}`), rueidismock.RedisInt64(600000))), wantTTL: 10 * time.Minute},
		{name: "millisecond precision", response: rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString(`{"name":"Ada"}`), rueidismock.RedisInt64(1250))), wantTTL: 1250 * time.Millisecond},
		{name: "last millisecond", response: rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString(`{"name":"Ada"}`), rueidismock.RedisInt64(0)))},
		{name: "persistent", response: rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString(`{"name":"Ada"}`), rueidismock.RedisInt64(-1))), wantTTL: -time.Millisecond},
		{name: "missing", response: rueidismock.Result(rueidismock.RedisNil()), wantMiss: true, wantError: true},
		{name: "missing TTL", response: rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString(`{"name":"Ada"}`), rueidismock.RedisInt64(-2))), wantMiss: true, wantError: true},
		{name: "invalid TTL", response: rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString(`{"name":"Ada"}`), rueidismock.RedisInt64(-3))), wantErr: ErrInvalidExpirationResponse, wantError: true},
		{name: "invalid JSON", response: rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString(`{"name":true}`), rueidismock.RedisInt64(1000))), wantError: true},
		{name: "invalid response", response: rueidismock.Result(rueidismock.RedisArray()), wantError: true},
		{name: "transport error", response: rueidismock.ErrorResult(transportErr), wantErr: transportErr, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := rueidismock.NewClient(gomock.NewController(t))
			cache := NewCache[profile](&Redis{client: client, keyPrefix: "app"}, "cache")
			ctx := context.Background()
			client.EXPECT().Do(ctx, rueidismock.MatchFn(func(command []string) bool {
				return len(command) == 4 && command[0] == "EVALSHA" && command[2] == "1" && command[3] == "app:cache:item"
			})).Return(test.response)
			value, ttl, err := cache.GetWithTTL(ctx, "item")
			if (err != nil) != test.wantError {
				t.Fatalf("GetWithTTL() error = %v, wantError %v", err, test.wantError)
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("GetWithTTL() error = %v, want %v", err, test.wantErr)
			}
			if test.wantMiss && (!IsNotFound(err) || !errors.Is(err, store.NotFound{})) {
				t.Fatalf("GetWithTTL() error = %v, want cache miss", err)
			}
			if test.wantError {
				if value != (profile{}) || ttl != 0 {
					t.Fatalf("GetWithTTL() = (%+v, %s), want zero values on error", value, ttl)
				}
			} else if value.Name != "Ada" || ttl != test.wantTTL {
				t.Fatalf("GetWithTTL() = (%+v, %s), want Ada, %s", value, ttl, test.wantTTL)
			}
		})
	}
}
