package rueidistpl

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

func TestNewAppliesDefaultsAndCloseIsIdempotent(t *testing.T) {
	controller := gomock.NewController(t)
	client := rueidismock.NewClient(controller)
	client.EXPECT().Close().Times(1)

	originalFactory := rueidisClientFactory
	t.Cleanup(func() { rueidisClientFactory = originalFactory })
	var receivedOption rueidis.ClientOption
	rueidisClientFactory = func(option rueidis.ClientOption) (rueidis.Client, error) {
		receivedOption = option
		return client, nil
	}

	redis, err := New(Config{
		ClientOption: rueidis.ClientOption{InitAddress: []string{"redis.example:6379"}},
		KeyPrefix:    "app",
		CacheTTL:     time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("New() unexpected error: %v", err)
	}
	if !reflect.DeepEqual(receivedOption.InitAddress, []string{"redis.example:6379"}) {
		t.Fatalf("New() passed InitAddress %v, want redis.example:6379", receivedOption.InitAddress)
	}
	if redis.keyPrefix != "app" {
		t.Fatalf("New() key prefix = %q, want app", redis.keyPrefix)
	}
	if redis.lockerOption.KeyPrefix != "app:locks" {
		t.Fatalf("New() locker key prefix = %q, want app:locks", redis.lockerOption.KeyPrefix)
	}
	if redis.lockerOption.KeyMajority != 2 {
		t.Fatalf("New() locker key majority = %d, want 2", redis.lockerOption.KeyMajority)
	}

	redis.Close()
	redis.Close()
	if _, err := redis.getLocker(); !errors.Is(err, ErrClosed) {
		t.Fatalf("getLocker() after Close error = %v, want ErrClosed", err)
	}
}

func TestRedisStringCommandsApplyPrefixAndTTL(t *testing.T) {
	controller := gomock.NewController(t)
	client := rueidismock.NewClient(controller)
	redis := &Redis{client: client, keyPrefix: "app"}
	ctx := context.Background()

	client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:token", "value", "PX", "1")).
		Return(rueidismock.Result(rueidismock.RedisString("OK")))
	if err := redis.SetString(ctx, "token", "value", time.Nanosecond); err != nil {
		t.Fatalf("SetString() unexpected error: %v", err)
	}

	client.EXPECT().Do(ctx, rueidismock.Match("GET", "app:token")).
		Return(rueidismock.Result(rueidismock.RedisBlobString("value")))
	got, err := redis.GetString(ctx, "token")
	if err != nil {
		t.Fatalf("GetString() unexpected error: %v", err)
	}
	if got != "value" {
		t.Fatalf("GetString() = %q, want value", got)
	}

	client.EXPECT().Do(ctx, rueidismock.Match("PING")).
		Return(rueidismock.Result(rueidismock.RedisString("PONG")))
	if err := redis.Ping(ctx); err != nil {
		t.Fatalf("Ping() unexpected error: %v", err)
	}
}

func TestValueCommandsEncodeAndDecodeJSON(t *testing.T) {
	type profile struct {
		Name string `json:"name"`
	}

	controller := gomock.NewController(t)
	client := rueidismock.NewClient(controller)
	value := NewValue[profile](&Redis{client: client, keyPrefix: "app"}, "profile")
	ctx := context.Background()

	client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:profile", `{"name":"Ada"}`, "PX", "1")).
		Return(rueidismock.Result(rueidismock.RedisString("OK")))
	if err := value.Set(ctx, profile{Name: "Ada"}, time.Nanosecond); err != nil {
		t.Fatalf("Value.Set() unexpected error: %v", err)
	}

	client.EXPECT().Do(ctx, rueidismock.Match("GET", "app:profile")).
		Return(rueidismock.Result(rueidismock.RedisBlobString(`{"name":"Ada"}`)))
	got, err := value.Get(ctx)
	if err != nil {
		t.Fatalf("Value.Get() unexpected error: %v", err)
	}
	if got != (profile{Name: "Ada"}) {
		t.Fatalf("Value.Get() = %#v, want Ada", got)
	}

	client.EXPECT().Do(ctx, rueidismock.Match("SET", "app:profile", `{"name":"Ada"}`, "NX")).
		Return(rueidismock.Result(rueidismock.RedisNil()))
	created, err := value.SetNX(ctx, profile{Name: "Ada"}, 0)
	if err != nil {
		t.Fatalf("Value.SetNX() unexpected error: %v", err)
	}
	if created {
		t.Fatal("Value.SetNX() = true on Redis Nil, want false")
	}
}

func TestDeleteEachPreservesPartialResults(t *testing.T) {
	controller := gomock.NewController(t)
	client := rueidismock.NewClient(controller)
	redis := &Redis{client: client, keyPrefix: "app"}
	ctx := context.Background()
	wantErr := errors.New("redis unavailable")

	client.EXPECT().DoMulti(
		ctx,
		rueidismock.Match("DEL", "app:a"),
		rueidismock.Match("DEL", "app:b"),
		rueidismock.Match("DEL", "app:c"),
	).Return([]rueidis.RedisResult{
		rueidismock.Result(rueidismock.RedisInt64(1)),
		rueidismock.ErrorResult(wantErr),
		rueidismock.Result(rueidismock.RedisInt64(0)),
	})

	got := redis.DeleteEach(ctx, "a", "b", "c")
	if got.Deleted != 1 {
		t.Fatalf("DeleteEach().Deleted = %d, want 1", got.Deleted)
	}
	if !errors.Is(got.Err, wantErr) {
		t.Fatalf("DeleteEach().Err = %v, want wrapped Redis error", got.Err)
	}
	if len(got.Results) != 3 {
		t.Fatalf("DeleteEach() returned %d results, want 3", len(got.Results))
	}
	if got.Results[0].Key != "a" || !got.Results[0].Deleted || got.Results[0].Err != nil {
		t.Fatalf("DeleteEach() first result = %#v, want successful deletion", got.Results[0])
	}
	if got.Results[1].Key != "b" || !errors.Is(got.Results[1].Err, wantErr) {
		t.Fatalf("DeleteEach() second result = %#v, want wrapped error", got.Results[1])
	}
	if got.Results[2].Key != "c" || got.Results[2].Deleted || got.Results[2].Err != nil {
		t.Fatalf("DeleteEach() third result = %#v, want successful miss", got.Results[2])
	}
}

func TestBooleanBatchResultsRejectsInvalidResponses(t *testing.T) {
	results := make([]error, 2)
	count, err := booleanBatchResults("EXISTS", []string{"a", "b"}, []rueidis.RedisResult{
		rueidismock.Result(rueidismock.RedisInt64(2)),
	}, func(index int, value bool, err error) {
		results[index] = err
	})
	if count != 0 {
		t.Fatalf("booleanBatchResults() count = %d, want 0", count)
	}
	if !errors.Is(err, ErrInvalidBatchResponse) {
		t.Fatalf("booleanBatchResults() error = %v, want ErrInvalidBatchResponse", err)
	}
	if results[0] == nil || results[1] == nil {
		t.Fatalf("booleanBatchResults() results = %#v, want invalid and missing errors", results)
	}
}
