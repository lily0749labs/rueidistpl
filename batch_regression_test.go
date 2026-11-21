package rueidistpl

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

func TestDeleteEachChunksAndPreservesPartialResults(t *testing.T) {
	client := rueidismock.NewClient(gomock.NewController(t), rueidismock.WithSlotCheck())
	redis := &Redis{client: client, keyPrefix: "app"}
	ctx := context.Background()
	keys := make([]string, keyBatchSize+3)
	for index := range keys {
		keys[index] = fmt.Sprintf("key-%d", index)
	}
	wantErr := errors.New("first chunk failed for one key")
	offset := 0
	client.EXPECT().DoMulti(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, commands ...rueidis.Completed) []rueidis.RedisResult {
		wantSize := min(keyBatchSize, len(keys)-offset)
		if len(commands) != wantSize {
			t.Fatalf("DoMulti() received %d commands, want %d", len(commands), wantSize)
		}
		responses := make([]rueidis.RedisResult, len(commands))
		for index, command := range commands {
			if !reflect.DeepEqual(command.Commands(), []string{"DEL", "app:" + keys[offset+index]}) {
				t.Fatalf("command %d = %v, want deletion of %s", offset+index, command.Commands(), keys[offset+index])
			}
			responses[index] = rueidismock.Result(rueidismock.RedisInt64(1))
		}
		if offset == 0 {
			responses[keyBatchSize-1] = rueidismock.ErrorResult(wantErr)
		}
		offset += len(commands)
		return responses
	}).Times(2)
	got := redis.DeleteEach(ctx, keys...)
	if got.Deleted != int64(len(keys)-1) || !errors.Is(got.Err, wantErr) || offset != len(keys) {
		t.Fatalf("DeleteEach() count=%d err=%v processed=%d", got.Deleted, got.Err, offset)
	}
	for index, result := range got.Results {
		if result.Key != keys[index] {
			t.Fatalf("result %d key = %q, want %q", index, result.Key, keys[index])
		}
		if index == keyBatchSize-1 {
			if result.Deleted || !errors.Is(result.Err, wantErr) {
				t.Fatalf("failed key result = %+v", result)
			}
		} else if !result.Deleted || result.Err != nil {
			t.Fatalf("successful key result = %+v", result)
		}
	}
}

func TestExistsEachChunksAndCountsDuplicateInputs(t *testing.T) {
	client := rueidismock.NewClient(gomock.NewController(t))
	redis := &Redis{client: client, keyPrefix: "app"}
	keys := make([]string, keyBatchSize+1)
	for index := range keys {
		keys[index] = "duplicate"
	}
	ctx := context.Background()
	client.EXPECT().DoMulti(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, commands ...rueidis.Completed) []rueidis.RedisResult {
		responses := make([]rueidis.RedisResult, len(commands))
		for index, command := range commands {
			if !reflect.DeepEqual(command.Commands(), []string{"EXISTS", "app:duplicate"}) {
				t.Fatalf("unexpected command: %v", command.Commands())
			}
			responses[index] = rueidismock.Result(rueidismock.RedisInt64(1))
		}
		return responses
	}).Times(2)
	got := redis.ExistsEach(ctx, keys...)
	if got.Err != nil || got.Existing != int64(len(keys)) || len(got.Results) != len(keys) {
		t.Fatalf("ExistsEach() count=%d err=%v results=%d", got.Existing, got.Err, len(got.Results))
	}
	for _, result := range got.Results {
		if result.Key != "duplicate" || !result.Exists || result.Err != nil {
			t.Fatalf("unexpected existence result: %+v", result)
		}
	}
}

func TestDeleteEachStopsAfterCancellation(t *testing.T) {
	client := rueidismock.NewClient(gomock.NewController(t))
	redis := &Redis{client: client}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	keys := make([]string, keyBatchSize+1)
	client.EXPECT().DoMulti(ctx, gomock.Any()).DoAndReturn(func(_ context.Context, commands ...rueidis.Completed) []rueidis.RedisResult {
		cancel()
		responses := make([]rueidis.RedisResult, len(commands))
		for index := range responses {
			responses[index] = rueidismock.Result(rueidismock.RedisInt64(0))
		}
		return responses
	}).Times(1)
	got := redis.DeleteEach(ctx, keys...)
	if got.Deleted != 0 || !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("DeleteEach() count=%d err=%v, want cancellation", got.Deleted, got.Err)
	}
	if !errors.Is(got.Results[keyBatchSize].Err, context.Canceled) || got.Results[0].Err != nil {
		t.Fatalf("DeleteEach() did not preserve success and cancellation details")
	}
}

func TestEmptyBatchesDoNotCallRedis(t *testing.T) {
	client := rueidismock.NewClient(gomock.NewController(t))
	redis := &Redis{client: client}
	ctx := context.Background()
	deleted := redis.DeleteEach(ctx)
	existing := redis.ExistsEach(ctx)
	if deleted.Results == nil || existing.Results == nil || deleted.Err != nil || existing.Err != nil || deleted.Deleted != 0 || existing.Existing != 0 {
		t.Fatalf("empty batches returned unexpected results")
	}
}
