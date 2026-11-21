package rueidistpl

import (
	"context"
	"errors"
	"testing"
	"time"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestDelayQueueLifecycle 验证延迟队列的核心命令契约：
//  1. Schedule 的 Lua 同时收到四个同 slot key、任务 ID、JSON 和毫秒时间；
//  2. Poll 将扁平 Lua 响应还原为强类型任务；
//  3. Remove、Len、InFlight、Clear 始终操作对应的真实 key。
func TestDelayQueueLifecycle(t *testing.T) {
	// Arrange：固定毫秒时间和 JSON 载荷，确保 Lua ARGV 可以精确匹配。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	queue := NewDelayQueue[unitUser](redis, "jobs")
	valuesKey, scoresKey := queue.Keys()
	processingKey, claimsKey := queue.ReliableKeys()
	alice := unitUser{ID: 1, Name: "Alice"}
	payload := `{"id":1,"name":"Alice"}`
	dueAt := time.UnixMilli(1000)
	now := time.UnixMilli(2000)

	// mock 不依赖脚本 SHA，只核对两个同 slot key 及每个业务参数的顺序。
	// Poll 和 Remove 会删除数据，必须额外确认命令没有自动重试标记。
	gomock.InOrder(
		client.EXPECT().Do(ctx, matchLua(
			[]string{valuesKey, scoresKey, processingKey, claimsKey},
			[]string{"job-1", payload, "1000"},
		)).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, nonRetryable(matchLua(
			[]string{valuesKey, scoresKey},
			[]string{"2000", "10"},
		))).Return(rueidismock.Result(redisStrings("job-1", "1000", payload))),
		client.EXPECT().Do(ctx, nonRetryable(matchLua(
			[]string{valuesKey, scoresKey, processingKey, claimsKey},
			[]string{"job-2"},
		))).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZCARD", scoresKey)).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZCARD", processingKey)).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", valuesKey, scoresKey, processingKey, claimsKey)).Return(rueidismock.Result(rueidismock.RedisInt64(4))),
	)

	// Act/Assert：调度后轮询到期任务，确认 ID、时间和强类型载荷完整还原。
	scheduleErr := queue.Schedule(ctx, "job-1", dueAt, alice)
	if scheduleErr != nil {
		t.Fatal(scheduleErr)
	}
	items, err := queue.Poll(ctx, now, 10)
	if err != nil || len(items) != 1 || items[0] != (Delayed[unitUser]{ID: "job-1", DueAt: dueAt, Value: alice}) {
		t.Fatalf("Poll items=%+v err=%v", items, err)
	}
	// 再验证移除、计数和清空分别使用正确的内部 key。
	removed, removeErr := queue.Remove(ctx, "job-2")
	if removeErr != nil || !removed {
		t.Fatalf("Remove value=%t err=%v", removed, removeErr)
	}
	queueLength, lenErr := queue.Len(ctx)
	if lenErr != nil || queueLength != 1 {
		t.Fatalf("Len value=%d err=%v", queueLength, lenErr)
	}
	inFlight, inFlightErr := queue.InFlight(ctx)
	if inFlightErr != nil || inFlight != 2 {
		t.Fatalf("InFlight value=%d err=%v", inFlight, inFlightErr)
	}
	clearErr := queue.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestDelayQueueReliableLifecycle 验证可靠消费使用 Redis 时间和所有权 token，
// 并覆盖 Claim、Ack、Nack、Extend 的完整命令与类型解码契约。
func TestDelayQueueReliableLifecycle(t *testing.T) {
	redis, client := newMockRedis(t)
	ctx := context.Background()
	queue := NewDelayQueue[unitUser](redis, "jobs")
	valuesKey, scoresKey := queue.Keys()
	processingKey, claimsKey := queue.ReliableKeys()
	payload := `{"id":1,"name":"Alice"}`
	claimToken := "claim-token:1"

	gomock.InOrder(
		client.EXPECT().Do(ctx, nonRetryable(matchLuaArgs(
			[]string{valuesKey, scoresKey, processingKey, claimsKey},
			func(args []string) bool {
				return len(args) == 3 && args[0] == "10" && args[1] == "30000" && len(args[2]) == 32
			},
		))).Return(rueidismock.Result(redisStrings("job-1", "1000", payload, claimToken))),
		client.EXPECT().Do(ctx, nonRetryable(matchLua(
			[]string{valuesKey, processingKey, claimsKey},
			[]string{"job-1", claimToken},
		))).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, nonRetryable(matchLua(
			[]string{scoresKey, processingKey, claimsKey},
			[]string{"job-2", "claim-token:2", "2000"},
		))).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, nonRetryable(matchLua(
			[]string{processingKey, claimsKey},
			[]string{"job-3", "claim-token:3", "45000"},
		))).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	items, err := queue.Claim(ctx, 30*time.Second, 10)
	want := ClaimedDelayed[unitUser]{
		Delayed: Delayed[unitUser]{ID: "job-1", DueAt: time.UnixMilli(1000), Value: unitUser{ID: 1, Name: "Alice"}},
		Token:   claimToken,
	}
	if err != nil || len(items) != 1 || items[0] != want {
		t.Fatalf("Claim items=%+v err=%v", items, err)
	}
	acked, err := queue.Ack(ctx, "job-1", claimToken)
	if err != nil || !acked {
		t.Fatalf("Ack value=%t err=%v", acked, err)
	}
	requeued, err := queue.Nack(ctx, "job-2", "claim-token:2", time.UnixMilli(2000))
	if err != nil || !requeued {
		t.Fatalf("Nack value=%t err=%v", requeued, err)
	}
	extended, err := queue.Extend(ctx, "job-3", "claim-token:3", 45*time.Second)
	if err != nil || !extended {
		t.Fatalf("Extend value=%t err=%v", extended, err)
	}
}

// TestDelayQueueValidationAndDecodeErrors 验证队列面对无效输入和异常 Lua 响应时不会静默成功：
// limit 非正直接返回空结果，超过安全上限直接拒绝；不支持 JSON 的载荷、残缺响应、非法时间分数和脏 JSON 都返回错误。
func TestDelayQueueValidationAndDecodeErrors(t *testing.T) {
	// 第一组边界不需要客户端：limit 非正、超限和 JSON 编码失败都应在本地返回。
	ctx := context.Background()
	queue := &DelayQueue[unitUser]{}
	pollItems, pollErr := queue.Poll(ctx, time.Now(), 0)
	if pollErr != nil || len(pollItems) != 0 {
		t.Fatalf("Poll items=%+v err=%v", pollItems, pollErr)
	}
	oversizedItems, oversizedErr := queue.Poll(ctx, time.Now(), MaxDelayQueuePollBatch+1)
	if oversizedItems != nil || !errors.Is(oversizedErr, ErrDelayQueuePollLimit) {
		t.Fatalf("oversized Poll items=%+v err=%v", oversizedItems, oversizedErr)
	}
	claimed, claimErr := queue.Claim(ctx, 0, 1)
	if claimed != nil || !errors.Is(claimErr, ErrInvalidExpiration) {
		t.Fatalf("Claim items=%+v err=%v", claimed, claimErr)
	}
	if extended, extendErr := queue.Extend(ctx, "job", "token", 0); extended || !errors.Is(extendErr, ErrInvalidExpiration) {
		t.Fatalf("Extend value=%t err=%v", extended, extendErr)
	}
	badQueue := &DelayQueue[chan int]{}
	scheduleErr := badQueue.Schedule(ctx, "bad", time.Now(), make(chan int))
	if scheduleErr == nil {
		t.Fatal("Schedule should reject values unsupported by JSON")
	}

	// 第二组通过 mock 依次注入三类不可信 Lua 响应。
	redis, client := newMockRedis(t)
	queue = NewDelayQueue[unitUser](redis, "jobs")
	valuesKey, scoresKey := queue.Keys()
	args := []string{"2000", "10"}
	gomock.InOrder(
		client.EXPECT().Do(ctx, matchLua([]string{valuesKey, scoresKey}, args)).Return(rueidismock.Result(redisStrings("incomplete"))),
		client.EXPECT().Do(ctx, matchLua([]string{valuesKey, scoresKey}, args)).Return(rueidismock.Result(redisStrings("job-1", "bad-score", `{}`))),
		client.EXPECT().Do(ctx, matchLua([]string{valuesKey, scoresKey}, args)).Return(rueidismock.Result(redisStrings("job-1", "1000", "not-json"))),
	)
	// 响应长度不是三的倍数，说明 ID/score/payload 三元组不完整。
	_, pollErr2 := queue.Poll(ctx, time.UnixMilli(2000), 10)
	if pollErr2 == nil {
		t.Fatal("Poll should reject incomplete responses")
	}
	// score 不是整数毫秒时不能构造一个错误的 DueAt。
	_, pollErr3 := queue.Poll(ctx, time.UnixMilli(2000), 10)
	if pollErr3 == nil {
		t.Fatal("Poll should reject invalid scores")
	}
	// payload 不是合法 JSON 时必须暴露脏任务，而不是返回对象零值。
	_, pollErr4 := queue.Poll(ctx, time.UnixMilli(2000), 10)
	if pollErr4 == nil {
		t.Fatal("Poll should reject invalid JSON")
	}
}
