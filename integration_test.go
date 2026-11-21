package rueidistpl_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lily0749labs/rueidistpl"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidislock"
)

type integrationUser struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func envOr(name, fallback string) string {
	lookupEnvValue, lookupEnvOk := os.LookupEnv(name)
	if lookupEnvOk {
		return lookupEnvValue
	}
	return fallback
}

// TestRedisIntegration 使用真实 Redis 验证 mock 无法覆盖的端到端行为：
// 客户端缓存、Lua 原子性、Hash field TTL、Stream 消费组、Pub/Sub 并发收发和分布式锁。
// 测试默认跳过，只有显式设置 RUEIDISTPL_INTEGRATION=1 时才会运行。
func TestRedisIntegration(t *testing.T) {
	// 只有显式开启时才连接本机 Redis，普通 go test 始终保持纯单元测试属性。
	if os.Getenv("RUEIDISTPL_INTEGRATION") != "1" {
		t.Skip("set RUEIDISTPL_INTEGRATION=1 to run Redis integration test")
	}

	// 每次运行使用唯一前缀，避免并行执行或上次异常退出造成数据互相污染。
	prefix := fmt.Sprintf("rueidistpl:test:%d", time.Now().UnixNano())
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{envOr("REDIS_ADDR", "127.0.0.1:16479")},
			Password:    envOr("REDIS_PASSWORD", "redis1234"),
		},
		KeyPrefix:          prefix,
		CacheTTL:           time.Minute,
		ClientSideCacheTTL: time.Second,
		LockerOption: rueidislock.LockerOption{
			KeyPrefix:   prefix + ":locks",
			KeyMajority: 1,
			KeyValidity: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pingErr := redisClient.Ping(ctx)
	if pingErr != nil {
		t.Fatal(pingErr)
	}

	// 一次性构造全部门面，后续按结构类型验证真实 Redis 行为。
	cache := rueidistpl.NewCache[integrationUser](redisClient, "cache:user")
	valueStore := rueidistpl.NewValue[integrationUser](redisClient, "value:user")
	hash := rueidistpl.NewHash[integrationUser](redisClient, "users")
	ranking := rueidistpl.NewRanking[integrationUser](redisClient, "ranking")
	typedList := rueidistpl.NewList[integrationUser](redisClient, "typed-list")
	typedSet := rueidistpl.NewSet[integrationUser](redisClient, "typed-set")
	sortedSet := redisClient.SortedSet("scores")
	stream := rueidistpl.NewStream[integrationUser](redisClient, "events")
	pubsub := rueidistpl.NewPubSub[integrationUser](redisClient, "notifications")
	idempotency := redisClient.Idempotency("idempotency")
	delayQueue := rueidistpl.NewDelayQueue[integrationUser](redisClient, "delayed")
	// 无论中途哪一步失败，都尽量清理本次唯一前缀下创建的数据。
	defer func() {
		_ = cache.Delete(ctx, "1")
		_, _ = valueStore.Delete(ctx)
		_ = hash.Clear(ctx)
		_ = ranking.Clear(ctx)
		_ = typedList.Clear(ctx)
		_ = typedSet.Clear(ctx)
		_ = sortedSet.Clear(ctx)
		_ = stream.Clear(ctx)
		_ = idempotency.Forget(ctx, "event-1")
		_ = idempotency.Forget(ctx, "failed-event")
		_ = idempotency.Forget(ctx, "ownership-event")
		_ = delayQueue.Clear(ctx)
		_, _ = redisClient.Delete(ctx, "string", "count", "list", "set")
	}()

	// 从对象 Cache 和单值 Value 开始，验证 JSON、TTL 与未命中语义。
	alice := integrationUser{ID: 1, Name: "Alice"}
	setErr := cache.Set(ctx, "1", alice)
	if setErr != nil {
		t.Fatal(setErr)
	}
	cached, ttl, err := cache.GetWithTTL(ctx, "1")
	if err != nil || cached != alice || ttl <= 0 {
		t.Fatalf("cache: value=%+v ttl=%s err=%v", cached, ttl, err)
	}
	_, getErr := cache.Get(ctx, "missing")
	if !rueidistpl.IsNotFound(getErr) {
		t.Fatalf("cache miss: got %v", getErr)
	}

	stored, err := valueStore.SetNX(ctx, alice, time.Minute)
	if err != nil || !stored {
		t.Fatalf("value setnx: stored=%t err=%v", stored, err)
	}
	stored, err = valueStore.SetNX(ctx, integrationUser{ID: 9}, time.Minute)
	if err != nil || stored {
		t.Fatalf("value duplicate setnx: stored=%t err=%v", stored, err)
	}
	storedValue, err := valueStore.Get(ctx)
	if err != nil || storedValue != alice {
		t.Fatalf("value get: value=%+v err=%v", storedValue, err)
	}
	valueTTL, err := valueStore.TTL(ctx)
	if err != nil || valueTTL <= 0 {
		t.Fatalf("value ttl: ttl=%s err=%v", valueTTL, err)
	}
	// 结构化结果必须将正常 TTL 与永久/不存在状态分离，业务层不再解释负数哨兵值。
	valueExpiration, valueExpirationErr := valueStore.Expiration(ctx)
	if valueExpirationErr != nil || !valueExpiration.Exists || valueExpiration.Persistent || valueExpiration.TTL <= 0 {
		t.Fatalf("value expiration: value=%+v err=%v", valueExpiration, valueExpirationErr)
	}
	// GETDEL 必须在一条 Redis 命令中返回原对象并删除 key，避免 GET + DEL 之间的并发竞态。
	deletedValue, getDeleteErr := valueStore.GetDelete(ctx)
	if getDeleteErr != nil || deletedValue != alice {
		t.Fatalf("value get-delete: value=%+v err=%v", deletedValue, getDeleteErr)
	}
	// Value 使用 Rueidis Nil，也应与 gocache 未命中一样通过 IsNotFound 判断。
	missingValue, missingValueErr := valueStore.Get(ctx)
	if missingValue != (integrationUser{}) || !rueidistpl.IsNotFound(missingValueErr) {
		t.Fatalf("value miss: value=%+v err=%v", missingValue, missingValueErr)
	}
	missingExpiration, missingExpirationErr := valueStore.Expiration(ctx)
	if missingExpirationErr != nil || missingExpiration.Exists || missingExpiration.Persistent || missingExpiration.TTL != 0 {
		t.Fatalf("missing value expiration: value=%+v err=%v", missingExpiration, missingExpirationErr)
	}

	// Hash 阶段同时覆盖批量对象、扫描、数值 field 和 Redis 7.4+ field TTL。
	users := map[string]integrationUser{
		"1": alice,
		"2": {ID: 2, Name: "Bob"},
	}
	setManyErr := hash.SetMany(ctx, users)
	if setManyErr != nil {
		t.Fatal(setManyErr)
	}
	allUsers, err := hash.All(ctx)
	if err != nil || len(allUsers) != 2 || allUsers["2"].Name != "Bob" {
		t.Fatalf("hash all: value=%+v err=%v", allUsers, err)
	}
	exists, err := hash.Exists(ctx, "1")
	if err != nil || !exists {
		t.Fatalf("hash exists: value=%t err=%v", exists, err)
	}
	setNX, err := hash.SetNX(ctx, "3", integrationUser{ID: 3, Name: "Carol"})
	if err != nil || !setNX {
		t.Fatalf("hash setnx: stored=%t err=%v", setNX, err)
	}
	setNX, err = hash.SetNX(ctx, "3", integrationUser{ID: 30})
	if err != nil || setNX {
		t.Fatalf("hash duplicate setnx: stored=%t err=%v", setNX, err)
	}
	keys, err := hash.Keys(ctx)
	if err != nil || len(keys) != 3 {
		t.Fatalf("hash keys: value=%v err=%v", keys, err)
	}
	values, err := hash.Values(ctx)
	if err != nil || len(values) != 3 {
		t.Fatalf("hash values: value=%+v err=%v", values, err)
	}
	scan, err := hash.Scan(ctx, 0, "*", 100)
	if err != nil || scan.Cursor != 0 || len(scan.Values) != 3 {
		t.Fatalf("hash scan: value=%+v err=%v", scan, err)
	}
	expired, err := hash.ExpireField(ctx, "3", time.Minute)
	if err != nil || !expired {
		t.Fatalf("hash field expire: value=%t err=%v", expired, err)
	}
	fieldTTL, err := hash.TTLField(ctx, "3")
	if err != nil || fieldTTL <= 0 {
		t.Fatalf("hash field ttl: value=%s err=%v", fieldTTL, err)
	}
	fieldExpiration, fieldExpirationErr := hash.ExpirationField(ctx, "3")
	if fieldExpirationErr != nil || !fieldExpiration.Exists || fieldExpiration.Persistent || fieldExpiration.TTL <= 0 {
		t.Fatalf("hash field expiration: value=%+v err=%v", fieldExpiration, fieldExpirationErr)
	}
	persisted, err := hash.PersistField(ctx, "3")
	if err != nil || !persisted {
		t.Fatalf("hash field persist: value=%t err=%v", persisted, err)
	}
	persistentField, persistentFieldErr := hash.ExpirationField(ctx, "3")
	if persistentFieldErr != nil || !persistentField.Exists || !persistentField.Persistent || persistentField.TTL != 0 {
		t.Fatalf("persistent hash field: value=%+v err=%v", persistentField, persistentFieldErr)
	}
	incrementIntNumber, incrementIntErr := hash.IncrementInt(ctx, "number", 2)
	if incrementIntErr != nil || incrementIntNumber != 2 {
		t.Fatalf("hash increment: value=%d err=%v", incrementIntNumber, incrementIntErr)
	}
	_, deleteErr := hash.Delete(ctx, "number")
	if deleteErr != nil {
		t.Fatal(deleteErr)
	}

	// Ranking、泛型 List/Set 和通用 ZSet 验证四类便利结构的真实命令结果。
	upsertErr := ranking.Upsert(ctx, "1", 100, alice)
	if upsertErr != nil {
		t.Fatal(upsertErr)
	}
	updated := integrationUser{ID: 1, Name: "Alice v2"}
	upsertErr2 := ranking.Upsert(ctx, "1", 120, updated)
	if upsertErr2 != nil {
		t.Fatal(upsertErr2)
	}
	upsertErr3 := ranking.Upsert(ctx, "2", 80, users["2"])
	if upsertErr3 != nil {
		t.Fatal(upsertErr3)
	}
	// Get 在单次 Lua 内同时读取 Hash 载荷与 ZSet 分数，不能组合两个时刻的数据。
	rankedAlice, rankedAliceErr := ranking.Get(ctx, "1")
	if rankedAliceErr != nil || rankedAlice != (rueidistpl.Ranked[integrationUser]{ID: "1", Score: 120, Value: updated}) {
		t.Fatalf("ranking get: value=%+v err=%v", rankedAlice, rankedAliceErr)
	}
	top, err := ranking.Top(ctx, 2)
	if err != nil || len(top) != 2 || top[0].ID != "1" || top[0].Score != 120 || top[0].Value != updated {
		t.Fatalf("ranking: value=%+v err=%v", top, err)
	}
	length, err := ranking.Len(ctx)
	if err != nil || length != 2 {
		t.Fatalf("ranking length: value=%d err=%v", length, err)
	}

	_, pushRightErr := typedList.PushRight(ctx, alice, users["2"])
	if pushRightErr != nil {
		t.Fatal(pushRightErr)
	}
	typedItems, err := typedList.Range(ctx, 0, -1)
	if err != nil || len(typedItems) != 2 || typedItems[1] != users["2"] {
		t.Fatalf("typed list: value=%+v err=%v", typedItems, err)
	}
	setErr2 := typedList.Set(ctx, 1, integrationUser{ID: 2, Name: "Bob v2"})
	if setErr2 != nil {
		t.Fatal(setErr2)
	}
	popped, err := typedList.PopLeft(ctx)
	if err != nil || popped != alice {
		t.Fatalf("typed list pop: value=%+v err=%v", popped, err)
	}

	setAdded, setAddErr := typedSet.Add(ctx, alice, users["2"], alice)
	if setAddErr != nil || setAdded != 2 {
		t.Fatalf("typed set add: value=%d err=%v", setAdded, setAddErr)
	}
	setContains, setContainsErr := typedSet.Contains(ctx, alice)
	if setContainsErr != nil || !setContains {
		t.Fatalf("typed set contains: value=%t err=%v", setContains, setContainsErr)
	}
	typedSetMembers, typedSetMembersErr := typedSet.Members(ctx)
	if typedSetMembersErr != nil || len(typedSetMembers) != 2 {
		t.Fatalf("typed set members: value=%+v err=%v", typedSetMembers, typedSetMembersErr)
	}
	typedSetScan, typedSetScanErr := typedSet.Scan(ctx, 0, "", 100)
	if typedSetScanErr != nil || typedSetScan.Cursor != 0 || len(typedSetScan.Values) != 2 {
		t.Fatalf("typed set scan: value=%+v err=%v", typedSetScan, typedSetScanErr)
	}
	setRemoved, setRemoveErr := typedSet.Remove(ctx, users["2"])
	if setRemoveErr != nil || setRemoved != 1 {
		t.Fatalf("typed set remove: value=%d err=%v", setRemoved, setRemoveErr)
	}

	addManyErr := sortedSet.AddMany(ctx,
		rueidistpl.ScoredMember{Member: "alice", Score: 10},
		rueidistpl.ScoredMember{Member: "bob", Score: 20},
	)
	if addManyErr != nil {
		t.Fatal(addManyErr)
	}
	incrementScore, incrementErr := sortedSet.Increment(ctx, "alice", 15)
	if incrementErr != nil || incrementScore != 25 {
		t.Fatalf("sorted set increment: value=%f err=%v", incrementScore, incrementErr)
	}
	scores, err := sortedSet.ReverseRange(ctx, 0, -1)
	if err != nil || len(scores) != 2 || scores[0].Member != "alice" || scores[0].Score != 25 {
		t.Fatalf("sorted set range: value=%+v err=%v", scores, err)
	}
	aliceReverseRank, reverseRankErr := sortedSet.ReverseRank(ctx, "alice")
	if reverseRankErr != nil || aliceReverseRank != 0 {
		t.Fatalf("sorted set rank: value=%d err=%v", aliceReverseRank, reverseRankErr)
	}
	poppedScores, popMinErr := sortedSet.PopMin(ctx, 1)
	if popMinErr != nil || len(poppedScores) != 1 || poppedScores[0].Member != "bob" {
		t.Fatalf("sorted set pop: value=%+v err=%v", poppedScores, popMinErr)
	}

	// Stream 使用真实消费组走完容量控制、投递、Pending、AutoClaim/Claim、Ack 和消费组清理链路。
	createGroupErr := stream.CreateGroup(ctx, "workers", "0")
	if createGroupErr != nil {
		t.Fatal(createGroupErr)
	}
	streamID1, err := stream.AddMaxLen(ctx, alice, 1000)
	if err != nil {
		t.Fatal(err)
	}
	streamID2, err := stream.AddMaxLen(ctx, users["2"], 1000)
	if err != nil {
		t.Fatal(err)
	}
	trimmed, trimErr := stream.TrimMaxLen(ctx, 1000)
	if trimErr != nil || trimmed < 0 {
		t.Fatalf("stream trim: value=%d err=%v", trimmed, trimErr)
	}
	streamItems, err := stream.ReadGroup(ctx, "workers", "worker-1", 10)
	if err != nil || len(streamItems) != 2 {
		t.Fatalf("stream read group: value=%+v err=%v", streamItems, err)
	}
	pending, err := stream.Pending(ctx, "workers", 10)
	if err != nil || len(pending) != 2 {
		t.Fatalf("stream pending: value=%+v err=%v", pending, err)
	}
	autoClaimed, autoClaimErr := stream.AutoClaim(ctx, "workers", "worker-auto", 0, "0-0", 10)
	if autoClaimErr != nil || len(autoClaimed.Entries) != 2 {
		t.Fatalf("stream auto claim: value=%+v err=%v", autoClaimed, autoClaimErr)
	}
	// XINFO 应直接返回强类型组/消费者运维状态，无需业务层拆解 RESP。
	streamGroups, streamGroupsErr := stream.Groups(ctx)
	if streamGroupsErr != nil || len(streamGroups) != 1 || streamGroups[0].Name != "workers" || streamGroups[0].Pending != 2 {
		t.Fatalf("stream groups: value=%+v err=%v", streamGroups, streamGroupsErr)
	}
	streamConsumers, streamConsumersErr := stream.Consumers(ctx, "workers")
	if streamConsumersErr != nil || len(streamConsumers) < 2 {
		t.Fatalf("stream consumers: value=%+v err=%v", streamConsumers, streamConsumersErr)
	}
	claimed, err := stream.Claim(ctx, "workers", "worker-2", 0, streamID1)
	if err != nil || len(claimed) != 1 || claimed[0].ID != streamID1 {
		t.Fatalf("stream claim: value=%+v err=%v", claimed, err)
	}
	acked, err := stream.Ack(ctx, "workers", streamID1, streamID2)
	if err != nil || acked != 2 {
		t.Fatalf("stream ack: value=%d err=%v", acked, err)
	}
	setGroupIDErr := stream.SetGroupID(ctx, "workers", "0-0")
	if setGroupIDErr != nil {
		t.Fatal(setGroupIDErr)
	}
	removedPending, deleteConsumerErr := stream.DeleteConsumer(ctx, "workers", "worker-auto")
	if deleteConsumerErr != nil || removedPending != 0 {
		t.Fatalf("stream delete consumer: pending=%d err=%v", removedPending, deleteConsumerErr)
	}
	groupDeleted, deleteGroupErr := stream.DeleteGroup(ctx, "workers")
	if deleteGroupErr != nil || !groupDeleted {
		t.Fatalf("stream delete group: deleted=%t err=%v", groupDeleted, deleteGroupErr)
	}

	// Pub/Sub 订阅启动存在并发时序，因此循环发布直到 Redis 报告已有订阅者。
	handlerErr := errors.New("subscription complete")
	received := make(chan rueidistpl.PubSubMessage[integrationUser], 1)
	subscribeErr := make(chan error, 1)
	go func() {
		subscribeErr <- pubsub.Subscribe(ctx, func(message rueidistpl.PubSubMessage[integrationUser]) error {
			received <- message
			return handlerErr
		}, "created")
	}()
	publishTicker := time.NewTicker(10 * time.Millisecond)
	defer publishTicker.Stop()
	for {
		receivers, publishErr := pubsub.Publish(ctx, "created", alice)
		if publishErr != nil {
			t.Fatal(publishErr)
		}
		if receivers > 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-publishTicker.C:
		}
	}
	select {
	case message := <-received:
		if message.Channel != "created" || message.Value != alice {
			t.Fatalf("pubsub message: %+v", message)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case err := <-subscribeErr:
		if !errors.Is(err, handlerErr) {
			t.Fatalf("pubsub error: got %v want %v", err, handlerErr)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// 幂等测试同时验证成功保留标记，以及业务失败甚至 context 取消后仍清理标记。
	ran, err := idempotency.Run(ctx, "event-1", time.Minute, func(context.Context) error { return nil })
	if err != nil || !ran {
		t.Fatalf("idempotency first run: ran=%t err=%v", ran, err)
	}
	ran, err = idempotency.Run(ctx, "event-1", time.Minute, func(context.Context) error {
		return errors.New("must not run")
	})
	if err != nil || ran {
		t.Fatalf("idempotency duplicate: ran=%t err=%v", ran, err)
	}
	workErr := errors.New("work failed")
	failedCtx, cancelFailed := context.WithCancel(ctx)
	ran, err = idempotency.Run(failedCtx, "failed-event", time.Minute, func(context.Context) error {
		cancelFailed()
		return workErr
	})
	if !ran || !errors.Is(err, workErr) {
		t.Fatalf("idempotency failed run: ran=%t err=%v", ran, err)
	}
	retry, err := idempotency.Try(ctx, "failed-event", time.Minute)
	if err != nil || !retry {
		t.Fatalf("idempotency retry: acquired=%t err=%v", retry, err)
	}

	// 模拟旧任务执行期间标记到期：先移除旧令牌，再让新请求取得同一 ID。
	// 旧任务随后失败时只能比较删除自己的令牌，必须保留新请求写入的标记。
	ownershipErr := errors.New("old owner failed")
	ran, err = idempotency.Run(ctx, "ownership-event", time.Minute, func(handlerCtx context.Context) error {
		if forgetErr := idempotency.Forget(handlerCtx, "ownership-event"); forgetErr != nil {
			return forgetErr
		}
		reacquired, reacquireErr := idempotency.Try(handlerCtx, "ownership-event", time.Minute)
		if reacquireErr != nil {
			return reacquireErr
		}
		if !reacquired {
			return errors.New("replacement owner did not acquire marker")
		}
		return ownershipErr
	})
	if !ran || !errors.Is(err, ownershipErr) {
		t.Fatalf("idempotency ownership run: ran=%t err=%v", ran, err)
	}
	replacementExists, replacementExistsErr := idempotency.Exists(ctx, "ownership-event")
	if replacementExistsErr != nil || !replacementExists {
		t.Fatalf("idempotency replacement marker: exists=%t err=%v", replacementExists, replacementExistsErr)
	}

	// 延迟队列只应取出 now 之前的任务，未来任务保留到显式 Remove。
	now := time.Now()
	scheduleErr := delayQueue.Schedule(ctx, "due", now.Add(-time.Second), alice)
	if scheduleErr != nil {
		t.Fatal(scheduleErr)
	}
	scheduleErr2 := delayQueue.Schedule(ctx, "future", now.Add(time.Hour), users["2"])
	if scheduleErr2 != nil {
		t.Fatal(scheduleErr2)
	}
	due, err := delayQueue.Poll(ctx, now, 10)
	if err != nil || len(due) != 1 || due[0].ID != "due" || due[0].Value != alice {
		t.Fatalf("delay queue poll: value=%+v err=%v", due, err)
	}
	queueLength, lenErr := delayQueue.Len(ctx)
	if lenErr != nil || queueLength != 1 {
		t.Fatalf("delay queue length: value=%d err=%v", queueLength, lenErr)
	}
	removed, removeErr := delayQueue.Remove(ctx, "future")
	if removeErr != nil || !removed {
		t.Fatalf("delay queue remove: value=%t err=%v", removed, removeErr)
	}

	// 最后验证基础 String、Counter、StringList、StringSet 和分布式锁门面。
	setStringErr := redisClient.SetString(ctx, "string", "value", time.Minute)
	if setStringErr != nil {
		t.Fatal(setStringErr)
	}
	value, err := redisClient.GetString(ctx, "string")
	if err != nil || value != "value" {
		t.Fatalf("string: value=%q err=%v", value, err)
	}
	stringExpiration, stringExpirationErr := redisClient.Expiration(ctx, "string")
	if stringExpirationErr != nil || !stringExpiration.Exists || stringExpiration.Persistent || stringExpiration.TTL <= 0 {
		t.Fatalf("string expiration: value=%+v err=%v", stringExpiration, stringExpirationErr)
	}

	// 每个 key 独立发送 EXISTS/DEL，带不同 hash tag 的输入在 Cluster 中也不会触发 CROSSSLOT。
	setBatchAErr := redisClient.SetString(ctx, "{batch-a}:value", "a")
	if setBatchAErr != nil {
		t.Fatal(setBatchAErr)
	}
	setBatchBErr := redisClient.SetString(ctx, "{batch-b}:value", "b")
	if setBatchBErr != nil {
		t.Fatal(setBatchBErr)
	}
	existsBatch := redisClient.ExistsEach(ctx, "{batch-a}:value", "{batch-missing}:value", "{batch-b}:value")
	if existsBatch.Err != nil || existsBatch.Existing != 2 || len(existsBatch.Results) != 3 ||
		!existsBatch.Results[0].Exists || existsBatch.Results[1].Exists || !existsBatch.Results[2].Exists {
		t.Fatalf("exists batch: value=%+v", existsBatch)
	}
	deleteBatch := redisClient.DeleteEach(ctx, "{batch-a}:value", "{batch-missing}:value", "{batch-b}:value")
	if deleteBatch.Err != nil || deleteBatch.Deleted != 2 || len(deleteBatch.Results) != 3 ||
		!deleteBatch.Results[0].Deleted || deleteBatch.Results[1].Deleted || !deleteBatch.Results[2].Deleted {
		t.Fatalf("delete batch: value=%+v", deleteBatch)
	}

	counter := redisClient.Counter("count")
	setErr3 := counter.Set(ctx, 10)
	if setErr3 != nil {
		t.Fatal(setErr3)
	}
	count, err := counter.Add(ctx, 2)
	if err != nil || count != 12 {
		t.Fatalf("counter: value=%d err=%v", count, err)
	}

	list := redisClient.List("list")
	_, pushRightErr2 := list.PushRight(ctx, "a", "b")
	if pushRightErr2 != nil {
		t.Fatal(pushRightErr2)
	}
	items, err := list.Range(ctx, 0, -1)
	if err != nil || len(items) != 2 || items[0] != "a" {
		t.Fatalf("list: value=%v err=%v", items, err)
	}

	set := redisClient.StringSet("set")
	_, addErr := set.Add(ctx, "go", "rueidis")
	if addErr != nil {
		t.Fatal(addErr)
	}
	member, err := set.Contains(ctx, "rueidis")
	if err != nil || !member {
		t.Fatalf("set member: value=%t err=%v", member, err)
	}
	setScan, setScanErr := set.Scan(ctx, 0, "r*", 100)
	if setScanErr != nil || setScan.Cursor != 0 || len(setScan.Members) != 1 || setScan.Members[0] != "rueidis" {
		t.Fatalf("set scan: value=%+v err=%v", setScan, setScanErr)
	}

	lockCtx, unlock, err := redisClient.TryLock(ctx, "job")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	_, _, tryLockErr := redisClient.TryLock(lockCtx, "job")
	if !errors.Is(tryLockErr, rueidislock.ErrNotLocked) {
		t.Fatalf("second lock: got %v want %v", tryLockErr, rueidislock.ErrNotLocked)
	}
}
