package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/lily0749labs/rueidistpl"
	"github.com/redis/rueidis"
)

// TestCoreTypesWithRealRedis 连接真实 Redis，按类型验证示例中最常用的读写路径。
// 测试只使用 Redis 6.2 基线内的命令，高版本结构由 redis8 子目录单独验证。
func TestCoreTypesWithRealRedis(t *testing.T) {
	// 普通 go test 默认跳过真实连接，防止开发机没有 Redis 时测试失败。
	// 执行 make test-examples 会自动设置该开关。
	if os.Getenv("RUEIDISTPL_EXAMPLES_INTEGRATION") != "1" {
		t.Skip("set RUEIDISTPL_EXAMPLES_INTEGRATION=1 to run real Redis example tests")
	}

	// 每次测试使用唯一前缀，避免本地重复执行、CI 并发和异常退出互相污染数据。
	prefix := fmt.Sprintf("%s:test:%d", envOrDefault("REDIS_KEY_PREFIX", defaultKeyPrefix), time.Now().UnixNano())
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{envOrDefault("REDIS_ADDR", defaultRedisAddr)},
			Password:    envOrDefault("REDIS_PASSWORD", defaultRedisPassword),
		},
		KeyPrefix: prefix,
		CacheTTL:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pingErr := redisClient.Ping(ctx)
	if pingErr != nil {
		t.Fatalf("真实 Redis 连接失败：%v", pingErr)
	}

	alice := User{ID: 1, Name: "Alice"}
	bob := User{ID: 2, Name: "Bob"}

	t.Run("String、Counter、StringList 和跨 Slot 批量结果", func(t *testing.T) {
		// String 验证普通文本和结构化 TTL，调用方不需要解释 -1ms/-2ms。
		setStringErr := redisClient.SetString(ctx, "message", "hello", time.Minute)
		message, getStringErr := redisClient.GetString(ctx, "message")
		expiration, expirationErr := redisClient.Expiration(ctx, "message")
		if setStringErr != nil || getStringErr != nil || message != "hello" ||
			expirationErr != nil || !expiration.Exists || expiration.Persistent || expiration.TTL <= 0 {
			t.Fatalf("String Set=%v Get=%q/%v Expiration=%+v/%v", setStringErr, message, getStringErr, expiration, expirationErr)
		}

		// Counter 验证整数设置和原子累加，返回值应是累加后的最新值。
		counter := redisClient.Counter("visits")
		counterSetErr := counter.Set(ctx, 10)
		counterValue, counterAddErr := counter.Add(ctx, 5)
		if counterSetErr != nil || counterAddErr != nil || counterValue != 15 {
			t.Fatalf("Counter Set=%v Add=%d/%v", counterSetErr, counterValue, counterAddErr)
		}

		// StringList 保留 Redis List 的字符串语义；泛型对象 List 在后续子测试验证。
		stringList := redisClient.StringList("recent-names")
		defer func() { _ = stringList.Clear(ctx) }()
		stringListLength, stringListPushErr := stringList.PushRight(ctx, "alice", "bob")
		stringListValues, stringListRangeErr := stringList.Range(ctx, 0, -1)
		if stringListPushErr != nil || stringListLength != 2 || stringListRangeErr != nil ||
			!slices.Equal(stringListValues, []string{"alice", "bob"}) {
			t.Fatalf("StringList Push=%d/%v Range=%v/%v", stringListLength, stringListPushErr, stringListValues, stringListRangeErr)
		}

		// 两个 key 使用不同 hash tag；DeleteEach/ExistsEach 应逐 key 返回稳定顺序和明细。
		firstKey := "{example-a}:value"
		secondKey := "{example-b}:value"
		firstSetErr := redisClient.SetString(ctx, firstKey, "one")
		secondSetErr := redisClient.SetString(ctx, secondKey, "two")
		existsResult := redisClient.ExistsEach(ctx, firstKey, secondKey, "{example-c}:missing")
		if firstSetErr != nil || secondSetErr != nil || existsResult.Err != nil ||
			existsResult.Existing != 2 || len(existsResult.Results) != 3 || existsResult.Results[2].Exists {
			t.Fatalf("跨 Slot ExistsEach Set=%v/%v Result=%+v", firstSetErr, secondSetErr, existsResult)
		}
		deleteResult := redisClient.DeleteEach(ctx, firstKey, secondKey, "{example-c}:missing")
		if deleteResult.Err != nil || deleteResult.Deleted != 2 || len(deleteResult.Results) != 3 {
			t.Fatalf("跨 Slot DeleteEach Result=%+v", deleteResult)
		}

		_ = counter.Delete(ctx)
		_, _ = redisClient.Delete(ctx, "message")
	})

	t.Run("Cache 和 Value 强类型 JSON 对象", func(t *testing.T) {
		// Cache 经由 gocache 保存 JSON，并同时返回强类型对象和剩余 TTL。
		cache := rueidistpl.NewCache[User](redisClient, "cache:users")
		defer func() { _ = cache.Delete(ctx, "1") }()
		cacheSetErr := cache.Set(ctx, "1", alice, time.Minute)
		cachedUser, cacheTTL, cacheGetErr := cache.GetWithTTL(ctx, "1")
		if cacheSetErr != nil || cacheGetErr != nil || cachedUser != alice || cacheTTL <= 0 {
			t.Fatalf("Cache Set=%v Get=%+v TTL=%s Err=%v", cacheSetErr, cachedUser, cacheTTL, cacheGetErr)
		}

		// Value 使用 Redis String 保存一个 JSON 对象，GETDEL 在 Redis 6.2+ 原子读取并删除。
		value := rueidistpl.NewValue[User](redisClient, "profile")
		defer func() { _, _ = value.Delete(ctx) }()
		valueSetErr := value.Set(ctx, bob, time.Minute)
		consumedValue, getDeleteErr := value.GetDelete(ctx)
		valueExists, valueExistsErr := value.Exists(ctx)
		if valueSetErr != nil || getDeleteErr != nil || consumedValue != bob || valueExistsErr != nil || valueExists {
			t.Fatalf("Value Set=%v GetDelete=%+v/%v Exists=%t/%v", valueSetErr, consumedValue, getDeleteErr, valueExists, valueExistsErr)
		}
	})

	t.Run("Hash、泛型 List、泛型 Set 和字符串 Set", func(t *testing.T) {
		// Hash 以 field 保存多个 JSON 对象，GetMany 只返回实际存在的 field。
		hash := rueidistpl.NewHash[User](redisClient, "users")
		defer func() { _ = hash.Clear(ctx) }()
		hashSetErr := hash.SetMany(ctx, map[string]User{"1": alice, "2": bob})
		hashValues, hashGetErr := hash.GetMany(ctx, "2", "missing", "1")
		if hashSetErr != nil || hashGetErr != nil || len(hashValues) != 2 || hashValues["1"] != alice || hashValues["2"] != bob {
			t.Fatalf("Hash Set=%v GetMany=%+v/%v", hashSetErr, hashValues, hashGetErr)
		}

		// 泛型 List 保证 JSON 对象按插入顺序解码，便于调试队列和最近访问记录。
		list := rueidistpl.NewList[User](redisClient, "recent-users")
		defer func() { _ = list.Clear(ctx) }()
		listLength, listPushErr := list.PushRight(ctx, alice, bob)
		listValues, listRangeErr := list.Range(ctx, 0, -1)
		if listPushErr != nil || listLength != 2 || listRangeErr != nil || !slices.Equal(listValues, []User{alice, bob}) {
			t.Fatalf("List Push=%d/%v Range=%+v/%v", listLength, listPushErr, listValues, listRangeErr)
		}

		// 泛型 Set 用 JSON 字符串去重；成员顺序不固定，因此按 Contains 和长度验证。
		set := rueidistpl.NewSet[User](redisClient, "online-users")
		defer func() { _ = set.Clear(ctx) }()
		setAdded, setAddErr := set.Add(ctx, alice, bob, alice)
		setContainsAlice, setContainsErr := set.Contains(ctx, alice)
		setLength, setLengthErr := set.Len(ctx)
		if setAddErr != nil || setAdded != 2 || setContainsErr != nil || !setContainsAlice || setLengthErr != nil || setLength != 2 {
			t.Fatalf("Set Add=%d/%v Contains=%t/%v Len=%d/%v", setAdded, setAddErr, setContainsAlice, setContainsErr, setLength, setLengthErr)
		}

		// StringSet 适合标签等无需 JSON 的字符串集合。
		stringSet := redisClient.StringSet("tags")
		defer func() { _ = stringSet.Clear(ctx) }()
		stringSetAdded, stringSetAddErr := stringSet.Add(ctx, "go", "redis", "go")
		stringSetContains, stringSetContainsErr := stringSet.Contains(ctx, "redis")
		if stringSetAddErr != nil || stringSetAdded != 2 || stringSetContainsErr != nil || !stringSetContains {
			t.Fatalf("StringSet Add=%d/%v Contains=%t/%v", stringSetAdded, stringSetAddErr, stringSetContains, stringSetContainsErr)
		}
	})

	t.Run("Sorted Set 和对象排行榜", func(t *testing.T) {
		// SortedSet 保存字符串 member；ReverseRange 按分数从高到低返回。
		scores := redisClient.SortedSet("scores")
		defer func() { _ = scores.Clear(ctx) }()
		scoresAddErr := scores.AddMany(ctx,
			rueidistpl.ScoredMember{Member: "alice", Score: 100},
			rueidistpl.ScoredMember{Member: "bob", Score: 80},
		)
		topScores, topScoresErr := scores.ReverseRange(ctx, 0, -1)
		if scoresAddErr != nil || topScoresErr != nil || len(topScores) != 2 || topScores[0].Member != "alice" || topScores[0].Score != 100 {
			t.Fatalf("SortedSet Add=%v ReverseRange=%+v/%v", scoresAddErr, topScores, topScoresErr)
		}

		// Ranking 将稳定 ID、分数和强类型对象组合起来，内部 Hash 与 ZSet 由 Lua 原子更新。
		ranking := rueidistpl.NewRanking[User](redisClient, "ranking")
		defer func() { _ = ranking.Clear(ctx) }()
		aliceUpsertErr := ranking.Upsert(ctx, "1", 100, alice)
		bobUpsertErr := ranking.Upsert(ctx, "2", 80, bob)
		topUsers, topUsersErr := ranking.Top(ctx, 2)
		if aliceUpsertErr != nil || bobUpsertErr != nil || topUsersErr != nil || len(topUsers) != 2 ||
			topUsers[0].ID != "1" || topUsers[0].Value != alice || topUsers[0].Score != 100 {
			t.Fatalf("Ranking Upsert=%v/%v Top=%+v/%v", aliceUpsertErr, bobUpsertErr, topUsers, topUsersErr)
		}
	})

	t.Run("Stream 消费组", func(t *testing.T) {
		// Stream 验证创建消费组、读取、确认和 Pending 清理的完整消息路径。
		stream := rueidistpl.NewStream[User](redisClient, "events")
		defer func() { _ = stream.Clear(ctx) }()
		groupErr := stream.CreateGroup(ctx, "workers", "0")
		entryID, addErr := stream.Add(ctx, alice)
		entries, readErr := stream.ReadGroup(ctx, "workers", "worker-1", 10)
		if groupErr != nil || addErr != nil || entryID == "" || readErr != nil || len(entries) != 1 || entries[0].Value != alice {
			t.Fatalf("Stream Group=%v Add=%q/%v Read=%+v/%v", groupErr, entryID, addErr, entries, readErr)
		}
		acked, ackErr := stream.Ack(ctx, "workers", entryID)
		if ackErr != nil || acked != 1 {
			t.Fatalf("Stream Ack=%d/%v", acked, ackErr)
		}
	})

	t.Run("Bitmap、Geo 和 HyperLogLog", func(t *testing.T) {
		// Bitmap 适合用户活跃状态等稠密布尔标记，Count 返回置位数量。
		bitmap := redisClient.Bitmap("active")
		defer func() { _ = bitmap.Clear(ctx) }()
		previousBit, bitmapSetErr := bitmap.Set(ctx, 7, true)
		currentBit, bitmapGetErr := bitmap.Get(ctx, 7)
		bitCount, bitmapCountErr := bitmap.Count(ctx)
		if bitmapSetErr != nil || previousBit || bitmapGetErr != nil || !currentBit || bitmapCountErr != nil || bitCount != 1 {
			t.Fatalf("Bitmap Set=%t/%v Get=%t/%v Count=%d/%v", previousBit, bitmapSetErr, currentBit, bitmapGetErr, bitCount, bitmapCountErr)
		}

		// Geo 验证坐标写入和附近搜索，结果同时包含成员、距离和经纬度。
		geo := redisClient.Geo("cities")
		defer func() { _ = geo.Clear(ctx) }()
		geoAdded, geoAddErr := geo.Add(ctx,
			rueidistpl.GeoPoint{Member: "shanghai", Longitude: 121.4737, Latitude: 31.2304},
			rueidistpl.GeoPoint{Member: "hangzhou", Longitude: 120.1551, Latitude: 30.2741},
		)
		nearbyCities, geoSearchErr := geo.SearchRadiusByMember(ctx, "shanghai", 200, rueidistpl.GeoKilometers, rueidistpl.GeoSearchOptions{})
		if geoAddErr != nil || geoAdded != 2 || geoSearchErr != nil || len(nearbyCities) != 2 || nearbyCities[0].Member != "shanghai" {
			t.Fatalf("Geo Add=%d/%v Search=%+v/%v", geoAdded, geoAddErr, nearbyCities, geoSearchErr)
		}

		// HyperLogLog 用近似算法统计去重用户，小样本下应得到三个唯一元素。
		hll := redisClient.HyperLogLog("visitors")
		defer func() { _ = hll.Clear(ctx) }()
		hllChanged, hllAddErr := hll.Add(ctx, "u1", "u2", "u1", "u3")
		hllCount, hllCountErr := hll.Count(ctx)
		if hllAddErr != nil || !hllChanged || hllCountErr != nil || hllCount != 3 {
			t.Fatalf("HyperLogLog Add=%t/%v Count=%d/%v", hllChanged, hllAddErr, hllCount, hllCountErr)
		}
	})

	t.Run("Pub/Sub 真实订阅与发布", func(t *testing.T) {
		// Subscribe 是阻塞调用，因此在 goroutine 中启动；循环发布直到 Redis 报告已有订阅者。
		pubsub := rueidistpl.NewPubSub[User](redisClient, "notifications")
		handlerDone := errors.New("示例消息接收完成")
		received := make(chan rueidistpl.PubSubMessage[User], 1)
		subscribeErr := make(chan error, 1)
		go func() {
			subscribeErr <- pubsub.Subscribe(ctx, func(message rueidistpl.PubSubMessage[User]) error {
				received <- message
				return handlerDone
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

		var message rueidistpl.PubSubMessage[User]
		select {
		case message = <-received:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if message.Channel != "created" || message.Value != alice {
			t.Fatalf("Pub/Sub message=%+v", message)
		}
		var subscriptionResult error
		select {
		case subscriptionResult = <-subscribeErr:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if !errors.Is(subscriptionResult, handlerDone) {
			t.Fatalf("Pub/Sub subscribe err=%v want=%v", subscriptionResult, handlerDone)
		}
	})

	t.Run("幂等、延迟队列和分布式锁", func(t *testing.T) {
		// Idempotency.Run 只执行首次出现的业务 ID，重复请求不会再次调用业务函数。
		idempotency := redisClient.Idempotency("idempotency")
		defer func() { _ = idempotency.Forget(ctx, "order-1") }()
		firstRan, firstRunErr := idempotency.Run(ctx, "order-1", time.Minute, func(context.Context) error { return nil })
		duplicateCalled := false
		duplicateRan, duplicateRunErr := idempotency.Run(ctx, "order-1", time.Minute, func(context.Context) error {
			duplicateCalled = true
			return nil
		})
		if firstRunErr != nil || !firstRan || duplicateRunErr != nil || duplicateRan || duplicateCalled {
			t.Fatalf("Idempotency first=%t/%v duplicate=%t/%v called=%t", firstRan, firstRunErr, duplicateRan, duplicateRunErr, duplicateCalled)
		}
		// handler 超过初始 TTL 后标记仍应由后台续租持有，不能被并发请求重新取得。
		defer func() { _ = idempotency.Forget(ctx, "slow-order") }()
		slowRan, slowErr := idempotency.Run(ctx, "slow-order", 60*time.Millisecond, func(handlerCtx context.Context) error {
			select {
			case <-time.After(90 * time.Millisecond):
			case <-handlerCtx.Done():
				return context.Cause(handlerCtx)
			}
			reacquired, reacquireErr := idempotency.Try(handlerCtx, "slow-order", time.Minute)
			if reacquireErr != nil {
				return reacquireErr
			}
			if reacquired {
				return errors.New("active idempotency lease was acquired twice")
			}
			return nil
		})
		if slowErr != nil || !slowRan {
			t.Fatalf("Idempotency slow=%t/%v", slowRan, slowErr)
		}

		// 可靠 DelayQueue 领取到期任务后显式 ACK，未来任务继续留在调度集合中。
		queue := rueidistpl.NewDelayQueue[User](redisClient, "jobs")
		defer func() { _ = queue.Clear(ctx) }()
		now := time.Now()
		dueScheduleErr := queue.Schedule(ctx, "due", now.Add(-time.Second), alice)
		futureScheduleErr := queue.Schedule(ctx, "future", now.Add(time.Hour), bob)
		dueJobs, claimErr := queue.Claim(ctx, time.Minute, 10)
		acked := false
		var ackErr error
		if len(dueJobs) == 1 {
			acked, ackErr = queue.Ack(ctx, dueJobs[0].ID, dueJobs[0].Token)
		}
		queueLength, queueLengthErr := queue.Len(ctx)
		if dueScheduleErr != nil || futureScheduleErr != nil || claimErr != nil || len(dueJobs) != 1 ||
			dueJobs[0].ID != "due" || dueJobs[0].Value != alice || ackErr != nil || !acked ||
			queueLengthErr != nil || queueLength != 1 {
			t.Fatalf("DelayQueue Schedule=%v/%v Claim=%+v/%v Ack=%t/%v Len=%d/%v", dueScheduleErr, futureScheduleErr, dueJobs, claimErr, acked, ackErr, queueLength, queueLengthErr)
		}

		// 模拟 worker 领取后崩溃：visibility timeout 到期后任务应重新投递，旧 token 不能 ACK 新租约。
		if err := queue.Schedule(ctx, "recover", time.Now().Add(-time.Second), bob); err != nil {
			t.Fatal(err)
		}
		firstClaim, err := queue.Claim(ctx, 20*time.Millisecond, 1)
		if err != nil || len(firstClaim) != 1 {
			t.Fatalf("DelayQueue first recovery claim=%+v/%v", firstClaim, err)
		}
		time.Sleep(50 * time.Millisecond)
		secondClaim, err := queue.Claim(ctx, time.Minute, 1)
		if err != nil || len(secondClaim) != 1 || secondClaim[0].ID != "recover" || secondClaim[0].Token == firstClaim[0].Token {
			t.Fatalf("DelayQueue second recovery claim=%+v/%v", secondClaim, err)
		}
		staleAck, staleAckErr := queue.Ack(ctx, firstClaim[0].ID, firstClaim[0].Token)
		freshAck, freshAckErr := queue.Ack(ctx, secondClaim[0].ID, secondClaim[0].Token)
		if staleAckErr != nil || staleAck || freshAckErr != nil || !freshAck {
			t.Fatalf("DelayQueue staleAck=%t/%v freshAck=%t/%v", staleAck, staleAckErr, freshAck, freshAckErr)
		}

		// TryLock 只尝试一次，成功后返回的 cancel 函数负责释放并停止自动续期。
		lockCtx, unlock, lockErr := redisClient.TryLock(ctx, "refresh-ranking")
		if lockErr != nil {
			t.Fatal(lockErr)
		}
		select {
		case <-lockCtx.Done():
			unlock()
			t.Fatalf("分布式锁刚获取就失效：%v", lockCtx.Err())
		default:
		}
		unlock()

		// fencing token 必须随每次成功持锁递增，供下游资源拒绝失去租约的旧 worker。
		var firstToken int64
		if err := redisClient.WithFencedLock(ctx, "settle", func(_ context.Context, token int64) error {
			firstToken = token
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := redisClient.WithFencedLock(ctx, "settle", func(_ context.Context, token int64) error {
			if token <= firstToken {
				return fmt.Errorf("fencing token did not increase: %d <= %d", token, firstToken)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}
