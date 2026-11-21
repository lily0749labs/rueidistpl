package rueidistpl_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lily0749labs/rueidistpl"
	"github.com/redis/rueidis"
)

// TestRedisClusterIntegration 使用真实三主节点 Redis Cluster 验证路由、跨 Slot 批量操作，
// 以及内部使用多个 key 的组合结构。单节点 Redis 无法发现这类 CROSSSLOT 问题。
func TestRedisClusterIntegration(t *testing.T) {
	// 普通单元测试不依赖 Docker；只有 CI 或开发者显式开启时才连接真实 Cluster。
	if os.Getenv("RUEIDISTPL_CLUSTER_INTEGRATION") != "1" {
		t.Skip("set RUEIDISTPL_CLUSTER_INTEGRATION=1 to run Redis Cluster integration test")
	}

	// 至少提供三个种子地址，确保该用例没有误连到单节点 Redis 后产生假阳性。
	addresses := splitRedisAddresses(envOr("REDIS_CLUSTER_ADDRS", "127.0.0.1:17300,127.0.0.1:17301,127.0.0.1:17302"))
	if len(addresses) < 3 {
		t.Fatalf("REDIS_CLUSTER_ADDRS must contain at least three addresses, got %v", addresses)
	}
	prefix := fmt.Sprintf("rueidistpl:cluster-test:%d", time.Now().UnixNano())
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: addresses,
			Password:    envOr("REDIS_PASSWORD", ""),
		},
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if pingErr := redisClient.Ping(ctx); pingErr != nil {
		t.Fatal(pingErr)
	}

	// 两个显式 hash tag 必须落到不同 Slot，随后用逐 key API 验证 Rueidis 能分别路由到正确主节点。
	firstKey := "{cluster-account-a}:profile"
	secondKey := "{cluster-account-b}:profile"
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_ = redisClient.DeleteEach(cleanupCtx, firstKey, secondKey)
	}()
	firstSlot := redisClusterSlot(t, ctx, redisClient, redisClient.Key(firstKey))
	secondSlot := redisClusterSlot(t, ctx, redisClient, redisClient.Key(secondKey))
	if firstSlot == secondSlot {
		t.Fatalf("test keys unexpectedly share slot %d", firstSlot)
	}
	first := rueidistpl.NewValue[integrationUser](redisClient, firstKey)
	second := rueidistpl.NewValue[integrationUser](redisClient, secondKey)
	alice := integrationUser{ID: 1, Name: "Alice"}
	bob := integrationUser{ID: 2, Name: "Bob"}
	if firstSetErr := first.Set(ctx, alice); firstSetErr != nil {
		t.Fatal(firstSetErr)
	}
	if secondSetErr := second.Set(ctx, bob); secondSetErr != nil {
		t.Fatal(secondSetErr)
	}
	existsResult := redisClient.ExistsEach(ctx, firstKey, secondKey, "{cluster-account-c}:missing")
	if existsResult.Err != nil || existsResult.Existing != 2 || len(existsResult.Results) != 3 ||
		!existsResult.Results[0].Exists || !existsResult.Results[1].Exists || existsResult.Results[2].Exists {
		t.Fatalf("ExistsEach across slots result=%+v", existsResult)
	}
	deleteResult := redisClient.DeleteEach(ctx, firstKey, secondKey)
	if deleteResult.Err != nil || deleteResult.Deleted != 2 || len(deleteResult.Results) != 2 ||
		!deleteResult.Results[0].Deleted || !deleteResult.Results[1].Deleted {
		t.Fatalf("DeleteEach across slots result=%+v", deleteResult)
	}

	// Ranking 和 DelayQueue 都用 Hash + ZSet + Lua；构造器必须自动给内部 key 加相同 hash tag。
	ranking := rueidistpl.NewRanking[integrationUser](redisClient, "ranking")
	queue := rueidistpl.NewDelayQueue[integrationUser](redisClient, "delayed")
	defer clearClusterStructures(redisClient, ranking, queue)
	rankingValuesKey, rankingScoresKey := ranking.Keys()
	if redisClusterSlot(t, ctx, redisClient, rankingValuesKey) != redisClusterSlot(t, ctx, redisClient, rankingScoresKey) {
		t.Fatalf("ranking keys do not share a cluster slot: %q %q", rankingValuesKey, rankingScoresKey)
	}
	if upsertErr := ranking.Upsert(ctx, "alice", 100, alice); upsertErr != nil {
		t.Fatal(upsertErr)
	}
	top, topErr := ranking.Top(ctx, 1)
	if topErr != nil || len(top) != 1 || top[0].ID != "alice" || top[0].Value != alice {
		t.Fatalf("Ranking Top values=%+v err=%v", top, topErr)
	}
	queueValuesKey, queueScoresKey := queue.Keys()
	queueProcessingKey, queueClaimsKey := queue.ReliableKeys()
	queueSlot := redisClusterSlot(t, ctx, redisClient, queueValuesKey)
	for _, key := range []string{queueScoresKey, queueProcessingKey, queueClaimsKey} {
		if queueSlot != redisClusterSlot(t, ctx, redisClient, key) {
			t.Fatalf("delay queue keys do not share a cluster slot: %q %q", queueValuesKey, key)
		}
	}
	dueAt := time.Now().Add(-time.Second).Truncate(time.Millisecond)
	if scheduleErr := queue.Schedule(ctx, "welcome", dueAt, bob); scheduleErr != nil {
		t.Fatal(scheduleErr)
	}
	delayed, claimErr := queue.Claim(ctx, time.Minute, 10)
	if claimErr != nil || len(delayed) != 1 || delayed[0].ID != "welcome" || delayed[0].Value != bob {
		t.Fatalf("DelayQueue Claim values=%+v err=%v", delayed, claimErr)
	}
	acked, ackErr := queue.Ack(ctx, delayed[0].ID, delayed[0].Token)
	if ackErr != nil || !acked {
		t.Fatalf("DelayQueue Ack value=%t err=%v", acked, ackErr)
	}

	// Redis 原生多 key 命令仍要求调用方使用相同 tag；便利方法必须完整保留这些逻辑 key。
	leftBitmap := redisClient.Bitmap("{cluster-bitmap}:left")
	rightBitmap := redisClient.Bitmap("{cluster-bitmap}:right")
	combinedBitmap := redisClient.Bitmap("{cluster-bitmap}:combined")
	if _, bitmapErr := leftBitmap.Set(ctx, 1, true); bitmapErr != nil {
		t.Fatal(bitmapErr)
	}
	if _, bitmapErr := rightBitmap.Set(ctx, 2, true); bitmapErr != nil {
		t.Fatal(bitmapErr)
	}
	if _, bitmapErr := leftBitmap.Combine(ctx, rueidistpl.BitmapOr, "{cluster-bitmap}:combined", "{cluster-bitmap}:right"); bitmapErr != nil {
		t.Fatal(bitmapErr)
	}
	combinedCount, combinedCountErr := combinedBitmap.Count(ctx)
	if combinedCountErr != nil || combinedCount != 2 {
		t.Fatalf("Bitmap Combine count=%d err=%v", combinedCount, combinedCountErr)
	}

	webHLL := redisClient.HyperLogLog("{cluster-hll}:web")
	mobileHLL := redisClient.HyperLogLog("{cluster-hll}:mobile")
	mergedHLL := redisClient.HyperLogLog("{cluster-hll}:merged")
	if _, hllErr := webHLL.Add(ctx, "u1", "u2"); hllErr != nil {
		t.Fatal(hllErr)
	}
	if _, hllErr := mobileHLL.Add(ctx, "u2", "u3"); hllErr != nil {
		t.Fatal(hllErr)
	}
	unionCount, unionCountErr := webHLL.CountWith(ctx, "{cluster-hll}:mobile")
	if unionCountErr != nil || unionCount != 3 {
		t.Fatalf("HyperLogLog CountWith count=%d err=%v", unionCount, unionCountErr)
	}
	if mergeErr := mergedHLL.MergeFrom(ctx, "{cluster-hll}:web", "{cluster-hll}:mobile"); mergeErr != nil {
		t.Fatal(mergeErr)
	}
	mergedCount, mergedCountErr := mergedHLL.Count(ctx)
	if mergedCountErr != nil || mergedCount != 3 {
		t.Fatalf("HyperLogLog MergeFrom count=%d err=%v", mergedCount, mergedCountErr)
	}

	// 主流程成功后也主动清理位图和 HLL，方便开发者重复执行该用例。
	cleanupResult := redisClient.DeleteEach(ctx,
		"{cluster-bitmap}:left", "{cluster-bitmap}:right", "{cluster-bitmap}:combined",
		"{cluster-hll}:web", "{cluster-hll}:mobile", "{cluster-hll}:merged",
	)
	if cleanupResult.Err != nil {
		t.Fatalf("cleanup cluster structures: %v", cleanupResult.Err)
	}
}

func splitRedisAddresses(value string) []string {
	parts := strings.Split(value, ",")
	addresses := make([]string, 0, len(parts))
	for _, part := range parts {
		if address := strings.TrimSpace(part); address != "" {
			addresses = append(addresses, address)
		}
	}
	return addresses
}

func redisClusterSlot(t *testing.T, ctx context.Context, redisClient *rueidistpl.Redis, key string) int64 {
	t.Helper()
	command := redisClient.Client().B().Arbitrary("CLUSTER", "KEYSLOT").Keys(key).ReadOnly()
	slot, err := redisClient.Client().Do(ctx, command).AsInt64()
	if err != nil {
		t.Fatalf("CLUSTER KEYSLOT %q: %v", key, err)
	}
	return slot
}

func clearClusterStructures(
	redisClient *rueidistpl.Redis,
	ranking *rueidistpl.Ranking[integrationUser],
	queue *rueidistpl.DelayQueue[integrationUser],
) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ranking.Clear(cleanupCtx)
	_ = queue.Clear(cleanupCtx)
	_ = redisClient.Bitmap("{cluster-bitmap}:left").Clear(cleanupCtx)
	_ = redisClient.Bitmap("{cluster-bitmap}:right").Clear(cleanupCtx)
	_ = redisClient.Bitmap("{cluster-bitmap}:combined").Clear(cleanupCtx)
	_ = redisClient.HyperLogLog("{cluster-hll}:web").Clear(cleanupCtx)
	_ = redisClient.HyperLogLog("{cluster-hll}:mobile").Clear(cleanupCtx)
	_ = redisClient.HyperLogLog("{cluster-hll}:merged").Clear(cleanupCtx)
}
