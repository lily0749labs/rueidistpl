package rueidistpl_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/lily0749labs/rueidistpl"
	"github.com/redis/rueidis"
)

// TestRedisCompatibilityIntegration 使用真实 Redis 6.2 验证项目声明的核心兼容基线。
// 高版本 Hash field TTL、Sharded Pub/Sub、模块结构和 Array 由其他集成测试分别负责。
func TestRedisCompatibilityIntegration(t *testing.T) {
	// 只有 CI 或开发者显式开启时才连接 Redis，普通 go test 保持纯单元测试属性。
	if os.Getenv("RUEIDISTPL_COMPAT_INTEGRATION") != "1" {
		t.Skip("set RUEIDISTPL_COMPAT_INTEGRATION=1 to run Redis 6.2 compatibility test")
	}

	// 唯一 key 前缀可以隔离并行任务和上次异常退出留下的数据。
	prefix := fmt.Sprintf("rueidistpl:compat-test:%d", time.Now().UnixNano())
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{envOr("REDIS_ADDR", "127.0.0.1:6379")},
			Password:    envOr("REDIS_PASSWORD", ""),
		},
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pingErr := redisClient.Ping(ctx)
	if pingErr != nil {
		t.Fatal(pingErr)
	}

	// String 和 Value 覆盖基础读写、结构化过期状态及 Redis 6.2 新增的 GETDEL。
	setStringErr := redisClient.SetString(ctx, "message", "hello", time.Minute)
	message, getStringErr := redisClient.GetString(ctx, "message")
	expiration, expirationErr := redisClient.Expiration(ctx, "message")
	if setStringErr != nil || getStringErr != nil || message != "hello" || expirationErr != nil || !expiration.Exists || expiration.TTL <= 0 {
		t.Fatalf("String Set=%v Get=%q/%v Expiration=%+v/%v", setStringErr, message, getStringErr, expiration, expirationErr)
	}
	profile := rueidistpl.NewValue[integrationUser](redisClient, "profile")
	alice := integrationUser{ID: 1, Name: "Alice"}
	profileSetErr := profile.Set(ctx, alice, time.Minute)
	consumedProfile, profileGetDeleteErr := profile.GetDelete(ctx)
	if profileSetErr != nil || profileGetDeleteErr != nil || consumedProfile != alice {
		t.Fatalf("Value Set=%v GetDelete=%+v/%v", profileSetErr, consumedProfile, profileGetDeleteErr)
	}

	// Hash、List、Set 和 ZSet 覆盖四类核心集合，并保留强类型 JSON 解码结果。
	hash := rueidistpl.NewHash[integrationUser](redisClient, "users")
	hashSetErr := hash.SetMany(ctx, map[string]integrationUser{"1": alice, "2": {ID: 2, Name: "Bob"}})
	hashValues, hashGetErr := hash.GetMany(ctx, "2", "1", "missing")
	if hashSetErr != nil || hashGetErr != nil || len(hashValues) != 2 || hashValues["1"] != alice {
		t.Fatalf("Hash Set=%v GetMany=%+v/%v", hashSetErr, hashValues, hashGetErr)
	}
	list := rueidistpl.NewList[integrationUser](redisClient, "users-list")
	listLength, listPushErr := list.PushRight(ctx, alice, integrationUser{ID: 2, Name: "Bob"})
	listValues, listRangeErr := list.Range(ctx, 0, -1)
	if listPushErr != nil || listLength != 2 || listRangeErr != nil || len(listValues) != 2 || listValues[0] != alice {
		t.Fatalf("List Push=%d/%v Range=%+v/%v", listLength, listPushErr, listValues, listRangeErr)
	}
	set := redisClient.StringSet("tags")
	setAdded, setAddErr := set.Add(ctx, "go", "redis")
	setMembers, setMembersErr := set.Members(ctx)
	if setAddErr != nil || setAdded != 2 || setMembersErr != nil || len(setMembers) != 2 {
		t.Fatalf("Set Add=%d/%v Members=%+v/%v", setAdded, setAddErr, setMembers, setMembersErr)
	}
	sortedSet := redisClient.SortedSet("scores")
	sortedSetErr := sortedSet.Add(ctx, "alice", 100)
	score, scoreErr := sortedSet.Score(ctx, "alice")
	if sortedSetErr != nil || scoreErr != nil || score != 100 {
		t.Fatalf("SortedSet Add=%v Score=%f/%v", sortedSetErr, score, scoreErr)
	}

	// Stream 的普通读写和 XAUTOCLAIM 是 Redis 6.2 兼容边界中的关键路径。
	stream := rueidistpl.NewStream[integrationUser](redisClient, "events")
	groupErr := stream.CreateGroup(ctx, "workers", "0")
	entryID, streamAddErr := stream.Add(ctx, alice)
	delivered, readGroupErr := stream.ReadGroup(ctx, "workers", "worker-1", 10)
	claimed, claimErr := stream.AutoClaim(ctx, "workers", "worker-2", 0, "0-0", 10)
	if groupErr != nil || streamAddErr != nil || entryID == "" || readGroupErr != nil || len(delivered) != 1 {
		t.Fatalf("Stream Group=%v Add=%q/%v Read=%+v/%v", groupErr, entryID, streamAddErr, delivered, readGroupErr)
	}
	if claimErr != nil || len(claimed.Entries) != 1 || claimed.Entries[0].Value != alice {
		t.Fatalf("Stream AutoClaim=%+v/%v", claimed, claimErr)
	}

	// Bitmap、Geo 和 HyperLogLog 均早于项目基线，真实测试防止便利封装误用高版本参数。
	bitmap := redisClient.Bitmap("active")
	previousBit, bitmapSetErr := bitmap.Set(ctx, 7, true)
	currentBit, bitmapGetErr := bitmap.Get(ctx, 7)
	geo := redisClient.Geo("cities")
	geoAdded, geoAddErr := geo.Add(ctx, rueidistpl.GeoPoint{Member: "shanghai", Longitude: 121.4737, Latitude: 31.2304})
	hll := redisClient.HyperLogLog("visitors")
	hllChanged, hllAddErr := hll.Add(ctx, "u1", "u2", "u1")
	hllCount, hllCountErr := hll.Count(ctx)
	if bitmapSetErr != nil || previousBit || bitmapGetErr != nil || !currentBit || geoAddErr != nil || geoAdded != 1 {
		t.Fatalf("Bitmap Set=%t/%v Get=%t/%v Geo=%d/%v", previousBit, bitmapSetErr, currentBit, bitmapGetErr, geoAdded, geoAddErr)
	}
	if hllAddErr != nil || !hllChanged || hllCountErr != nil || hllCount != 2 {
		t.Fatalf("HyperLogLog Add=%t/%v Count=%d/%v", hllChanged, hllAddErr, hllCount, hllCountErr)
	}

	// DeleteEach 使用两个不同 hash tag，单节点也可验证逐 key 结果顺序和明细字段。
	deleteResult := redisClient.DeleteEach(ctx, "{account-1}:missing", "{account-2}:missing")
	wantKeys := []string{"{account-1}:missing", "{account-2}:missing"}
	if len(deleteResult.Results) != len(wantKeys) {
		t.Fatalf("DeleteEach result length=%d want=%d result=%+v", len(deleteResult.Results), len(wantKeys), deleteResult)
	}
	actualKeys := []string{deleteResult.Results[0].Key, deleteResult.Results[1].Key}
	if deleteResult.Err != nil || deleteResult.Deleted != 0 || !slices.Equal(actualKeys, wantKeys) {
		t.Fatalf("DeleteEach result=%+v", deleteResult)
	}

	// 清理由唯一前缀隔离，但仍主动删除主要结构，方便本地反复运行观察 Redis。
	_ = hash.Clear(ctx)
	_ = list.Clear(ctx)
	_ = set.Clear(ctx)
	_ = sortedSet.Clear(ctx)
	_ = stream.Clear(ctx)
	_ = bitmap.Clear(ctx)
	_ = geo.Clear(ctx)
	_ = hll.Clear(ctx)
}
