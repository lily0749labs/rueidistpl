package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lily0749labs/rueidistpl"
	"github.com/redis/rueidis"
)

const (
	defaultRedisAddr     = "127.0.0.1:16479"
	defaultRedisPassword = "redis1234"
	defaultKeyPrefix     = "rueidistpl:example"
)

type User struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func envOrDefault(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	prefix := envOrDefault("REDIS_KEY_PREFIX", defaultKeyPrefix)
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{envOrDefault("REDIS_ADDR", defaultRedisAddr)},
			Password:    envOrDefault("REDIS_PASSWORD", defaultRedisPassword),
		},
		CacheTTL:  10 * time.Minute,
		KeyPrefix: prefix,
	})
	if err != nil {
		return err
	}
	defer redisClient.Close()
	if err := redisClient.Ping(ctx); err != nil {
		return err
	}

	users := map[string]User{
		"1": {ID: 1, Name: "Alice"},
		"2": {ID: 2, Name: "Bob"},
	}

	userCache := rueidistpl.NewCache[User](redisClient, "cache:user")
	if err := userCache.Set(ctx, "1", users["1"]); err != nil {
		return err
	}
	cachedUser, err := userCache.Get(ctx, "1")
	if err != nil {
		return err
	}
	fmt.Printf("TTL 缓存: %+v\n", cachedUser)

	profile := rueidistpl.NewValue[User](redisClient, "profile:1")
	if err := profile.Set(ctx, users["1"], time.Hour); err != nil {
		return err
	}
	storedProfile, err := profile.Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("String JSON 对象: %+v\n", storedProfile)
	expiration, err := profile.Expiration(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("结构化过期状态: exists=%t persistent=%t ttl=%s\n", expiration.Exists, expiration.Persistent, expiration.TTL)
	consumedProfile, err := profile.GetDelete(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("原子读取并删除: %+v\n", consumedProfile)

	userHash := rueidistpl.NewHash[User](redisClient, "users")
	if err := userHash.SetMany(ctx, users); err != nil {
		return err
	}
	storedUsers, err := userHash.All(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Hash 对象数量: %d\n", len(storedUsers))

	ranking := rueidistpl.NewRanking[User](redisClient, "ranking")
	if err := ranking.Upsert(ctx, "1", 100, users["1"]); err != nil {
		return err
	}
	if err := ranking.Upsert(ctx, "2", 80, users["2"]); err != nil {
		return err
	}
	topUsers, err := ranking.Top(ctx, 10)
	if err != nil {
		return err
	}
	fmt.Printf("排行榜: %+v\n", topUsers)

	scores := redisClient.SortedSet("scores")
	if err := scores.AddMany(ctx,
		rueidistpl.ScoredMember{Member: "alice", Score: 100},
		rueidistpl.ScoredMember{Member: "bob", Score: 80},
	); err != nil {
		return err
	}
	topScores, err := scores.ReverseRange(ctx, 0, 9)
	if err != nil {
		return err
	}
	fmt.Printf("ZSet: %+v\n", topScores)

	recentUsers := rueidistpl.NewList[User](redisClient, "recent-users")
	if err := recentUsers.Clear(ctx); err != nil {
		return err
	}
	if _, err := recentUsers.PushRight(ctx, users["1"], users["2"]); err != nil {
		return err
	}
	listedUsers, err := recentUsers.Range(ctx, 0, -1)
	if err != nil {
		return err
	}
	fmt.Printf("泛型 List: %+v\n", listedUsers)

	onlineUsers := rueidistpl.NewSet[User](redisClient, "online-users")
	if err := onlineUsers.Clear(ctx); err != nil {
		return err
	}
	if _, err := onlineUsers.Add(ctx, users["1"], users["2"]); err != nil {
		return err
	}
	userPage, err := onlineUsers.Scan(ctx, 0, "", 100)
	if err != nil {
		return err
	}
	fmt.Printf("泛型 Set 分页: cursor=%d values=%+v\n", userPage.Cursor, userPage.Values)

	tags := redisClient.StringSet("tags")
	if _, err := tags.Add(ctx, "go", "redis", "rueidis"); err != nil {
		return err
	}
	members, err := tags.Members(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Set 成员: %v\n", members)

	events := rueidistpl.NewStream[User](redisClient, "user-events")
	if err := events.Clear(ctx); err != nil {
		return err
	}
	if err := events.CreateGroup(ctx, "workers", "0"); err != nil {
		return err
	}
	if _, err := events.AddMaxLen(ctx, users["1"], 1000); err != nil {
		return err
	}
	delivered, err := events.ReadGroup(ctx, "workers", "worker-1", 10)
	if err != nil {
		return err
	}
	claimed, err := events.AutoClaim(ctx, "workers", "worker-2", 0, "0-0", 10)
	if err != nil {
		return err
	}
	if len(delivered) > 0 {
		if _, err := events.Ack(ctx, "workers", delivered[0].ID); err != nil {
			return err
		}
	}
	groups, err := events.Groups(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Stream: delivered=%d claimed=%d groups=%+v\n", len(delivered), len(claimed.Entries), groups)

	delayQueue := rueidistpl.NewDelayQueue[User](redisClient, "welcome-jobs")
	if err := delayQueue.Clear(ctx); err != nil {
		return err
	}
	if err := delayQueue.Schedule(ctx, "welcome-1", time.Now().Add(-time.Second), users["1"]); err != nil {
		return err
	}
	dueJobs, err := delayQueue.Claim(ctx, time.Minute, 100)
	if err != nil {
		return err
	}
	for _, job := range dueJobs {
		acked, ackErr := delayQueue.Ack(ctx, job.ID, job.Token)
		if ackErr != nil {
			return ackErr
		}
		if !acked {
			return fmt.Errorf("延迟任务 %q 的处理租约已失效", job.ID)
		}
	}
	fmt.Printf("延迟队列到期任务: %+v\n", dueJobs)

	notifications := rueidistpl.NewPubSub[User](redisClient, "notifications")
	receivers, err := notifications.Publish(ctx, "created", users["1"])
	if err != nil {
		return err
	}
	fmt.Printf("Pub/Sub 当前接收者: %d\n", receivers)

	count, err := redisClient.Counter("visits").Increment(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("访问计数: %d\n", count)

	// 批量入口逐 key 发送命令，可用于不同 Cluster Slot，并保留部分成功明细。
	if err := redisClient.SetString(ctx, "{batch-1}:value", "one"); err != nil {
		return err
	}
	existsResult := redisClient.ExistsEach(ctx, "{batch-1}:value", "{batch-2}:missing")
	if existsResult.Err != nil {
		return existsResult.Err
	}
	fmt.Printf("逐 key EXISTS: %+v\n", existsResult)
	deleteResult := redisClient.DeleteEach(ctx, "{batch-1}:value", "{batch-2}:missing")
	if deleteResult.Err != nil {
		return deleteResult.Err
	}
	fmt.Printf("逐 key DEL: %+v\n", deleteResult)

	lockCtx, unlock, err := redisClient.TryLock(ctx, "refresh-ranking")
	if err != nil {
		return err
	}
	defer unlock()
	select {
	case <-lockCtx.Done():
		return lockCtx.Err()
	default:
		fmt.Println("已获得可自动续期的分布式锁")
	}

	return nil
}
