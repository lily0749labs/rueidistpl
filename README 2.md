# rueidistpl

`rueidistpl` 是 Rueidis 的业务友好薄封装：调用方使用简洁的类型化 API，底层缓存、Redis 命令和分布式锁仍分别由 `gocache`、Rueidis、`rueidislock` 实现。

## 安装

要求 Go 1.25 或更高版本：

```bash
go get github.com/Lily0749/redis/rueidistpl@v0.1.0
```

代码中统一使用下面的导入路径：

```go
import "github.com/Lily0749/redis/rueidistpl"
```

## 能力概览

| 场景 | 对外 API | 底层实现 |
| --- | --- | --- |
| TTL 对象缓存 | `NewCache[T]` | `gocache` Rueidis Store |
| 单个 JSON 对象 | `NewValue[T]` | Redis String + JSON |
| Hash 对象表 | `NewHash[T]` | Rueidis Hash + JSON |
| 对象排行榜 | `NewRanking[T]` | Hash + ZSet + 原子 Lua |
| 计数器 | `redis.Counter` | `GET/SET/INCRBY` |
| 字符串 List、Set | `redis.StringList`、`redis.StringSet` | Rueidis Builder |
| 对象 List | `NewList[T]` | Redis List + JSON |
| 对象 Set | `NewSet[T]` | Redis Set + JSON |
| 稀疏对象 Array | `NewArray[T]` | Redis 8.8+ 原生 Array + JSON |
| 位图 | `redis.Bitmap` | Redis String 位操作 |
| 地理位置 | `redis.Geo` | Redis Geo/ZSet |
| 近似去重 | `redis.HyperLogLog` | Redis HyperLogLog |
| JSON 文档 | `NewJSONDocument[T]` | RedisJSON + JSONPath |
| 时间序列 | `redis.TimeSeries` | RedisTimeSeries |
| 向量集合 | `redis.VectorSet` | Redis 8.0+ Vector Set |
| Bloom/Cuckoo Filter | `redis.BloomFilter`、`redis.CuckooFilter` | RedisBloom |
| 频率统计 | `redis.CountMinSketch`、`redis.TopK` | RedisBloom |
| 分布统计 | `redis.TDigest` | RedisBloom t-digest |
| 通用有序集合 | `redis.SortedSet` | Redis ZSet |
| 消息流 | `NewStream[T]` | Redis Stream + JSON |
| 发布订阅 | `NewPubSub[T]` | 普通、模式、Sharded Pub/Sub |
| 幂等控制 | `redis.Idempotency` | `SET NX PX` |
| 轻量延迟任务 | `NewDelayQueue[T]` | Hash + ZSet + 原子 Lua |
| 分布式锁 | `redis.Lock`、`redis.TryLock` | `rueidislock` |
| 特殊 Redis 命令 | `redis.Client()` | 原生 Rueidis Client |

封装层只负责统一配置、JSON 转换、键命名和少量业务数据布局，不重新实现连接池、缓存一致性、锁续期或 Redis 命令协议。

所有返回 `(T, error)` 的单对象读取在 `error != nil` 时都会返回 `T` 的零值，避免暴露 JSON 部分解码结果；调用方仍应遵循 Go 的错误处理约定，不使用错误情况下的其他返回值。

## 创建客户端

```go
redisClient, err := rueidistpl.New(rueidistpl.Config{
    ClientOption: rueidis.ClientOption{
        InitAddress: []string{"127.0.0.1:6379"},
        Password:    "",
    },
    KeyPrefix: "myapp",        // 所有业务 key 自动加 myapp:
    CacheTTL:  10*time.Minute, // 对象缓存默认 TTL
})
if err != nil {
    return err
}
defer redisClient.Close()
```

锁客户端按需创建：不调用 `Lock` 或 `TryLock` 就不会额外建立锁连接。单节点默认 `KeyMajority: 1`；多节点必须通过 `Config.LockerOption` 设置正确的多数派。`LockerOption.ClientOption` 由统一的 `Config.ClientOption` 覆盖，避免维护两份连接配置。

## 常用调用

### 结构化过期状态与跨 Slot 批量操作

```go
expiration, err := redisClient.Expiration(ctx, "session:1")
if err != nil {
    return err
}
switch {
case !expiration.Exists:
    // key 不存在
case expiration.Persistent:
    // key 存在且永久保存
default:
    fmt.Printf("剩余时间：%s\n", expiration.TTL)
}

batch := redisClient.DeleteEach(ctx, "{account-1}:profile", "{account-2}:profile")
for _, item := range batch.Results {
    fmt.Printf("key=%s deleted=%t err=%v\n", item.Key, item.Deleted, item.Err)
}
if batch.Err != nil {
    // 部分失败时，batch.Deleted 和其他成功 Results 仍然有效。
    return batch.Err
}
```

`ExpirationInfo` 已将 PTTL/HPTTL 的 `-1ms`、`-2ms` 分别解释为 `Persistent` 和 `Exists=false`。除通用 `Redis.Expiration` 外，`Value.Expiration` 和 Redis 7.4+ 的 `Hash.ExpirationField` 也返回相同结构；原有 `TTL`/`TTLField` 继续保留以兼容旧代码。

`DeleteEach`、`ExistsEach` 会为每个 key 单独发送命令，输入可以位于不同 Redis Cluster Slot，返回明细顺序与输入一致。批次级 `Err` 由失败项聚合而成，可使用 `errors.Is` 判断根因；原有 `Delete`/`Exists` 仍可使用，并在失败时返回已经确认的部分计数。

### TTL 对象缓存

```go
users := rueidistpl.NewCache[User](redisClient, "cache:user")

err := users.Set(ctx, "1", user)                  // 使用默认 TTL
err = users.Set(ctx, "2", user, 30*time.Minute) // 覆盖本次 TTL
user, err = users.Get(ctx, "1")
err = users.Delete(ctx, "1")
```

`Prefix()` 可查看包含全局前缀的真实缓存命名空间。`rueidistpl.IsNotFound(err)` 同时识别 gocache 未命中和 Rueidis Redis Nil，可统一用于 Cache、Value、Hash、List、Ranking 等单值读取。

Rueidis Store 的过期精度为秒；小于 1 秒的正 TTL 会自动提升为 1 秒。

不需要 gocache，只想用一个 String 保存 JSON 对象时：

```go
profile := rueidistpl.NewValue[User](redisClient, "profile:1")
created, err := profile.SetNX(ctx, user, time.Hour)
user, err = profile.Get(ctx)
ttl, err := profile.TTL(ctx)
expiration, err := profile.Expiration(ctx)
consumed, err := profile.GetDelete(ctx) // 原子读取并删除，Redis 6.2+
```

### Hash 对象表

```go
users := rueidistpl.NewHash[User](redisClient, "users")

err := users.Set(ctx, "1", user)
err = users.SetMany(ctx, map[string]User{"1": user1, "2": user2})
user, err := users.Get(ctx, "1")
all, err := users.All(ctx)
deleted, err := users.Delete(ctx, "1")
```

还支持 `SetNX`、`GetMany`、`Keys`、`Values` 和游标式 `Scan`。`SetMany` 会按 field 字典序构造命令，使日志和执行顺序保持稳定。`IncrementInt`、`IncrementFloat` 用于保存 JSON 数字的 field；`ExpireField`、`TTLField`、`ExpirationField`、`PersistField` 需要 Redis 7.4+。

### 对象排行榜

```go
ranking := rueidistpl.NewRanking[User](redisClient, "ranking")

err := ranking.Upsert(ctx, "user-1", 100, user)
top, err := ranking.Top(ctx, 10)
removed, err := ranking.Remove(ctx, "user-1")
```

排行榜的 ZSet 只保存稳定 ID，对象保存在 Hash，因此更新对象不会产生重复成员。两个键自动使用相同的 Redis Cluster hash tag，`Upsert` 和 `Remove` 通过 Lua 原子更新，`Get` 和 `Range` 也在单次 Lua 中读取一致快照，避免并发更新时组合出跨时刻数据。`Upsert` 会在执行 Lua 前拒绝 `NaN` 分数，避免 HSET 成功后 ZADD 才失败。

### 计数器、List、Set、ZSet

```go
count, err := redisClient.Counter("visits").Increment(ctx)

events := redisClient.StringList("events")
_, err = events.PushRight(ctx, "created", "paid")
items, err := events.Range(ctx, 0, -1)

jobs := rueidistpl.NewList[Job](redisClient, "jobs")
_, err = jobs.PushRight(ctx, job1, job2)
job, err := jobs.PopLeft(ctx)

tags := redisClient.StringSet("tags")
_, err = tags.Add(ctx, "go", "redis")
contains, err := tags.Contains(ctx, "rueidis")
tagPage, err := tags.Scan(ctx, 0, "go*", 100)

onlineUsers := rueidistpl.NewSet[User](redisClient, "online-users")
_, err = onlineUsers.Add(ctx, user1, user2)
containsUser, err := onlineUsers.Contains(ctx, user1)
users, err := onlineUsers.Members(ctx)
userPage, err := onlineUsers.Scan(ctx, 0, "", 100)

scores := redisClient.SortedSet("scores")
err = scores.Add(ctx, "alice", 100)
score, err := scores.Increment(ctx, "alice", 10)
top, err := scores.ReverseRange(ctx, 0, 9)
```

`redisClient.List` 保留为 `StringList` 的简写；保存结构体时使用 `NewList[T]`。`StringSet` 用于原始字符串，`NewSet[T]` 用于 JSON 对象；两者都提供游标式 `Scan`，大集合应优先分批扫描而不是一次调用 `Members`。泛型 Set 的 `pattern` 匹配原始 JSON 字符串，不是结构化字段查询。对象是否相同也由 JSON 字节串决定，自定义 `MarshalJSON` 应保证输出稳定。

`Counter`、`StringList`、`StringSet`、`Value`、`Hash`、泛型 `List/Set`、`SortedSet` 和 `Stream` 都可通过 `Key()` 查看真实 Redis key；Cache 和 Pub/Sub 则使用 `Prefix()` 查看命名空间。

`SortedSet` 只保存字符串 member；需要同时保存对象内容时使用 `NewRanking[T]`。两种 ZSet 封装都拒绝 Redis 不接受的 `NaN`，正负无穷仍可作为合法分数边界。

### Redis 8.8+ 原生 Array

```go
users := rueidistpl.NewArray[User](redisClient, "users-by-index")

// 连续写入，以及按任意下标构造稀疏数组。
_, err := users.Set(ctx, 0, user1, user2)
_, err = users.SetMany(ctx, map[int64]User{10: user10, 1000: user1000})

// ARMGET/ARGETRANGE 保留空槽；Exists=false 时 Value 是 User 零值。
items, err := users.GetMany(ctx, 0, 9, 10)
rangeItems, err := users.Range(ctx, 10, 0) // start>end 时倒序

// ARSCAN 只返回非空槽，适合扫描很稀疏的下标范围。
usedItems, err := users.Scan(ctx, 0, 1000, 100)

// 游标式追加和固定窗口 Ring。
_, err = users.Insert(ctx, user1, user2)
nextIndex, err := users.Next(ctx)
positioned, err := users.Seek(ctx, 100)
_, err = users.Ring(ctx, 1000, user3)
```

`ArrayItem[T]` 同时返回 `Index`、`Exists` 和强类型 `Value`，因此合法的对象零值不会与 Redis Array 空槽混淆。`Len` 返回“最高使用下标 + 1”的逻辑长度，`Count` 返回非空槽数；另有 `LastItems`、`Delete`、`DeleteRanges` 和 `Clear`。`Scan` 同时兼容 Redis 8.10.0 RESP3 的嵌套 pair 响应和命令文档描述的扁平 pair 响应。

该封装保存的是 JSON 字符串，覆盖常用的强类型结构操作。按原始字符串执行的 `ARGREP`、数值/位聚合 `AROP` 和运维信息 `ARINFO` 不在泛型层重复抽象，可通过 `redisClient.Client().B()` 的 Rueidis 强类型 Builder 直接调用。Array 命令从 Redis 8.8.0 才提供；连接旧版本不会影响其他结构，但调用 Array 方法会返回 Redis 未知命令错误。

### Bitmap、Geo 与 HyperLogLog

```go
active := redisClient.Bitmap("active-users")
_, err := active.Set(ctx, userID, true)
enabled, err := active.Get(ctx, userID)
activeCount, err := active.Count(ctx)

cities := redisClient.Geo("cities")
_, err = cities.Add(ctx, rueidistpl.GeoPoint{
    Member: "shanghai", Longitude: 121.4737, Latitude: 31.2304,
})
nearby, err := cities.SearchRadiusByMember(
    ctx, "shanghai", 10, rueidistpl.GeoKilometers,
    rueidistpl.GeoSearchOptions{Count: 20},
)

visitors := redisClient.HyperLogLog("daily-visitors:{2026-08-23}")
_, err = visitors.Add(ctx, "user-1", "user-2")
estimated, err := visitors.Count(ctx)
```

Bitmap 提供位读写、位区间统计、位查找、`BITOP` 和单值 `BITFIELD_RO`；Geo 提供批量坐标、距离、半径搜索、删除和计数；HyperLogLog 提供添加、联合计数和合并。`Bitmap.Combine`、`HyperLogLog.CountWith`/`MergeFrom` 涉及多个 key，在 Cluster 中调用方仍需确保 key 使用相同 hash tag。

### JSON 文档、TimeSeries 与 Vector Set

```go
profile := rueidistpl.NewJSONDocument[User](redisClient, "profile:1")
err := profile.Set(ctx, user)
err = profile.SetPath(ctx, "$.name", "Alice")
user, err = profile.Get(ctx)

temperature := redisClient.TimeSeries("temperature:s1")
err = temperature.Create(ctx, rueidistpl.TimeSeriesOptions{
    Retention: 24*time.Hour,
    Labels:    map[string]string{"sensor": "s1"},
})
_, err = temperature.AddNow(ctx, 21.5)
samples, err := temperature.RangeAll(ctx, 100)

products := redisClient.VectorSet("products")
_, err = products.Add(ctx, "p1", []float64{1, 0.5, -1})
matches, err := products.Search(ctx, []float64{1, 0.4, -1},
    rueidistpl.VectorSearchOptions{Count: 10})
```

`JSONDocument[T]` 的根读写保持强类型，局部 JSONPath 读取返回 `json.RawMessage`，不会丢失多匹配结果。`TimeSeries` 覆盖创建、写入、最新值、正反向范围、增减和区间删除。`VectorSet` 使用可读的 `VALUES` 向量参数，覆盖属性、相似搜索、维度、成员判断与删除；`VSIM WITHSCORES` 同时兼容 RESP3 Map 和 RESP2 Array 响应，并稳定按相似度降序返回。

Redis 8 标准发行版已内置 JSON、TimeSeries 和概率结构；Redis 7.x 等旧版需要安装相应模块。Vector Set 从 Redis 8.0 提供，其中 `VectorSet.Contains` 使用的 `VISMEMBER` 需要 Redis 8.2+。

### 概率数据结构

```go
bloom := redisClient.BloomFilter("seen-orders")
err := bloom.Reserve(ctx, 0.01, 1_000_000)
_, err = bloom.Add(ctx, orderID)
possiblySeen, err := bloom.Exists(ctx, orderID)

frequency := redisClient.CountMinSketch("page-views")
err = frequency.InitByProbability(ctx, 0.01, 0.001)
counts, err := frequency.Increment(ctx,
    rueidistpl.CountMinIncrement{Item: "/home", Delta: 1},
)

latency := redisClient.TDigest("request-latency")
err = latency.Create(ctx, 100)
err = latency.Add(ctx, 12.5, 20, 31.2)
percentiles, err := latency.Quantiles(ctx, 0.5, 0.95, 0.99)
```

另有支持删除和重复计数的 `CuckooFilter`，以及返回结构化挤出结果和带计数榜单的 `TopK`。这些结构都是近似算法：Bloom/Cuckoo 的 `true` 可能是假阳性，CMS/TopK/t-digest 返回估算值；需要精确集合、计数或排序时应继续使用 Set、Hash/Counter 或 ZSet。

### Stream

```go
orders := rueidistpl.NewStream[Order](redisClient, "orders")
err := orders.CreateGroup(ctx, "billing", "0")
id, err := orders.AddMaxLen(ctx, order, 100_000) // MAXLEN ~ 近似限容

entries, err := orders.ReadGroup(ctx, "billing", "worker-1", 20)
// 业务处理成功后确认
_, err = orders.Ack(ctx, "billing", entries[0].ID)

// 定时接管崩溃 worker 长时间未确认的消息
claimed, err := orders.AutoClaim(ctx, "billing", "worker-2", 30*time.Second, "0-0", 100)

groups, err := orders.Groups(ctx)
consumers, err := orders.Consumers(ctx, "billing")
err = orders.SetGroupID(ctx, "billing", "0-0") // 重放时调整投递位置
```

还提供 `Range`、`Read`、`ReadBlock`、`Pending`、`Claim`、`TrimMaxLen`、`DeleteConsumer`、`DeleteGroup` 和 `Delete`。`Claim`/`AutoClaim` 的最小空闲时间不接受负数，正的亚毫秒值会提升为 1ms。`AutoClaim` 需要 Redis 6.2+，`DeletedIDs` 只会由 Redis 7.0+ 返回。

`Groups` 和 `Consumers` 基于 `XINFO` 返回强类型运维状态。`EntriesReadKnown`、`LagKnown`、`InactiveKnown` 用于区分真实零值与 Redis 旧版本或当前状态无法提供数据的情况。`SetGroupID` 可用于重放或跳过历史消息，但不会清理已有 Pending。

`AddMaxLen` 和 `TrimMaxLen` 使用 `MAXLEN ~` 近似裁剪，换取比精确裁剪更稳定的写入性能，实际长度可能暂时略超过阈值。每条消息统一存放在名为 `data` 的 JSON field 中；需要多字段原始消息时直接使用 Rueidis。

### Pub/Sub

```go
notifications := rueidistpl.NewPubSub[Notification](redisClient, "notifications")

go func() {
    _ = notifications.Subscribe(ctx, func(message rueidistpl.PubSubMessage[Notification]) error {
        return handle(message.Value)
    }, "created")
}()

_, err := notifications.Publish(ctx, "created", notification)
```

`Subscribe` 会阻塞到 context 取消、连接失败或 handler 返回错误；另有 `SubscribePattern`、`SubscribeShard` 和 `PublishShard`。Pub/Sub 不持久化消息，需要消费确认与重试时使用 Stream。

### 幂等与轻量延迟任务

```go
first, err := redisClient.Idempotency("payments").Run(
    ctx, paymentID, 24*time.Hour,
    func(ctx context.Context) error { return charge(ctx, paymentID) },
)

queue := rueidistpl.NewDelayQueue[Job](redisClient, "email-jobs")
err = queue.Schedule(ctx, job.ID, time.Now().Add(5*time.Minute), job)
dueJobs, err := queue.Poll(ctx, time.Now(), 100)
```

`Idempotency.Run` 为每次执行写入随机所有权令牌；业务函数失败时只在令牌仍属于本次执行的情况下删除标记。即使 TTL 已到期且另一个请求重新取得执行权，旧请求也不会误删新标记。

`DelayQueue.Poll` 会原子取出并删除到期任务，语义是至多一次，而且该破坏性 Lua 不启用客户端自动重试。单次 `limit` 不得超过 `MaxDelayQueuePollBatch`（1000），调用方应循环分批拉取。由于任务在返回前已删除，即使 JSON 解码失败也不会重新入队；要求 ACK、失败重试、死信或持久 worker 时，优先采用成熟任务队列，或使用本包的 Stream 封装。

### 分布式锁

```go
lockCtx, unlock, err := redisClient.TryLock(ctx, "refresh-ranking")
if err != nil {
    return err
}
defer unlock()

select {
case <-lockCtx.Done():
    return lockCtx.Err() // 续期失败，停止受保护任务
default:
}
```

需要等待锁时使用 `redisClient.Lock`；只尝试一次时使用 `TryLock`。锁被占用时，`TryLock` 返回 `rueidislock.ErrNotLocked`。

## 原生命令出口

封装未覆盖的命令直接使用 Rueidis，不需要继续扩充工具类：

```go
key := redisClient.Key("stream:orders")
result := redisClient.Client().Do(ctx, redisClient.Client().B().Xlen().Key(key).Build())
```

## 版本要求

最低支持 Go 1.25。生产环境建议使用所选 Go 系列的最新安全补丁版本。

### Redis 版本边界

| 功能 | 最低 Redis 版本 |
| --- | --- |
| 基础 String/Hash/List/Set/ZSet、缓存、普通 Stream | 6.2（项目默认基线） |
| `Value.GetDelete`、`Stream.AutoClaim` | 6.2 |
| Sharded Pub/Sub、`AutoClaimResult.DeletedIDs`、XINFO `entries-read/lag` | 7.0 |
| XINFO Consumer `inactive` | 7.2 |
| Hash field `ExpireField`/`TTLField`/`PersistField` | 7.4 |
| Bitmap、Geo、HyperLogLog | 6.2 基线内可用 |
| JSONDocument、TimeSeries、Bloom/Cuckoo/CMS/TopK/TDigest | Redis 8 标准版；旧版需要对应模块 |
| Vector Set 基础操作与相似搜索 | 8.0 |
| `VectorSet.Contains` (`VISMEMBER`) | 8.2 |
| 原生 Array：`NewArray[T]` 的全部方法 | 8.8 |

项目保持 Redis 6.2 基础能力兼容，并将 Redis 8.10.0 作为当前已验证的最高版本。不使用高版本便利 API 时，其他封装不会主动发送这些新命令；这里的“最高版本”表示已验证上限，不会主动拒绝后续 Redis 版本。

### 部署拓扑

| 拓扑 | 支持情况 | 说明 |
| --- | --- | --- |
| Standalone | 支持 | 可使用全部符合 Redis 版本要求的封装 |
| Cluster | 支持核心路由 | 跨 Slot 批量操作会逐 key 路由；组合结构自动使用 hash tag |
| Sentinel | 复用 Rueidis | 通过 `ClientOption.Sentinel` 配置连接和认证 |

Cluster 中，`DeleteEach` 和 `ExistsEach` 会逐 key 路由，可以接收跨 Slot 输入。Ranking 和 DelayQueue 会自动让内部 key 共用 Slot。`Bitmap.Combine`、`HyperLogLog.CountWith`/`MergeFrom` 等 Redis 原生多 key 命令仍要求调用方在逻辑 key 中使用相同 `{hash-tag}`。

### 升级建议

- `v0.x` 阶段的补丁版本保持公开 API 兼容；次版本可能调整 API，并在更新日志中说明迁移方式。
- 升级 rueidistpl 前阅读根目录 [CHANGELOG.md](../CHANGELOG.md)，重点关注返回值语义、最低 Redis 版本和 key 组织变化。
- 升级 Redis 或变更 Cluster 部署前，应在目标版本和拓扑上验证高版本命令、节点公布地址、TLS/认证、跨 Slot 路由和 hash tag。
