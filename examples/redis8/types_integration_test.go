package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lily0749labs/rueidistpl"
	"github.com/redis/rueidis"
)

// TestRedis8TypesWithRealRedis 使用真实 Redis 8.10 验证高版本数据结构。
// 这些命令不属于 Redis 6.2 核心基线，因此与普通 examples 测试分开运行。
func TestRedis8TypesWithRealRedis(t *testing.T) {
	// 普通 go test 默认跳过真实连接；执行 make test-examples-redis8 时才开启。
	if os.Getenv("RUEIDISTPL_EXAMPLES_REDIS8_INTEGRATION") != "1" {
		t.Skip("set RUEIDISTPL_EXAMPLES_REDIS8_INTEGRATION=1 to run Redis 8 example tests")
	}

	// 与可运行示例复用同一组连接变量，并保留环境变量覆盖能力。
	redisAddress := envOrDefault("REDIS_ADDR", redis8Address)
	redisPassword := envOrDefault("REDIS_PASSWORD", redis8Password)
	baseKeyPrefix := envOrDefault("REDIS_KEY_PREFIX", redis8KeyPrefix)
	// 唯一 key 前缀保证每次执行从空结构开始，断言不会受历史测试数据影响。
	keyPrefix := fmt.Sprintf("%s:test:%d", baseKeyPrefix, time.Now().UnixNano())
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{redisAddress},
			Password:    redisPassword,
		},
		KeyPrefix: keyPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pingErr := redisClient.Ping(ctx)
	if pingErr != nil {
		t.Fatalf("真实 Redis 8.10 连接失败：%v", pingErr)
	}

	alice := product{ID: 1, Name: "Alice"}
	bob := product{ID: 2, Name: "Bob"}

	t.Run("Redis 8.8+ 原生 Array", func(t *testing.T) {
		// Array 是稀疏下标结构，Exists 字段必须能区分空槽和 product 的合法零值。
		array := rueidistpl.NewArray[product](redisClient, "products")
		defer func() { _ = array.Clear(ctx) }()
		createdSlots, setErr := array.SetMany(ctx, map[int64]product{0: alice, 10: bob})
		items, getErr := array.GetMany(ctx, 0, 1, 10)
		arrayLength, lengthErr := array.Len(ctx)
		arrayCount, countErr := array.Count(ctx)
		if setErr != nil || createdSlots != 2 || getErr != nil || len(items) != 3 ||
			!items[0].Exists || items[0].Value != alice || items[1].Exists || !items[2].Exists || items[2].Value != bob ||
			lengthErr != nil || arrayLength != 11 || countErr != nil || arrayCount != 2 {
			t.Fatalf("Array Set=%d/%v Get=%+v/%v Len=%d/%v Count=%d/%v", createdSlots, setErr, items, getErr, arrayLength, lengthErr, arrayCount, countErr)
		}
	})

	t.Run("RedisJSON 强类型文档", func(t *testing.T) {
		// 根文档以 product 强类型读写，JSONPath 只更新 name 字段而保留 ID。
		document := rueidistpl.NewJSONDocument[product](redisClient, "document")
		defer func() { _ = document.Clear(ctx) }()
		setErr := document.Set(ctx, alice)
		setPathErr := document.SetPath(ctx, "$.name", "Alice v2")
		loaded, getErr := document.Get(ctx)
		exists, existsErr := document.Exists(ctx)
		if setErr != nil || setPathErr != nil || getErr != nil || loaded != (product{ID: 1, Name: "Alice v2"}) || existsErr != nil || !exists {
			t.Fatalf("JSONDocument Set=%v SetPath=%v Get=%+v/%v Exists=%t/%v", setErr, setPathErr, loaded, getErr, exists, existsErr)
		}
	})

	t.Run("RedisTimeSeries 时间序列", func(t *testing.T) {
		// 使用固定时间戳写入两个样本，RangeAll 应保持时间升序和浮点值。
		series := redisClient.TimeSeries("temperature")
		defer func() { _ = series.Clear(ctx) }()
		createErr := series.Create(ctx, rueidistpl.TimeSeriesOptions{
			Retention: time.Hour,
			Labels:    map[string]string{"sensor": "s1"},
		})
		firstTimestamp, firstAddErr := series.Add(ctx, 1000, 20.5)
		secondTimestamp, secondAddErr := series.Add(ctx, 2000, 21.5)
		samples, rangeErr := series.RangeAll(ctx, 10)
		if createErr != nil || firstAddErr != nil || firstTimestamp != 1000 || secondAddErr != nil || secondTimestamp != 2000 ||
			rangeErr != nil || len(samples) != 2 || samples[0].Timestamp != 1000 || samples[0].Value != 20.5 || samples[1].Value != 21.5 {
			t.Fatalf("TimeSeries Create=%v Add=%d/%v,%d/%v Range=%+v/%v", createErr, firstTimestamp, firstAddErr, secondTimestamp, secondAddErr, samples, rangeErr)
		}
	})

	t.Run("Vector Set 向量检索", func(t *testing.T) {
		// 两个三维向量写入后，查询向量应首先匹配完全相同的 p1。
		vectors := redisClient.VectorSet("vectors")
		defer func() { _ = vectors.Clear(ctx) }()
		firstAdded, firstAddErr := vectors.Add(ctx, "p1", []float64{1, 0, 0})
		secondAdded, secondAddErr := vectors.Add(ctx, "p2", []float64{0.9, 0.1, 0})
		dimension, dimensionErr := vectors.Dimension(ctx)
		contains, containsErr := vectors.Contains(ctx, "p1")
		matches, searchErr := vectors.Search(ctx, []float64{1, 0, 0}, rueidistpl.VectorSearchOptions{Count: 2})
		if firstAddErr != nil || !firstAdded || secondAddErr != nil || !secondAdded || dimensionErr != nil || dimension != 3 ||
			containsErr != nil || !contains || searchErr != nil || len(matches) != 2 || matches[0].Member != "p1" {
			t.Fatalf("VectorSet Add=%t/%v,%t/%v Dim=%d/%v Contains=%t/%v Search=%+v/%v", firstAdded, firstAddErr, secondAdded, secondAddErr, dimension, dimensionErr, contains, containsErr, matches, searchErr)
		}
	})

	t.Run("Bloom Filter 存在性过滤", func(t *testing.T) {
		// Bloom Filter 允许假阳性但不应产生假阴性，刚添加的 order-1 必须返回可能存在。
		filter := redisClient.BloomFilter("bloom")
		defer func() { _ = filter.Clear(ctx) }()
		reserveErr := filter.Reserve(ctx, 0.01, 1000)
		added, addErr := filter.Add(ctx, "order-1")
		exists, existsErr := filter.Exists(ctx, "order-1")
		cardinality, cardErr := filter.Card(ctx)
		if reserveErr != nil || addErr != nil || !added || existsErr != nil || !exists || cardErr != nil || cardinality != 1 {
			t.Fatalf("Bloom Reserve=%v Add=%t/%v Exists=%t/%v Card=%d/%v", reserveErr, added, addErr, exists, existsErr, cardinality, cardErr)
		}
	})

	t.Run("Cuckoo Filter 可删除过滤", func(t *testing.T) {
		// Cuckoo Filter 支持删除和近似重复计数，删除后同一元素应不再存在。
		filter := redisClient.CuckooFilter("cuckoo")
		defer func() { _ = filter.Clear(ctx) }()
		reserveErr := filter.Reserve(ctx, 1000)
		added, addErr := filter.Add(ctx, "session-1")
		count, countErr := filter.Count(ctx, "session-1")
		deleted, deleteErr := filter.Delete(ctx, "session-1")
		existsAfterDelete, existsErr := filter.Exists(ctx, "session-1")
		if reserveErr != nil || addErr != nil || !added || countErr != nil || count != 1 || deleteErr != nil || !deleted || existsErr != nil || existsAfterDelete {
			t.Fatalf("Cuckoo Reserve=%v Add=%t/%v Count=%d/%v Delete=%t/%v Exists=%t/%v", reserveErr, added, addErr, count, countErr, deleted, deleteErr, existsAfterDelete, existsErr)
		}
	})

	t.Run("Count-Min Sketch 高频估算", func(t *testing.T) {
		// CMS 使用正整数增量记录访问频率，Query 按输入顺序返回估算结果。
		sketch := redisClient.CountMinSketch("cms")
		defer func() { _ = sketch.Clear(ctx) }()
		initErr := sketch.InitByProbability(ctx, 0.01, 0.001)
		incremented, incrementErr := sketch.Increment(ctx,
			rueidistpl.CountMinIncrement{Item: "/home", Delta: 3},
			rueidistpl.CountMinIncrement{Item: "/products", Delta: 1},
		)
		counts, queryErr := sketch.Query(ctx, "/products", "/home")
		if initErr != nil || incrementErr != nil || len(incremented) != 2 || incremented[0] != 3 || incremented[1] != 1 ||
			queryErr != nil || len(counts) != 2 || counts[0] != 1 || counts[1] != 3 {
			t.Fatalf("CMS Init=%v Increment=%v/%v Query=%v/%v", initErr, incremented, incrementErr, counts, queryErr)
		}
	})

	t.Run("Top-K 高频元素", func(t *testing.T) {
		// K=2 时，重复两次的 go 应位于结果首位，并保留估算次数。
		top := redisClient.TopK("top")
		defer func() { _ = top.Clear(ctx) }()
		reserveErr := top.Reserve(ctx, 2)
		addResults, addErr := top.Add(ctx, "go", "redis", "go")
		items, listErr := top.List(ctx)
		if reserveErr != nil || addErr != nil || len(addResults) != 3 || listErr != nil || len(items) != 2 || items[0].Item != "go" || items[0].Count != 2 {
			t.Fatalf("TopK Reserve=%v Add=%+v/%v List=%+v/%v", reserveErr, addResults, addErr, items, listErr)
		}
	})

	t.Run("t-digest 分位统计", func(t *testing.T) {
		// t-digest 保存分布摘要，最小值、最大值和 50% 分位点应与三个样本一致。
		digest := redisClient.TDigest("digest")
		defer func() { _ = digest.Clear(ctx) }()
		createErr := digest.Create(ctx, 100)
		addErr := digest.Add(ctx, 10, 20, 30)
		quantiles, quantileErr := digest.Quantiles(ctx, 0.5)
		minimum, minErr := digest.Min(ctx)
		maximum, maxErr := digest.Max(ctx)
		if createErr != nil || addErr != nil || quantileErr != nil || len(quantiles) != 1 || quantiles[0] != 20 ||
			minErr != nil || minimum != 10 || maxErr != nil || maximum != 30 {
			t.Fatalf("TDigest Create=%v Add=%v Quantile=%v/%v Min=%f/%v Max=%f/%v", createErr, addErr, quantiles, quantileErr, minimum, minErr, maximum, maxErr)
		}
	})
}
