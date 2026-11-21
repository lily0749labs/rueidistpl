// Command redis8 展示 Redis 8.10 中 JSON、TimeSeries、Vector Set、Array 和概率结构的便利 API。
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

var (
	// redis8Address 是 Redis 8 示例的默认连接地址，可由 REDIS_ADDR 环境变量覆盖。
	redis8Address = "127.0.0.1:16479"
	// redis8Password 是 Redis 8 示例的默认密码，可由 REDIS_PASSWORD 环境变量覆盖。
	redis8Password = "redis1234"
	// redis8KeyPrefix 是示例 key 的默认业务前缀，可由 REDIS_KEY_PREFIX 环境变量覆盖。
	redis8KeyPrefix = "rueidistpl:redis8-example"
)

type product struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

func envOrDefault(name, fallback string) string {
	value, exists := os.LookupEnv(name)
	if exists {
		return value
	}
	return fallback
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	// 先解析成具名变量，打断点时可以直接查看最终使用的地址、密码和 key 前缀。
	redisAddress := envOrDefault("REDIS_ADDR", redis8Address)
	redisPassword := envOrDefault("REDIS_PASSWORD", redis8Password)
	baseKeyPrefix := envOrDefault("REDIS_KEY_PREFIX", redis8KeyPrefix)
	// 末尾时间戳让示例可重复运行，且不会与其他开发者或 CI 冲突。
	keyPrefix := fmt.Sprintf("%s:%d", baseKeyPrefix, time.Now().UnixNano())
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{redisAddress},
			Password:    redisPassword,
		},
		KeyPrefix: keyPrefix,
	})
	if err != nil {
		return err
	}
	defer redisClient.Close()
	if err := redisClient.Ping(ctx); err != nil {
		return err
	}

	alice := product{ID: 1, Name: "Alice"}
	bob := product{ID: 2, Name: "Bob"}

	// Redis 8.8+ Array：稀疏下标和泛型零值通过 ArrayItem.Exists 明确区分。
	products := rueidistpl.NewArray[product](redisClient, "products")
	defer func() { _ = products.Clear(context.Background()) }()
	createdSlots, err := products.SetMany(ctx, map[int64]product{0: alice, 10: bob})
	if err != nil {
		return fmt.Errorf("array set: %w", err)
	}
	arrayItems, err := products.GetMany(ctx, 0, 1, 10)
	if err != nil {
		return fmt.Errorf("array get: %w", err)
	}
	fmt.Printf("Array: created=%d items=%+v\n", createdSlots, arrayItems)

	// Bitmap、Geo 和 HyperLogLog 是核心 Redis 结构，在 Redis 8 中继续使用相同便利入口。
	active := redisClient.Bitmap("active")
	defer func() { _ = active.Clear(context.Background()) }()
	if _, err := active.Set(ctx, alice.ID, true); err != nil {
		return fmt.Errorf("bitmap set: %w", err)
	}
	activeCount, err := active.Count(ctx)
	if err != nil {
		return fmt.Errorf("bitmap count: %w", err)
	}
	cities := redisClient.Geo("cities")
	defer func() { _ = cities.Clear(context.Background()) }()
	if _, err := cities.Add(ctx, rueidistpl.GeoPoint{Member: "shanghai", Longitude: 121.4737, Latitude: 31.2304}); err != nil {
		return fmt.Errorf("geo add: %w", err)
	}
	visitors := redisClient.HyperLogLog("visitors")
	defer func() { _ = visitors.Clear(context.Background()) }()
	if _, err := visitors.Add(ctx, "u1", "u2", "u1"); err != nil {
		return fmt.Errorf("hyperloglog add: %w", err)
	}
	visitorCount, err := visitors.Count(ctx)
	if err != nil {
		return fmt.Errorf("hyperloglog count: %w", err)
	}
	fmt.Printf("Bitmap/HyperLogLog: active=%d visitors≈%d\n", activeCount, visitorCount)

	// RedisJSON：根文档保持强类型，局部字段使用 JSONPath 更新。
	document := rueidistpl.NewJSONDocument[product](redisClient, "document")
	defer func() { _ = document.Clear(context.Background()) }()
	if err := document.Set(ctx, alice); err != nil {
		return fmt.Errorf("json set: %w", err)
	}
	if err := document.SetPath(ctx, "$.name", "Alice v2"); err != nil {
		return fmt.Errorf("json path set: %w", err)
	}
	loadedProduct, err := document.Get(ctx)
	if err != nil {
		return fmt.Errorf("json get: %w", err)
	}
	fmt.Printf("JSONDocument: %+v\n", loadedProduct)

	// RedisTimeSeries：创建带标签的序列，并读取最新样本。
	series := redisClient.TimeSeries("temperature")
	defer func() { _ = series.Clear(context.Background()) }()
	if err := series.Create(ctx, rueidistpl.TimeSeriesOptions{Retention: time.Hour, Labels: map[string]string{"sensor": "s1"}}); err != nil {
		return fmt.Errorf("time series create: %w", err)
	}
	if _, err := series.AddNow(ctx, 21.5); err != nil {
		return fmt.Errorf("time series add: %w", err)
	}
	latestSample, err := series.Get(ctx)
	if err != nil {
		return fmt.Errorf("time series get: %w", err)
	}
	fmt.Printf("TimeSeries: %+v\n", latestSample)

	// Vector Set：VALUES 形式避免调用方处理 FP32 字节序，VSIM 返回结构化相似度。
	vectors := redisClient.VectorSet("vectors")
	defer func() { _ = vectors.Clear(context.Background()) }()
	if _, err := vectors.Add(ctx, "p1", []float64{1, 0, 0}); err != nil {
		return fmt.Errorf("vector add p1: %w", err)
	}
	if _, err := vectors.Add(ctx, "p2", []float64{0.9, 0.1, 0}); err != nil {
		return fmt.Errorf("vector add p2: %w", err)
	}
	matches, err := vectors.Search(ctx, []float64{1, 0, 0}, rueidistpl.VectorSearchOptions{Count: 2})
	if err != nil {
		return fmt.Errorf("vector search: %w", err)
	}
	fmt.Printf("VectorSet: %+v\n", matches)

	// RedisBloom：分别展示存在性、可删除过滤器、高频估算、Top-K 和分位统计。
	bloom := redisClient.BloomFilter("bloom")
	cuckoo := redisClient.CuckooFilter("cuckoo")
	cms := redisClient.CountMinSketch("cms")
	top := redisClient.TopK("top")
	digest := redisClient.TDigest("digest")
	defer func() {
		_ = bloom.Clear(context.Background())
		_ = cuckoo.Clear(context.Background())
		_ = cms.Clear(context.Background())
		_ = top.Clear(context.Background())
		_ = digest.Clear(context.Background())
	}()
	if err := bloom.Reserve(ctx, 0.01, 1000); err != nil {
		return fmt.Errorf("bloom reserve: %w", err)
	}
	if _, err := bloom.Add(ctx, "order-1"); err != nil {
		return fmt.Errorf("bloom add: %w", err)
	}
	possiblyExists, err := bloom.Exists(ctx, "order-1")
	if err != nil {
		return fmt.Errorf("bloom exists: %w", err)
	}
	if err := cuckoo.Reserve(ctx, 1000); err != nil {
		return fmt.Errorf("cuckoo reserve: %w", err)
	}
	if _, err := cuckoo.Add(ctx, "session-1"); err != nil {
		return fmt.Errorf("cuckoo add: %w", err)
	}
	cuckooExists, err := cuckoo.Exists(ctx, "session-1")
	if err != nil {
		return fmt.Errorf("cuckoo exists: %w", err)
	}
	if err := cms.InitByProbability(ctx, 0.01, 0.001); err != nil {
		return fmt.Errorf("cms init: %w", err)
	}
	frequencies, err := cms.Increment(ctx, rueidistpl.CountMinIncrement{Item: "/home", Delta: 3})
	if err != nil {
		return fmt.Errorf("cms increment: %w", err)
	}
	if err := top.Reserve(ctx, 2); err != nil {
		return fmt.Errorf("top-k reserve: %w", err)
	}
	if _, err := top.Add(ctx, "go", "redis", "go"); err != nil {
		return fmt.Errorf("top-k add: %w", err)
	}
	topItems, err := top.List(ctx)
	if err != nil {
		return fmt.Errorf("top-k list: %w", err)
	}
	if err := digest.Create(ctx, 100); err != nil {
		return fmt.Errorf("t-digest create: %w", err)
	}
	if err := digest.Add(ctx, 10, 20, 30); err != nil {
		return fmt.Errorf("t-digest add: %w", err)
	}
	percentiles, err := digest.Quantiles(ctx, 0.5, 0.95)
	if err != nil {
		return fmt.Errorf("t-digest quantiles: %w", err)
	}
	fmt.Printf(
		"Probabilistic: bloom=%t cuckoo=%t cms=%v top=%+v percentiles=%v\n",
		possiblyExists,
		cuckooExists,
		frequencies,
		topItems,
		percentiles,
	)

	return nil
}
