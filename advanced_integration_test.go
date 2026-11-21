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

// TestRedisAdvancedStructuresIntegration 使用真实 Redis 8.10.0 验证 P0/P1/P2 新增便利结构。
// 测试默认跳过，只有显式设置 RUEIDISTPL_ADVANCED_INTEGRATION=1 时才会运行。
func TestRedisAdvancedStructuresIntegration(t *testing.T) {
	// 这些用例包含 Redis 8 内置的 JSON、TimeSeries、概率结构和 Vector Set，不能放进旧版本矩阵。
	if os.Getenv("RUEIDISTPL_ADVANCED_INTEGRATION") != "1" {
		t.Skip("set RUEIDISTPL_ADVANCED_INTEGRATION=1 to run Redis 8.10 advanced structures integration test")
	}

	// 每次执行使用唯一前缀，既避免并发 CI 冲突，也让失败残留不会污染下次结果。
	prefix := fmt.Sprintf("rueidistpl:advanced-test:%d", time.Now().UnixNano())
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{envOr("REDIS_ADDR", "127.0.0.1:16479")},
			Password:    envOr("REDIS_PASSWORD", "redis1234"),
		},
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pingErr := redisClient.Ping(ctx)
	if pingErr != nil {
		t.Fatal(pingErr)
	}

	// 先构造所有结构，统一 defer 清理，确保任何中间断言失败都尽量释放测试 key。
	bitmap := redisClient.Bitmap("bitmap")
	geo := redisClient.Geo("geo")
	hll := redisClient.HyperLogLog("hll")
	document := rueidistpl.NewJSONDocument[integrationUser](redisClient, "json")
	series := redisClient.TimeSeries("series")
	vectors := redisClient.VectorSet("vectors")
	bloom := redisClient.BloomFilter("bloom")
	cuckoo := redisClient.CuckooFilter("cuckoo")
	cms := redisClient.CountMinSketch("cms")
	top := redisClient.TopK("top")
	digest := redisClient.TDigest("digest")
	defer func() {
		_ = bitmap.Clear(ctx)
		_ = geo.Clear(ctx)
		_ = hll.Clear(ctx)
		_ = document.Clear(ctx)
		_ = series.Clear(ctx)
		_ = vectors.Clear(ctx)
		_ = bloom.Clear(ctx)
		_ = cuckoo.Clear(ctx)
		_ = cms.Clear(ctx)
		_ = top.Clear(ctx)
		_ = digest.Clear(ctx)
	}()

	// P0：Bitmap 验证位语义，Geo 验证真实坐标协议，HyperLogLog 验证近似去重计数。
	previousBit, bitmapSetErr := bitmap.Set(ctx, 9, true)
	currentBit, bitmapGetErr := bitmap.Get(ctx, 9)
	bitCount, bitmapCountErr := bitmap.Count(ctx)
	if bitmapSetErr != nil || previousBit || bitmapGetErr != nil || !currentBit || bitmapCountErr != nil || bitCount != 1 {
		t.Fatalf("bitmap Set=%t/%v Get=%t/%v Count=%d/%v", previousBit, bitmapSetErr, currentBit, bitmapGetErr, bitCount, bitmapCountErr)
	}
	geoAdded, geoAddErr := geo.Add(ctx,
		rueidistpl.GeoPoint{Member: "shanghai", Longitude: 121.4737, Latitude: 31.2304},
		rueidistpl.GeoPoint{Member: "beijing", Longitude: 116.4074, Latitude: 39.9042},
	)
	geoResults, geoSearchErr := geo.SearchRadiusByMember(ctx, "shanghai", 1200, rueidistpl.GeoKilometers, rueidistpl.GeoSearchOptions{Count: 2})
	if geoAddErr != nil || geoAdded != 2 || geoSearchErr != nil || len(geoResults) != 2 || geoResults[0].Member != "shanghai" {
		t.Fatalf("geo Add=%d/%v Search=%+v/%v", geoAdded, geoAddErr, geoResults, geoSearchErr)
	}
	hllChanged, hllAddErr := hll.Add(ctx, "u1", "u2", "u1")
	hllCount, hllCountErr := hll.Count(ctx)
	if hllAddErr != nil || !hllChanged || hllCountErr != nil || hllCount != 2 {
		t.Fatalf("hll Add=%t/%v Count=%d/%v", hllChanged, hllAddErr, hllCount, hllCountErr)
	}

	// P1 JSON：根文档保持强类型，JSONPath 局部更新不需要调用方手写 JSON 字符串。
	alice := integrationUser{ID: 1, Name: "Alice"}
	documentSetErr := document.Set(ctx, alice)
	documentPathErr := document.SetPath(ctx, "$.name", "Alice v2")
	documentValue, documentGetErr := document.Get(ctx)
	if documentSetErr != nil || documentPathErr != nil || documentGetErr != nil || documentValue != (integrationUser{ID: 1, Name: "Alice v2"}) {
		t.Fatalf("json Set=%v SetPath=%v Get=%+v/%v", documentSetErr, documentPathErr, documentValue, documentGetErr)
	}

	// P1 TimeSeries：显式配置标签后写两个样本，最新值和区间结果都解析成 TimeSeriesSample。
	seriesCreateErr := series.Create(ctx, rueidistpl.TimeSeriesOptions{Retention: time.Minute, Labels: map[string]string{"sensor": "s1"}})
	firstTimestamp, firstAddErr := series.Add(ctx, 1000, 21.5)
	secondTimestamp, secondAddErr := series.Add(ctx, 2000, 22.5)
	latestSample, latestErr := series.Get(ctx)
	seriesSamples, seriesRangeErr := series.Range(ctx, 1000, 2000, 10)
	wantSamples := []rueidistpl.TimeSeriesSample{{Timestamp: 1000, Value: 21.5}, {Timestamp: 2000, Value: 22.5}}
	if seriesCreateErr != nil || firstAddErr != nil || firstTimestamp != 1000 || secondAddErr != nil || secondTimestamp != 2000 {
		t.Fatalf("series Create=%v Add1=%d/%v Add2=%d/%v", seriesCreateErr, firstTimestamp, firstAddErr, secondTimestamp, secondAddErr)
	}
	if latestErr != nil || latestSample != wantSamples[1] || seriesRangeErr != nil || !slices.Equal(seriesSamples, wantSamples) {
		t.Fatalf("series Get=%+v/%v Range=%+v/%v", latestSample, latestErr, seriesSamples, seriesRangeErr)
	}

	// P1 Vector Set：VALUES 文本向量、属性和 WITHSCORES 响应全部走真实 Redis 8.10 协议。
	vectorAdded1, vectorAddErr1 := vectors.Add(ctx, "p1", []float64{1, 0, 0})
	vectorAdded2, vectorAddErr2 := vectors.Add(ctx, "p2", []float64{0.9, 0.1, 0})
	attributesSet, attributesErr := vectors.SetAttributes(ctx, "p1", map[string]string{"category": "book"})
	vectorMatches, vectorSearchErr := vectors.Search(ctx, []float64{1, 0, 0}, rueidistpl.VectorSearchOptions{Count: 2})
	if vectorAddErr1 != nil || !vectorAdded1 || vectorAddErr2 != nil || !vectorAdded2 || attributesErr != nil || !attributesSet {
		t.Fatalf("vectors Add1=%t/%v Add2=%t/%v Attributes=%t/%v", vectorAdded1, vectorAddErr1, vectorAdded2, vectorAddErr2, attributesSet, attributesErr)
	}
	if vectorSearchErr != nil || len(vectorMatches) != 2 || vectorMatches[0].Member != "p1" || vectorMatches[0].Score < vectorMatches[1].Score {
		t.Fatalf("vectors Search=%+v/%v", vectorMatches, vectorSearchErr)
	}

	// P2 Bloom/Cuckoo：分别验证 Bloom 的批量存在性和 Cuckoo 的可删除语义。
	bloomReserveErr := bloom.Reserve(ctx, 0.01, 100)
	bloomAdded, bloomAddErr := bloom.AddMany(ctx, "a", "b")
	bloomExists, bloomExistsErr := bloom.ExistsMany(ctx, "a", "missing")
	if bloomReserveErr != nil || bloomAddErr != nil || !slices.Equal(bloomAdded, []bool{true, true}) || bloomExistsErr != nil || !slices.Equal(bloomExists, []bool{true, false}) {
		t.Fatalf("bloom Reserve=%v Add=%+v/%v Exists=%+v/%v", bloomReserveErr, bloomAdded, bloomAddErr, bloomExists, bloomExistsErr)
	}
	cuckooReserveErr := cuckoo.Reserve(ctx, 100)
	cuckooAdded, cuckooAddErr := cuckoo.Add(ctx, "a")
	cuckooDeleted, cuckooDeleteErr := cuckoo.Delete(ctx, "a")
	cuckooExists, cuckooExistsErr := cuckoo.Exists(ctx, "a")
	if cuckooReserveErr != nil || cuckooAddErr != nil || !cuckooAdded || cuckooDeleteErr != nil || !cuckooDeleted || cuckooExistsErr != nil || cuckooExists {
		t.Fatalf("cuckoo Reserve=%v Add=%t/%v Delete=%t/%v Exists=%t/%v", cuckooReserveErr, cuckooAdded, cuckooAddErr, cuckooDeleted, cuckooDeleteErr, cuckooExists, cuckooExistsErr)
	}

	// P2 CMS/TopK：高频计数和榜单都保持批量输入顺序，TopK List 额外返回估算计数。
	cmsInitErr := cms.InitByProbability(ctx, 0.01, 0.001)
	cmsCounts, cmsIncrementErr := cms.Increment(ctx,
		rueidistpl.CountMinIncrement{Item: "home", Delta: 3},
		rueidistpl.CountMinIncrement{Item: "docs", Delta: 2},
	)
	cmsQuery, cmsQueryErr := cms.Query(ctx, "home", "docs")
	if cmsInitErr != nil || cmsIncrementErr != nil || !slices.Equal(cmsCounts, []int64{3, 2}) || cmsQueryErr != nil || !slices.Equal(cmsQuery, []int64{3, 2}) {
		t.Fatalf("cms Init=%v Increment=%+v/%v Query=%+v/%v", cmsInitErr, cmsCounts, cmsIncrementErr, cmsQuery, cmsQueryErr)
	}
	topReserveErr := top.Reserve(ctx, 2)
	topAddResults, topAddErr := top.Add(ctx, "a", "a", "b", "c")
	topItems, topListErr := top.List(ctx)
	if topReserveErr != nil || topAddErr != nil || len(topAddResults) != 4 || topListErr != nil || len(topItems) != 2 {
		t.Fatalf("top Reserve=%v Add=%+v/%v List=%+v/%v", topReserveErr, topAddResults, topAddErr, topItems, topListErr)
	}

	// P2 t-digest：真实模块应能从三个样本计算中位数、最小值和最大值。
	digestCreateErr := digest.Create(ctx, 100)
	digestAddErr := digest.Add(ctx, 10, 20, 30)
	medianValues, medianErr := digest.Quantiles(ctx, 0.5)
	minimum, minErr := digest.Min(ctx)
	maximum, maxErr := digest.Max(ctx)
	if digestCreateErr != nil || digestAddErr != nil || medianErr != nil || len(medianValues) != 1 || medianValues[0] != 20 {
		t.Fatalf("digest Create=%v Add=%v Quantile=%+v/%v", digestCreateErr, digestAddErr, medianValues, medianErr)
	}
	if minErr != nil || minimum != 10 || maxErr != nil || maximum != 30 {
		t.Fatalf("digest Min=%f/%v Max=%f/%v", minimum, minErr, maximum, maxErr)
	}
}
