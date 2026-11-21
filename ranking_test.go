package rueidistpl

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestRankingLifecycle 验证对象排行榜的双结构一致性：
// Upsert/Remove 必须通过 Lua 同时操作 Hash 与 ZSet；Get/Top 也通过单次 Lua 读取一致快照；
// Len 与 Clear 使用分数 key 计数并同时清理两个内部 key。
func TestRankingLifecycle(t *testing.T) {
	// Arrange：获取内部 Hash/ZSet key，并准备两个成员的稳定对象与分数。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	ranking := NewRanking[unitUser](redis, "ranking")
	valuesKey, scoresKey := ranking.Keys()
	alice := unitUser{ID: 1, Name: "Alice"}
	bob := unitUser{ID: 2, Name: "Bob"}
	aliceJSON := `{"id":1,"name":"Alice"}`
	bobJSON := `{"id":2,"name":"Bob"}`

	// Lua 只忽略 SHA，严格核对双 key 和业务参数；普通命令则完整匹配 Builder 输出。
	gomock.InOrder(
		client.EXPECT().Do(ctx, matchLua([]string{valuesKey, scoresKey}, []string{"1", aliceJSON, "100"})).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, retryable(matchLua([]string{valuesKey, scoresKey}, []string{"1"}))).Return(rueidismock.Result(redisStrings(aliceJSON, "100"))),
		client.EXPECT().Do(ctx, retryable(matchLua([]string{valuesKey, scoresKey}, []string{"0", "1"}))).Return(rueidismock.Result(redisStrings("1", "100", aliceJSON, "2", "80", bobJSON))),
		// Remove 的返回值会因重复执行从 1 变成 0，因此必须禁止网络层自动重试。
		client.EXPECT().Do(ctx, nonRetryable(matchLua([]string{valuesKey, scoresKey}, []string{"1"}))).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZCARD", scoresKey)).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", valuesKey, scoresKey)).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
	)

	// 写入与单项读取：对象和分数必须组合成同一个 Ranked 结果。
	upsertErr := ranking.Upsert(ctx, "1", 100, alice)
	if upsertErr != nil {
		t.Fatal(upsertErr)
	}
	getItem, getErr := ranking.Get(ctx, "1")
	if getErr != nil || getItem != (Ranked[unitUser]{ID: "1", Score: 100, Value: alice}) {
		t.Fatalf("Get item=%+v err=%v", getItem, getErr)
	}
	// Top 先从 ZSet 取得稳定 ID/分数，再用 HMGET 批量补齐对象且保持排名顺序。
	items, err := ranking.Top(ctx, 2)
	if err != nil || len(items) != 2 || items[0].Value != alice || items[1].Value != bob {
		t.Fatalf("Top items=%+v err=%v", items, err)
	}
	// 删除和清理：Remove 原子清两种结构，Clear 一次删除两个同 slot key。
	removed, removeErr := ranking.Remove(ctx, "1")
	if removeErr != nil || !removed {
		t.Fatalf("Remove value=%t err=%v", removed, removeErr)
	}
	rankingLength, lenErr := ranking.Len(ctx)
	if lenErr != nil || rankingLength != 2 {
		t.Fatalf("Len value=%d err=%v", rankingLength, lenErr)
	}
	clearErr := ranking.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestRankingEmptyAndMissingValue 验证 limit 非正时不访问 Redis，
// 以及原子快照发现 ZSet 有成员但 Hash 缺少对象时返回 ErrRankingValueMissing。
func TestRankingEmptyAndMissingValue(t *testing.T) {
	// limit=0 使用无客户端实例，证明 Top 会直接返回空切片。
	ranking := &Ranking[unitUser]{}
	topItems, topErr := ranking.Top(context.Background(), 0)
	if topErr != nil || len(topItems) != 0 {
		t.Fatalf("Top items=%+v err=%v", topItems, topErr)
	}

	// 再模拟 ZSet 有 ID、Hash 无对象的数据损坏状态。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	ranking = NewRanking[unitUser](redis, "ranking")
	valuesKey, scoresKey := ranking.Keys()
	client.EXPECT().Do(ctx, retryable(matchLua([]string{valuesKey, scoresKey}, []string{"0", "0"}))).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisBlobString("missing"),
		rueidismock.RedisBlobString("10"),
		rueidismock.RedisNil(),
	)))
	// Range 必须返回专用一致性错误，不能丢弃该成员或返回对象零值。
	_, rangeErr := ranking.Range(ctx, 0, 0)
	if !errors.Is(rangeErr, ErrRankingValueMissing) {
		t.Fatalf("got %v want %v", rangeErr, ErrRankingValueMissing)
	}
}

// TestRankingGetConsistencyErrors 验证原子 Get 能区分“整体不存在”和双结构单边缺失，
// 防止调用方把数据损坏误认为普通未命中，也确认非法 JSON 不会泄露部分对象。
func TestRankingGetConsistencyErrors(t *testing.T) {
	// Arrange：依次模拟两个键都不存在、仅分数缺失，以及对象 JSON 部分解码失败。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	ranking := NewRanking[unitUser](redis, "ranking")
	valuesKey, scoresKey := ranking.Keys()
	gomock.InOrder(
		client.EXPECT().Do(ctx, retryable(matchLua([]string{valuesKey, scoresKey}, []string{"absent"}))).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisNil(), rueidismock.RedisNil(),
		))),
		client.EXPECT().Do(ctx, retryable(matchLua([]string{valuesKey, scoresKey}, []string{"score-missing"}))).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString(`{"id":1,"name":"Alice"}`), rueidismock.RedisNil(),
		))),
		client.EXPECT().Do(ctx, retryable(matchLua([]string{valuesKey, scoresKey}, []string{"invalid"}))).Return(rueidismock.Result(redisStrings(`{"id":99,"name":[]}`, "10"))),
	)

	// Act/Assert：整体不存在继续使用 Rueidis Nil，便于复用官方未命中判断。
	absent, absentErr := ranking.Get(ctx, "absent")
	if absent != (Ranked[unitUser]{}) || !rueidis.IsRedisNil(absentErr) {
		t.Fatalf("absent item=%+v err=%v", absent, absentErr)
	}
	// Hash 有对象但 ZSet 无分数属于结构损坏，必须返回专用错误。
	missingScore, missingScoreErr := ranking.Get(ctx, "score-missing")
	if missingScore != (Ranked[unitUser]{}) || !errors.Is(missingScoreErr, ErrRankingScoreMissing) {
		t.Fatalf("missing score item=%+v err=%v", missingScore, missingScoreErr)
	}
	// JSON 已写入 id 后才在 name 失败时，返回值仍必须是完整零值。
	invalid, invalidErr := ranking.Get(ctx, "invalid")
	if invalid != (Ranked[unitUser]{}) || invalidErr == nil {
		t.Fatalf("invalid item=%+v err=%v", invalid, invalidErr)
	}
}

// TestRankingMalformedLuaResponses 验证排行榜对异常 Lua 响应保持防御性解析，
// 包括字段数量、字段类型、分数字符串和 JSON 载荷，任何异常都不能返回半成品条目。
func TestRankingMalformedLuaResponses(t *testing.T) {
	// Arrange：为 Get 和 Range 的每个关键解析阶段分别构造一条畸形响应。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	ranking := NewRanking[unitUser](redis, "ranking")
	valuesKey, scoresKey := ranking.Keys()
	keys := []string{valuesKey, scoresKey}
	validJSON := `{"id":1,"name":"Alice"}`
	gomock.InOrder(
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"short"}))).Return(rueidismock.Result(redisStrings(validJSON))),
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"bad-payload"}))).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1), rueidismock.RedisBlobString("10")))),
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"bad-score"}))).Return(rueidismock.Result(redisStrings(validJSON, "not-a-score"))),
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"value-missing"}))).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisNil(), rueidismock.RedisBlobString("10")))),
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"0", "0"}))).Return(rueidismock.Result(redisStrings("only-id"))),
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"1", "1"}))).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1), rueidismock.RedisBlobString("10"), rueidismock.RedisBlobString(validJSON)))),
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"2", "2"}))).Return(rueidismock.Result(redisStrings("1", "not-a-score", validJSON))),
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"3", "3"}))).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("1"), rueidismock.RedisBlobString("10"), rueidismock.RedisInt64(1)))),
		client.EXPECT().Do(ctx, retryable(matchLua(keys, []string{"4", "4"}))).Return(rueidismock.Result(redisStrings("1", "10", `{"id":99,"name":[]}`))),
	)

	// Act/Assert：Get 的长度、载荷类型、分数及单边缺失都应返回零值和明确错误。
	for _, id := range []string{"short", "bad-payload", "bad-score", "value-missing"} {
		item, err := ranking.Get(ctx, id)
		if item != (Ranked[unitUser]{}) || err == nil {
			t.Fatalf("Get(%q) item=%+v err=%v", id, item, err)
		}
	}
	// Range 的三元组长度及每个字段解析失败时，都必须丢弃已构造的切片。
	for index := int64(0); index <= 4; index++ {
		items, err := ranking.Range(ctx, index, index)
		if items != nil || err == nil {
			t.Fatalf("Range(%d) items=%+v err=%v", index, items, err)
		}
	}
}

// TestRankingRejectsUnsupportedJSON 验证对象无法序列化时不会执行更新 Lua，
// 防止只更新分数而没有有效对象载荷。
func TestRankingRejectsUnsupportedJSON(t *testing.T) {
	// chan 无法 JSON 序列化；未配置客户端可确保失败发生在执行 Lua 之前。
	ranking := &Ranking[chan int]{}
	upsertErr := ranking.Upsert(context.Background(), "bad", 1, make(chan int))
	if upsertErr == nil {
		t.Fatal("Upsert should reject values unsupported by JSON")
	}
}

// TestRankingRejectsNaNScore 验证排行榜在执行双结构 Lua 前拒绝 NaN，
// 否则 HSET 可能已经成功，而后续 ZADD 才报错，破坏 Hash 与 ZSet 的原子一致性。
func TestRankingRejectsNaNScore(t *testing.T) {
	// Arrange：不配置 Redis 客户端，确保参数错误必须在 JSON 编码和 Lua 执行之前返回。
	ranking := &Ranking[unitUser]{}

	// Act/Assert：返回统一 ErrInvalidScore，且不会留下只有对象、没有分数的半写入状态。
	err := ranking.Upsert(context.Background(), "bad", math.NaN(), unitUser{ID: 1, Name: "Alice"})
	if !errors.Is(err, ErrInvalidScore) {
		t.Fatalf("Upsert error=%v", err)
	}
}
