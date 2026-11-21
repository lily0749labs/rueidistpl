package rueidistpl

import (
	"context"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestVectorSetLifecycle 验证向量写入、维度、存在、向量/属性读取、两类搜索和删除。
func TestVectorSetLifecycle(t *testing.T) {
	// Arrange：VALUES 使用文本浮点格式，避免调用方处理 FP32 二进制字节序。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	set := redis.VectorSet("products")
	actualKey := set.Key()
	if actualKey != "app:products" {
		t.Fatalf("Key value=%q", actualKey)
	}
	searchArrayResponse := rueidismock.RedisArray(
		rueidismock.RedisBlobString("p1"), rueidismock.RedisBlobString("0.98"),
		rueidismock.RedisBlobString("p2"), rueidismock.RedisBlobString("0.75"),
	)
	searchMapResponse := rueidismock.RedisMap(map[string]rueidis.RedisMessage{
		"p2": rueidismock.RedisBlobString("0.75"),
		"p1": rueidismock.RedisBlobString("0.98"),
	})
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("VADD", "app:products", "VALUES", "3", "1", "0.5", "-1", "p1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("VCARD", "app:products")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("VDIM", "app:products")).Return(rueidismock.Result(rueidismock.RedisInt64(3))),
		client.EXPECT().Do(ctx, rueidismock.Match("VISMEMBER", "app:products", "p1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("VEMB", "app:products", "p1")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisFloat64(1), rueidismock.RedisFloat64(0.5), rueidismock.RedisFloat64(-1)))),
		client.EXPECT().Do(ctx, rueidismock.Match("VSETATTR", "app:products", "p1", `{"category":"book"}`)).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("VGETATTR", "app:products", "p1")).Return(rueidismock.Result(rueidismock.RedisBlobString(`{"category":"book"}`))),
		client.EXPECT().Do(ctx, rueidismock.Match("VSIM", "app:products", "VALUES", "3", "1", "0.5", "-1", "WITHSCORES", "COUNT", "2", "FILTER", `.category == "book"`)).Return(rueidismock.Result(searchMapResponse)),
		client.EXPECT().Do(ctx, rueidismock.Match("VSIM", "app:products", "ELE", "p1", "WITHSCORES", "COUNT", "10")).Return(rueidismock.Result(searchArrayResponse)),
		client.EXPECT().Do(ctx, rueidismock.Match("VSETATTR", "app:products", "p1", "")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("VREM", "app:products", "p1")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:products")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// Add 的布尔值区分新增和更新；计数、维度及成员存在检查均返回 Go 基础类型。
	added, addErr := set.Add(ctx, "p1", []float64{1, 0.5, -1})
	cardinality, cardErr := set.Card(ctx)
	dimension, dimensionErr := set.Dimension(ctx)
	contains, containsErr := set.Contains(ctx, "p1")
	if addErr != nil || !added || cardErr != nil || cardinality != 2 || dimensionErr != nil || dimension != 3 || containsErr != nil || !contains {
		t.Fatalf("Add=%t/%v Card=%d/%v Dimension=%d/%v Contains=%t/%v", added, addErr, cardinality, cardErr, dimension, dimensionErr, contains, containsErr)
	}

	// 向量和 JSON 属性保留为独立变量，使调试断点能直接看到每一个返回数据。
	vector, vectorErr := set.Vector(ctx, "p1")
	attributesSet, attributesSetErr := set.SetAttributes(ctx, "p1", map[string]string{"category": "book"})
	attributes, attributesErr := set.AttributesRaw(ctx, "p1")
	if vectorErr != nil || !slices.Equal(vector, []float64{1, 0.5, -1}) || attributesSetErr != nil || !attributesSet || attributesErr != nil || string(attributes) != `{"category":"book"}` {
		t.Fatalf("Vector=%+v/%v SetAttributes=%t/%v Attributes=%s/%v", vector, vectorErr, attributesSet, attributesSetErr, attributes, attributesErr)
	}

	// 两种搜索都解析为 member/score 结构；非正 Count 自动使用 10，调用方不必重复默认配置。
	byVector, byVectorErr := set.Search(ctx, []float64{1, 0.5, -1}, VectorSearchOptions{Count: 2, Filter: `.category == "book"`})
	byMember, byMemberErr := set.SearchByMember(ctx, "p1", VectorSearchOptions{})
	wantMatches := []VectorMatch{{Member: "p1", Score: 0.98}, {Member: "p2", Score: 0.75}}
	if byVectorErr != nil || !slices.Equal(byVector, wantMatches) || byMemberErr != nil || !slices.Equal(byMember, wantMatches) {
		t.Fatalf("Search=%+v/%v SearchByMember=%+v/%v", byVector, byVectorErr, byMember, byMemberErr)
	}

	// 清空属性、删除单成员和删除整个 key 都明确返回结果或错误。
	attributesCleared, clearAttributesErr := set.ClearAttributes(ctx, "p1")
	removed, removeErr := set.Remove(ctx, "p1")
	clearErr := set.Clear(ctx)
	if clearAttributesErr != nil || !attributesCleared || removeErr != nil || !removed || clearErr != nil {
		t.Fatalf("ClearAttributes=%t/%v Remove=%t/%v Clear=%v", attributesCleared, clearAttributesErr, removed, removeErr, clearErr)
	}
}

// TestVectorSetValidationAndDecodeErrors 验证空向量、非有限数、缺失向量及异常 VSIM 响应。
func TestVectorSetValidationAndDecodeErrors(t *testing.T) {
	// 参数错误必须在访问客户端前返回，因此可以安全地使用没有 client 的门面。
	ctx := context.Background()
	emptySet := (&Redis{}).VectorSet("products")
	emptyAdded, emptyErr := emptySet.Add(ctx, "p1", nil)
	badMatches, badVectorErr := emptySet.Search(ctx, []float64{math.Inf(-1)}, VectorSearchOptions{})
	if emptyAdded || !errors.Is(emptyErr, ErrVectorRequired) || badMatches != nil || !errors.Is(badVectorErr, ErrInvalidVectorValue) {
		t.Fatalf("empty Add=%t/%v bad Search=%+v/%v", emptyAdded, emptyErr, badMatches, badVectorErr)
	}

	// VEMB 空数组转换成统一 Redis Nil；奇数长度 VSIM 和非法分数必须拒绝解码。
	redis, client := newMockRedis(t)
	set := redis.VectorSet("products")
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("VEMB", "app:products", "missing")).Return(rueidismock.Result(rueidismock.RedisArray())),
		client.EXPECT().Do(ctx, rueidismock.Match("VSIM", "app:products", "ELE", "p1", "WITHSCORES", "COUNT", "10")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("p1")))),
		client.EXPECT().Do(ctx, rueidismock.Match("VSIM", "app:products", "ELE", "p1", "WITHSCORES", "COUNT", "10")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisBlobString("p1"), rueidismock.RedisBlobString("bad")))),
	)
	missingVector, missingErr := set.Vector(ctx, "missing")
	oddMatches, oddErr := set.SearchByMember(ctx, "p1", VectorSearchOptions{})
	invalidMatches, invalidErr := set.SearchByMember(ctx, "p1", VectorSearchOptions{})
	if missingVector != nil || !errors.Is(missingErr, rueidis.Nil) {
		t.Fatalf("missing Vector=%+v err=%v", missingVector, missingErr)
	}
	if oddMatches != nil || oddErr == nil || invalidMatches != nil || invalidErr == nil {
		t.Fatalf("odd=%+v/%v invalid=%+v/%v", oddMatches, oddErr, invalidMatches, invalidErr)
	}
}
