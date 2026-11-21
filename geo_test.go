package rueidistpl

import (
	"context"
	"errors"
	"slices"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestGeoLifecycle 验证 Geo 的批量写入、空槽坐标、距离、两种圆心搜索以及通用 ZSet 删除维护。
func TestGeoLifecycle(t *testing.T) {
	// Arrange：用上海和北京坐标验证浮点命令参数，并为不存在成员准备 Redis Null。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	geo := redis.Geo("cities")
	shanghai := GeoPoint{Member: "shanghai", Longitude: 121.4737, Latitude: 31.2304}
	beijing := GeoPoint{Member: "beijing", Longitude: 116.4074, Latitude: 39.9042}
	actualKey := geo.Key()
	if actualKey != "app:cities" {
		t.Fatalf("Key value=%q", actualKey)
	}
	shanghaiCoordinates := rueidismock.RedisArray(
		rueidismock.RedisBlobString("121.47370010614395142"),
		rueidismock.RedisBlobString("31.230399851091316"),
	)
	searchShanghai := rueidismock.RedisArray(
		rueidismock.RedisBlobString("shanghai"),
		rueidismock.RedisBlobString("0.0000"),
		shanghaiCoordinates,
	)
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("GEOADD", "app:cities", "121.4737", "31.2304", "shanghai", "116.4074", "39.9042", "beijing")).Return(
			rueidismock.Result(rueidismock.RedisInt64(2)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("GEOPOS", "app:cities", "shanghai", "missing")).Return(
			rueidismock.Result(rueidismock.RedisArray(shanghaiCoordinates, rueidismock.RedisNil())),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("GEOPOS", "app:cities", "shanghai")).Return(
			rueidismock.Result(rueidismock.RedisArray(shanghaiCoordinates)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("GEODIST", "app:cities", "shanghai", "beijing", "km")).Return(
			rueidismock.Result(rueidismock.RedisBlobString("1067.5980")),
		),
		client.EXPECT().Do(ctx, rueidismock.Match(
			"GEOSEARCH", "app:cities", "FROMLONLAT", "121.4737", "31.2304", "BYRADIUS", "10", "km",
			"WITHDIST", "WITHCOORD", "ASC", "COUNT", "5",
		)).Return(rueidismock.Result(rueidismock.RedisArray(searchShanghai))),
		client.EXPECT().Do(ctx, rueidismock.Match(
			"GEOSEARCH", "app:cities", "FROMMEMBER", "shanghai", "BYRADIUS", "20", "km",
			"WITHDIST", "WITHCOORD", "DESC", "COUNT", "2", "ANY",
		)).Return(rueidismock.Result(rueidismock.RedisArray(searchShanghai))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZREM", "app:cities", "beijing")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("ZCARD", "app:cities")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:cities")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// GEOADD 返回真正新增的成员数，更新已有成员不会虚增该数值。
	addedCount, addErr := geo.Add(ctx, shanghai, beijing)
	if addErr != nil || addedCount != 2 {
		t.Fatalf("Add count=%d err=%v", addedCount, addErr)
	}

	// 批量位置保持输入顺序，并通过 Exists=false 明确表示 missing；单成员入口复用相同语义。
	positions, positionsErr := geo.Positions(ctx, "shanghai", "missing")
	if positionsErr != nil || len(positions) != 2 || !positions[0].Exists || positions[0].Member != "shanghai" || positions[1] != (GeoPosition{Member: "missing"}) {
		t.Fatalf("Positions values=%+v err=%v", positions, positionsErr)
	}
	position, positionErr := geo.Position(ctx, "shanghai")
	if positionErr != nil || !position.Exists || position.Longitude == 0 || position.Latitude == 0 {
		t.Fatalf("Position value=%+v err=%v", position, positionErr)
	}

	// 距离使用调用方选择的 km 单位；搜索结果强类型化成员、距离和坐标。
	distance, distanceErr := geo.Distance(ctx, "shanghai", "beijing", GeoKilometers)
	if distanceErr != nil || distance != 1067.598 {
		t.Fatalf("Distance value=%f err=%v", distance, distanceErr)
	}
	coordinateResults, coordinateSearchErr := geo.SearchRadius(ctx, 121.4737, 31.2304, 10, GeoKilometers, GeoSearchOptions{Count: 5})
	memberResults, memberSearchErr := geo.SearchRadiusByMember(ctx, "shanghai", 20, GeoKilometers, GeoSearchOptions{Count: 2, Any: true, Desc: true})
	wantResult := GeoSearchResult{Member: "shanghai", Longitude: positions[0].Longitude, Latitude: positions[0].Latitude}
	if coordinateSearchErr != nil || len(coordinateResults) != 1 || coordinateResults[0] != wantResult {
		t.Fatalf("SearchRadius values=%+v err=%v", coordinateResults, coordinateSearchErr)
	}
	if memberSearchErr != nil || !slices.Equal(memberResults, []GeoSearchResult{wantResult}) {
		t.Fatalf("SearchRadiusByMember values=%+v err=%v", memberResults, memberSearchErr)
	}

	// GEO 底层是 ZSet，删除、计数和清空分别复用稳定的 ZREM、ZCARD、DEL。
	removedCount, removeErr := geo.Remove(ctx, "beijing")
	length, lenErr := geo.Len(ctx)
	clearErr := geo.Clear(ctx)
	if removeErr != nil || removedCount != 1 || lenErr != nil || length != 1 || clearErr != nil {
		t.Fatalf("Remove=%d err=%v; Len=%d err=%v; Clear err=%v", removedCount, removeErr, length, lenErr, clearErr)
	}
}

// TestGeoValidationAndDecodeErrors 验证坐标、单位、半径校验及 GEO 嵌套 RESP 解码错误。
func TestGeoValidationAndDecodeErrors(t *testing.T) {
	// 无客户端 Geo 用于验证全部参数错误都在访问 Redis 前返回。
	ctx := context.Background()
	emptyGeo := (&Redis{}).Geo("cities")
	emptyPositions, emptyPositionsErr := emptyGeo.Positions(ctx)
	emptyRemoved, emptyRemoveErr := emptyGeo.Remove(ctx)
	badAddCount, badAddErr := emptyGeo.Add(ctx, GeoPoint{Member: "bad", Longitude: 181})
	badDistance, badDistanceErr := emptyGeo.Distance(ctx, "a", "b", GeoUnit("yard"))
	badCoordinateResults, badCoordinateErr := emptyGeo.SearchRadius(ctx, 181, 0, 1, GeoMeters, GeoSearchOptions{})
	badRadiusResults, badRadiusErr := emptyGeo.SearchRadiusByMember(ctx, "a", 0, GeoMeters, GeoSearchOptions{})
	badUnitResults, badUnitErr := emptyGeo.SearchRadiusByMember(ctx, "a", 1, GeoUnit("yard"), GeoSearchOptions{})
	if emptyPositionsErr != nil || emptyPositions == nil || len(emptyPositions) != 0 || emptyRemoveErr != nil || emptyRemoved != 0 {
		t.Fatalf("empty Positions=%+v err=%v; Remove=%d err=%v", emptyPositions, emptyPositionsErr, emptyRemoved, emptyRemoveErr)
	}
	if badAddCount != 0 || !errors.Is(badAddErr, ErrInvalidGeoCoordinate) || badDistance != 0 || !errors.Is(badDistanceErr, ErrInvalidGeoUnit) {
		t.Fatalf("bad Add=%d err=%v; Distance=%f err=%v", badAddCount, badAddErr, badDistance, badDistanceErr)
	}
	if badCoordinateResults != nil || !errors.Is(badCoordinateErr, ErrInvalidGeoCoordinate) || badRadiusResults != nil || !errors.Is(badRadiusErr, ErrInvalidGeoRadius) || badUnitResults != nil || !errors.Is(badUnitErr, ErrInvalidGeoUnit) {
		t.Fatalf("coordinate=%+v/%v radius=%+v/%v unit=%+v/%v", badCoordinateResults, badCoordinateErr, badRadiusResults, badRadiusErr, badUnitResults, badUnitErr)
	}

	// 三次 GEOPOS 分别返回长度不匹配、坐标数量异常和非法纬度类型；结果都必须整体丢弃。
	redis, client := newMockRedis(t)
	geo := redis.Geo("cities")
	command := rueidismock.Match("GEOPOS", "app:cities", "a")
	gomock.InOrder(
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray())),
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisArray(rueidismock.RedisBlobString("1")),
		))),
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisArray(rueidismock.RedisBlobString("1"), rueidismock.RedisArray()),
		))),
	)
	lengthPositions, lengthErr := geo.Positions(ctx, "a")
	coordinatePositions, coordinateErr := geo.Positions(ctx, "a")
	typePositions, typeErr := geo.Positions(ctx, "a")
	if lengthPositions != nil || lengthErr == nil || coordinatePositions != nil || coordinateErr == nil || typePositions != nil || typeErr == nil {
		t.Fatalf("length=%+v/%v coordinate=%+v/%v type=%+v/%v", lengthPositions, lengthErr, coordinatePositions, coordinateErr, typePositions, typeErr)
	}

	// 搜索项必须严格包含 member、distance、coordinates 三段，避免错位解析未来协议字段。
	client.EXPECT().Do(ctx, rueidismock.Match(
		"GEOSEARCH", "app:cities", "FROMMEMBER", "a", "BYRADIUS", "1", "m", "WITHDIST", "WITHCOORD", "ASC",
	)).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisArray(
		rueidismock.RedisBlobString("a"), rueidismock.RedisBlobString("0"),
	))))
	searchResults, searchErr := geo.SearchRadiusByMember(ctx, "a", 1, GeoMeters, GeoSearchOptions{})
	if searchResults != nil || searchErr == nil {
		t.Fatalf("Search values=%+v err=%v", searchResults, searchErr)
	}
}
