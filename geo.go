package rueidistpl

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/redis/rueidis"
)

var (
	// ErrInvalidGeoCoordinate 表示经纬度超出 Redis GEO 接受范围。
	ErrInvalidGeoCoordinate = errors.New("rueidistpl: invalid geo coordinate")
	// ErrInvalidGeoUnit 表示距离单位不是 m、km、ft 或 mi。
	ErrInvalidGeoUnit = errors.New("rueidistpl: invalid geo unit")
	// ErrInvalidGeoRadius 表示搜索半径不是正数。
	ErrInvalidGeoRadius = errors.New("rueidistpl: geo radius must be positive")
)

// GeoUnit 是 Redis GEO 距离单位。
type GeoUnit string

const (
	GeoMeters     GeoUnit = "m"
	GeoKilometers GeoUnit = "km"
	GeoFeet       GeoUnit = "ft"
	GeoMiles      GeoUnit = "mi"
)

// GeoPoint 是待写入的地理位置。
type GeoPoint struct {
	Member    string
	Longitude float64
	Latitude  float64
}

// GeoPosition 是一个成员的位置；Exists=false 表示成员不存在。
type GeoPosition struct {
	Member    string
	Exists    bool
	Longitude float64
	Latitude  float64
}

// GeoSearchOptions 控制半径搜索的数量和顺序。
type GeoSearchOptions struct {
	// Count 大于 0 时限制结果数。
	Count int64
	// Any 允许 Redis 在找到足够结果后提前停止；只有 Count>0 时有效。
	Any bool
	// Desc 为 true 时按距离从远到近返回，默认从近到远。
	Desc bool
}

// GeoSearchResult 是带距离和坐标的 GEOSEARCH 结果。
type GeoSearchResult struct {
	Member    string
	Distance  float64
	Longitude float64
	Latitude  float64
}

// Geo 是 Redis Geospatial 索引的便利封装。
type Geo struct {
	redis *Redis
	key   string
}

// Geo 创建地理位置集合并自动应用全局 key 前缀。
func (redis *Redis) Geo(key string) *Geo {
	return &Geo{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (geo *Geo) Key() string {
	return geo.key
}

// Add 添加或更新地理位置，并返回新增成员数；空输入返回 0。
func (geo *Geo) Add(ctx context.Context, points ...GeoPoint) (int64, error) {
	if len(points) == 0 {
		return 0, nil
	}
	args := make([]string, 0, len(points)*3)
	for _, point := range points {
		if !validGeoCoordinate(point.Longitude, point.Latitude) {
			return 0, fmt.Errorf("%w: member %q", ErrInvalidGeoCoordinate, point.Member)
		}
		args = append(args,
			strconv.FormatFloat(point.Longitude, 'f', -1, 64),
			strconv.FormatFloat(point.Latitude, 'f', -1, 64),
			point.Member,
		)
	}
	command := geo.redis.client.B().Arbitrary("GEOADD").Keys(geo.key).Args(args...).Build()
	return geo.redis.client.Do(ctx, command).AsInt64()
}

// Positions 按输入顺序返回成员坐标，并通过 Exists 区分不存在成员；空输入返回空切片。
func (geo *Geo) Positions(ctx context.Context, members ...string) ([]GeoPosition, error) {
	if len(members) == 0 {
		return []GeoPosition{}, nil
	}
	command := geo.redis.client.B().Geopos().Key(geo.key).Member(members...).Build()
	messages, err := geo.redis.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	if len(messages) != len(members) {
		return nil, fmt.Errorf("decode geo positions: got %d values for %d members", len(messages), len(members))
	}
	positions := make([]GeoPosition, len(messages))
	for index := range messages {
		position, err := decodeGeoPosition(members[index], &messages[index])
		if err != nil {
			return nil, fmt.Errorf("decode geo member %q: %w", members[index], err)
		}
		positions[index] = position
	}
	return positions, nil
}

// Position 返回单个成员坐标；成员不存在时 Exists=false。
func (geo *Geo) Position(ctx context.Context, member string) (GeoPosition, error) {
	positions, err := geo.Positions(ctx, member)
	if err != nil {
		return GeoPosition{}, err
	}
	return positions[0], nil
}

// Distance 返回两个成员之间按 unit 表示的距离；成员不存在时返回 Rueidis Nil。
func (geo *Geo) Distance(ctx context.Context, member1, member2 string, unit GeoUnit) (float64, error) {
	if !validGeoUnit(unit) {
		return 0, ErrInvalidGeoUnit
	}
	command := geo.redis.client.B().Arbitrary("GEODIST").Keys(geo.key).Args(member1, member2, string(unit)).ReadOnly()
	return geo.redis.client.Do(ctx, command).AsFloat64()
}

// SearchRadius 以经纬度为圆心搜索，并始终返回成员、距离和坐标。
func (geo *Geo) SearchRadius(ctx context.Context, longitude, latitude, radius float64, unit GeoUnit, options GeoSearchOptions) ([]GeoSearchResult, error) {
	if !validGeoCoordinate(longitude, latitude) {
		return nil, ErrInvalidGeoCoordinate
	}
	return geo.searchRadius(ctx, []string{"FROMLONLAT", formatFloat(longitude), formatFloat(latitude)}, radius, unit, options)
}

// SearchRadiusByMember 以已有 member 为圆心搜索，并始终返回成员、距离和坐标。
func (geo *Geo) SearchRadiusByMember(ctx context.Context, member string, radius float64, unit GeoUnit, options GeoSearchOptions) ([]GeoSearchResult, error) {
	return geo.searchRadius(ctx, []string{"FROMMEMBER", member}, radius, unit, options)
}

func (geo *Geo) searchRadius(ctx context.Context, center []string, radius float64, unit GeoUnit, options GeoSearchOptions) ([]GeoSearchResult, error) {
	if radius <= 0 {
		return nil, ErrInvalidGeoRadius
	}
	if !validGeoUnit(unit) {
		return nil, ErrInvalidGeoUnit
	}
	args := append([]string{}, center...)
	args = append(args, "BYRADIUS", formatFloat(radius), string(unit), "WITHDIST", "WITHCOORD")
	if options.Desc {
		args = append(args, "DESC")
	} else {
		args = append(args, "ASC")
	}
	if options.Count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(options.Count, 10))
		if options.Any {
			args = append(args, "ANY")
		}
	}
	command := geo.redis.client.B().Arbitrary("GEOSEARCH").Keys(geo.key).Args(args...).ReadOnly()
	messages, err := geo.redis.client.Do(ctx, command).ToArray()
	if err != nil {
		return nil, err
	}
	results := make([]GeoSearchResult, len(messages))
	for index := range messages {
		result, err := decodeGeoSearchResult(&messages[index])
		if err != nil {
			return nil, fmt.Errorf("decode geo search result %d: %w", index, err)
		}
		results[index] = result
	}
	return results, nil
}

// Remove 删除成员并返回实际删除数；空输入返回 0。
func (geo *Geo) Remove(ctx context.Context, members ...string) (int64, error) {
	if len(members) == 0 {
		return 0, nil
	}
	command := geo.redis.client.B().Zrem().Key(geo.key).Member(members...).Build()
	return geo.redis.client.Do(ctx, command).AsInt64()
}

// Len 返回地理位置成员数。
func (geo *Geo) Len(ctx context.Context) (int64, error) {
	return geo.redis.client.Do(ctx, geo.redis.client.B().Zcard().Key(geo.key).Build()).AsInt64()
}

// Clear 删除整个 GEO key。
func (geo *Geo) Clear(ctx context.Context) error {
	return geo.redis.client.Do(ctx, geo.redis.client.B().Del().Key(geo.key).Build()).Error()
}

func validGeoCoordinate(longitude, latitude float64) bool {
	return longitude >= -180 && longitude <= 180 && latitude >= -85.05112878 && latitude <= 85.05112878
}

func validGeoUnit(unit GeoUnit) bool {
	return unit == GeoMeters || unit == GeoKilometers || unit == GeoFeet || unit == GeoMiles
}

func formatFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func decodeGeoPosition(member string, message *rueidis.RedisMessage) (GeoPosition, error) {
	if message.IsNil() {
		return GeoPosition{Member: member}, nil
	}
	coordinates, err := message.ToArray()
	if err != nil {
		return GeoPosition{}, err
	}
	if len(coordinates) != 2 {
		return GeoPosition{}, fmt.Errorf("expected 2 coordinates, got %d", len(coordinates))
	}
	longitude, err := coordinates[0].AsFloat64()
	if err != nil {
		return GeoPosition{}, err
	}
	latitude, err := coordinates[1].AsFloat64()
	if err != nil {
		return GeoPosition{}, err
	}
	return GeoPosition{Member: member, Exists: true, Longitude: longitude, Latitude: latitude}, nil
}

func decodeGeoSearchResult(message *rueidis.RedisMessage) (GeoSearchResult, error) {
	values, err := message.ToArray()
	if err != nil {
		return GeoSearchResult{}, err
	}
	if len(values) != 3 {
		return GeoSearchResult{}, fmt.Errorf("expected member, distance and coordinates, got %d elements", len(values))
	}
	member, err := values[0].ToString()
	if err != nil {
		return GeoSearchResult{}, err
	}
	distance, err := values[1].AsFloat64()
	if err != nil {
		return GeoSearchResult{}, err
	}
	position, err := decodeGeoPosition(member, &values[2])
	if err != nil {
		return GeoSearchResult{}, err
	}
	return GeoSearchResult{Member: member, Distance: distance, Longitude: position.Longitude, Latitude: position.Latitude}, nil
}
