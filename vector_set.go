package rueidistpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/redis/rueidis"
)

var (
	// ErrVectorRequired 表示向量至少需要一个维度。
	ErrVectorRequired = errors.New("rueidistpl: vector must not be empty")
	// ErrInvalidVectorValue 表示向量包含 NaN 或无穷大。
	ErrInvalidVectorValue = errors.New("rueidistpl: vector values must be finite")
)

const defaultVectorSearchCount int64 = 10

// VectorSearchOptions 配置向量相似度搜索。
type VectorSearchOptions struct {
	// Count 是最多返回数量，非正时默认返回 10 个。
	Count int64
	// Filter 是针对元素属性的 Redis Vector Set 过滤表达式；为空表示不过滤。
	Filter string
}

// VectorMatch 是一个相似元素及其余弦相似度分数。
type VectorMatch struct {
	Member string
	Score  float64
}

// VectorSet 封装 Redis 8.0+ 原生 Vector Set 的常用操作。
type VectorSet struct {
	redis *Redis
	key   string
}

// VectorSet 创建向量集合并自动应用全局 key 前缀。
func (redis *Redis) VectorSet(key string) *VectorSet {
	return &VectorSet{redis: redis, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (set *VectorSet) Key() string {
	return set.key
}

// Add 新增或更新元素向量；新增返回 true，更新已有元素返回 false。
func (set *VectorSet) Add(ctx context.Context, member string, vector []float64) (bool, error) {
	args, err := vectorArguments(vector)
	if err != nil {
		return false, err
	}
	args = append(args, member)
	command := set.redis.client.B().Arbitrary("VADD").Keys(set.key).Args(args...).Build()
	return set.redis.client.Do(ctx, command).AsBool()
}

// Card 返回集合中的元素数量。
func (set *VectorSet) Card(ctx context.Context) (int64, error) {
	command := set.redis.client.B().Arbitrary("VCARD").Keys(set.key).ReadOnly()
	return set.redis.client.Do(ctx, command).AsInt64()
}

// Dimension 返回向量维度；key 不存在时返回零。
func (set *VectorSet) Dimension(ctx context.Context) (int64, error) {
	command := set.redis.client.B().Arbitrary("VDIM").Keys(set.key).ReadOnly()
	return set.redis.client.Do(ctx, command).AsInt64()
}

// Contains 判断元素是否存在，需要 Redis 8.2+。
func (set *VectorSet) Contains(ctx context.Context, member string) (bool, error) {
	command := set.redis.client.B().Arbitrary("VISMEMBER").Keys(set.key).Args(member).ReadOnly()
	return set.redis.client.Do(ctx, command).AsBool()
}

// Vector 返回元素的近似向量；元素不存在时返回 Redis Nil。
func (set *VectorSet) Vector(ctx context.Context, member string) ([]float64, error) {
	command := set.redis.client.B().Arbitrary("VEMB").Keys(set.key).Args(member).ReadOnly()
	values, err := set.redis.client.Do(ctx, command).AsFloatSlice()
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, rueidis.Nil
	}
	return values, nil
}

// SetAttributes 将对象编码为 JSON 后设置为元素属性；元素存在时返回 true。
func (set *VectorSet) SetAttributes(ctx context.Context, member string, attributes any) (bool, error) {
	payload, err := json.Marshal(attributes)
	if err != nil {
		return false, err
	}
	command := set.redis.client.B().Arbitrary("VSETATTR").Keys(set.key).Args(member, string(payload)).Build()
	return set.redis.client.Do(ctx, command).AsBool()
}

// ClearAttributes 清空元素属性；元素存在时返回 true。
func (set *VectorSet) ClearAttributes(ctx context.Context, member string) (bool, error) {
	command := set.redis.client.B().Arbitrary("VSETATTR").Keys(set.key).Args(member, "").Build()
	return set.redis.client.Do(ctx, command).AsBool()
}

// AttributesRaw 返回元素属性的原始 JSON；元素不存在时返回 Redis Nil。
func (set *VectorSet) AttributesRaw(ctx context.Context, member string) (json.RawMessage, error) {
	command := set.redis.client.B().Arbitrary("VGETATTR").Keys(set.key).Args(member).ReadOnly()
	raw, err := set.redis.client.Do(ctx, command).ToString()
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

// Search 按给定向量返回相似元素，结果按相似度从高到低排列。
func (set *VectorSet) Search(ctx context.Context, vector []float64, options VectorSearchOptions) ([]VectorMatch, error) {
	args, err := vectorArguments(vector)
	if err != nil {
		return nil, err
	}
	return set.search(ctx, args, options)
}

// SearchByMember 使用已有元素的向量搜索相似元素。
func (set *VectorSet) SearchByMember(ctx context.Context, member string, options VectorSearchOptions) ([]VectorMatch, error) {
	return set.search(ctx, []string{"ELE", member}, options)
}

func (set *VectorSet) search(ctx context.Context, query []string, options VectorSearchOptions) ([]VectorMatch, error) {
	count := options.Count
	if count <= 0 {
		count = defaultVectorSearchCount
	}
	args := append([]string(nil), query...)
	args = append(args, "WITHSCORES", "COUNT", strconv.FormatInt(count, 10))
	if options.Filter != "" {
		args = append(args, "FILTER", options.Filter)
	}
	command := set.redis.client.B().Arbitrary("VSIM").Keys(set.key).Args(args...).ReadOnly()
	message, err := set.redis.client.Do(ctx, command).ToMessage()
	if err != nil {
		return nil, err
	}
	return decodeVectorMatches(message)
}

func decodeVectorMatches(message rueidis.RedisMessage) ([]VectorMatch, error) {
	// Redis 8.10 在 RESP3 下返回 member=>score Map；RESP2 和部分兼容实现返回扁平或嵌套 Array。
	if message.IsMap() {
		values, err := message.AsMap()
		if err != nil {
			return nil, err
		}
		matches := make([]VectorMatch, 0, len(values))
		for member, scoreMessage := range values {
			score, scoreErr := scoreMessage.AsFloat64()
			if scoreErr != nil {
				return nil, fmt.Errorf("decode vector match %q score: %w", member, scoreErr)
			}
			matches = append(matches, VectorMatch{Member: member, Score: score})
		}
		// RESP3 Map 不承诺迭代顺序，重新按分数降序排序；同分时按成员名稳定排序。
		sort.Slice(matches, func(left, right int) bool {
			if matches[left].Score == matches[right].Score {
				return matches[left].Member < matches[right].Member
			}
			return matches[left].Score > matches[right].Score
		})
		return matches, nil
	}
	messages, err := message.ToArray()
	if err != nil {
		return nil, err
	}
	if len(messages) > 0 && messages[0].IsArray() {
		matches := make([]VectorMatch, len(messages))
		for index, item := range messages {
			parts, partErr := item.ToArray()
			if partErr != nil || len(parts) != 2 {
				return nil, fmt.Errorf("decode vector match %d: unexpected pair", index)
			}
			match, matchErr := decodeVectorMatch(parts, index)
			if matchErr != nil {
				return nil, matchErr
			}
			matches[index] = match
		}
		return matches, nil
	}
	if len(messages)%2 != 0 {
		return nil, fmt.Errorf("unexpected VSIM response length: %d", len(messages))
	}
	matches := make([]VectorMatch, 0, len(messages)/2)
	for index := 0; index < len(messages); index += 2 {
		match, matchErr := decodeVectorMatch(messages[index:index+2], index/2)
		if matchErr != nil {
			return nil, matchErr
		}
		matches = append(matches, match)
	}
	return matches, nil
}

func decodeVectorMatch(parts []rueidis.RedisMessage, index int) (VectorMatch, error) {
	member, err := parts[0].ToString()
	if err != nil {
		return VectorMatch{}, fmt.Errorf("decode vector match %d member: %w", index, err)
	}
	score, err := parts[1].AsFloat64()
	if err != nil {
		return VectorMatch{}, fmt.Errorf("decode vector match %d score: %w", index, err)
	}
	return VectorMatch{Member: member, Score: score}, nil
}

// Remove 删除元素，实际删除时返回 true。
func (set *VectorSet) Remove(ctx context.Context, member string) (bool, error) {
	command := set.redis.client.B().Arbitrary("VREM").Keys(set.key).Args(member).Build()
	return set.redis.client.Do(ctx, command).AsBool()
}

// Clear 删除整个向量集合。
func (set *VectorSet) Clear(ctx context.Context) error {
	return set.redis.client.Do(ctx, set.redis.client.B().Del().Key(set.key).Build()).Error()
}

func vectorArguments(vector []float64) ([]string, error) {
	if len(vector) == 0 {
		return nil, ErrVectorRequired
	}
	args := make([]string, 0, len(vector)+2)
	args = append(args, "VALUES", strconv.Itoa(len(vector)))
	for _, value := range vector {
		if !isFinite(value) {
			return nil, ErrInvalidVectorValue
		}
		args = append(args, strconv.FormatFloat(value, 'g', -1, 64))
	}
	return args, nil
}
