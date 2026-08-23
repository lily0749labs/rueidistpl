package rueidistpl

import (
	"context"
	"encoding/json"

	"github.com/redis/rueidis"
)

const jsonRootPath = "$"

// JSONDocument 使用 RedisJSON 保存一个可按 JSONPath 局部读写的强类型文档。
// Redis 8 已内置 RedisJSON；旧版 Redis 需要单独安装 RedisJSON 模块。
type JSONDocument[T any] struct {
	client rueidis.Client
	key    string
}

// NewJSONDocument 创建强类型 JSON 文档并自动应用全局 key 前缀。
func NewJSONDocument[T any](redis *Redis, key string) *JSONDocument[T] {
	return &JSONDocument[T]{client: redis.client, key: redis.Key(key)}
}

// Key 返回应用全局前缀后的真实 Redis key。
func (document *JSONDocument[T]) Key() string {
	return document.key
}

// Set 新增或覆盖根文档。
func (document *JSONDocument[T]) Set(ctx context.Context, value T) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	command := document.client.B().JsonSet().Key(document.key).Path(jsonRootPath).Value(string(payload)).Build()
	return document.client.Do(ctx, command).Error()
}

// SetNX 仅在文档不存在时写入根文档，写入成功返回 true。
func (document *JSONDocument[T]) SetNX(ctx context.Context, value T) (bool, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	command := document.client.B().JsonSet().Key(document.key).Path(jsonRootPath).Value(string(payload)).Nx().Build()
	return jsonSetApplied(document.client.Do(ctx, command).Error())
}

// SetXX 仅在文档已存在时覆盖根文档，写入成功返回 true。
func (document *JSONDocument[T]) SetXX(ctx context.Context, value T) (bool, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	command := document.client.B().JsonSet().Key(document.key).Path(jsonRootPath).Value(string(payload)).Xx().Build()
	return jsonSetApplied(document.client.Do(ctx, command).Error())
}

// Get 读取并反序列化根文档；文档不存在时返回 T 的零值和 Redis Nil。
func (document *JSONDocument[T]) Get(ctx context.Context) (T, error) {
	raw, err := document.client.Do(ctx, document.client.B().JsonGet().Key(document.key).Build()).ToString()
	if err != nil {
		var zero T
		return zero, err
	}
	return decodeJSON[T](raw)
}

// SetPath 将 value 编码成 JSON 后写入指定 JSONPath。
func (document *JSONDocument[T]) SetPath(ctx context.Context, path string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	command := document.client.B().JsonSet().Key(document.key).Path(path).Value(string(payload)).Build()
	return document.client.Do(ctx, command).Error()
}

// GetPath 返回指定 JSONPath 的原始 JSON。
// 使用以 $ 开头的 JSONPath 时，RedisJSON 会返回匹配结果数组，调用方可按需要再解码。
func (document *JSONDocument[T]) GetPath(ctx context.Context, path string) (json.RawMessage, error) {
	command := document.client.B().JsonGet().Key(document.key).Path(path).Build()
	raw, err := document.client.Do(ctx, command).ToString()
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

// MergePath 按 RFC 7396 JSON Merge Patch 语义合并指定路径。
func (document *JSONDocument[T]) MergePath(ctx context.Context, path string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	command := document.client.B().JsonMerge().Key(document.key).Path(path).Value(string(payload)).Build()
	return document.client.Do(ctx, command).Error()
}

// DeletePath 删除路径匹配的值，并返回实际删除数量。
func (document *JSONDocument[T]) DeletePath(ctx context.Context, path string) (int64, error) {
	command := document.client.B().JsonDel().Key(document.key).Path(path).Build()
	return document.client.Do(ctx, command).AsInt64()
}

// Exists 判断整个 JSON 文档是否存在。
func (document *JSONDocument[T]) Exists(ctx context.Context) (bool, error) {
	command := document.client.B().Exists().Key(document.key).Build()
	return document.client.Do(ctx, command).AsBool()
}

// Clear 删除整个 JSON 文档。
func (document *JSONDocument[T]) Clear(ctx context.Context) error {
	return document.client.Do(ctx, document.client.B().Del().Key(document.key).Build()).Error()
}

func jsonSetApplied(err error) (bool, error) {
	if rueidis.IsRedisNil(err) {
		return false, nil
	}
	return err == nil, err
}
