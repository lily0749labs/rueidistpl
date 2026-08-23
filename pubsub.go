package rueidistpl

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"

	"github.com/redis/rueidis"
)

// ErrEmptyChannels 表示订阅时没有提供任何 channel 或 pattern。
var ErrEmptyChannels = errors.New("rueidistpl: at least one channel is required")

// PubSubMessage 是已经完成 JSON 解码和逻辑 channel 去前缀的消息。
type PubSubMessage[T any] struct {
	// Channel 是去掉命名空间前缀后的逻辑 channel。
	Channel string
	// Pattern 是模式订阅命中的逻辑 pattern；普通订阅时为空。
	Pattern string
	// Value 是消息体反序列化后的业务对象。
	Value T
}

// PubSub 为消息提供 JSON 编解码和统一 channel 前缀。
type PubSub[T any] struct {
	client rueidis.Client
	prefix string
}

// NewPubSub 创建具有统一 channel 命名空间和 JSON 编解码的发布订阅门面。
func NewPubSub[T any](redis *Redis, namespace string) *PubSub[T] {
	return &PubSub[T]{client: redis.client, prefix: redis.Key(namespace)}
}

// Prefix 返回应用全局前缀后的真实 channel 命名空间。
func (pubsub *PubSub[T]) Prefix() string {
	return pubsub.prefix
}

// Publish 发布普通 Pub/Sub 消息，并返回当前接收该消息的订阅者数量。
func (pubsub *PubSub[T]) Publish(ctx context.Context, channel string, value T) (int64, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	command := pubsub.client.B().Publish().Channel(pubsub.channel(channel)).Message(string(payload)).Build()
	return pubsub.client.Do(ctx, command).AsInt64()
}

// PublishShard 发布 Redis Cluster Sharded Pub/Sub 消息。
func (pubsub *PubSub[T]) PublishShard(ctx context.Context, channel string, value T) (int64, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	command := pubsub.client.B().Spublish().Channel(pubsub.channel(channel)).Message(string(payload)).Build()
	return pubsub.client.Do(ctx, command).AsInt64()
}

// Subscribe 阻塞直到 ctx 取消、连接失败或 handler 返回错误。
func (pubsub *PubSub[T]) Subscribe(ctx context.Context, handler func(PubSubMessage[T]) error, channels ...string) error {
	if len(channels) == 0 {
		return ErrEmptyChannels
	}
	resolved := pubsub.channels(channels)
	command := pubsub.client.B().Subscribe().Channel(resolved...).Build()
	return pubsub.receive(ctx, command, handler)
}

// SubscribePattern 按模式订阅，例如传入 "order:*"。
func (pubsub *PubSub[T]) SubscribePattern(ctx context.Context, handler func(PubSubMessage[T]) error, patterns ...string) error {
	if len(patterns) == 0 {
		return ErrEmptyChannels
	}
	resolved := pubsub.channels(patterns)
	command := pubsub.client.B().Psubscribe().Pattern(resolved...).Build()
	return pubsub.receive(ctx, command, handler)
}

// SubscribeShard 使用 Redis Cluster Sharded Pub/Sub。
func (pubsub *PubSub[T]) SubscribeShard(ctx context.Context, handler func(PubSubMessage[T]) error, channels ...string) error {
	if len(channels) == 0 {
		return ErrEmptyChannels
	}
	resolved := pubsub.channels(channels)
	command := pubsub.client.B().Ssubscribe().Channel(resolved...).Build()
	return pubsub.receive(ctx, command, handler)
}

func (pubsub *PubSub[T]) receive(ctx context.Context, command rueidis.Completed, handler func(PubSubMessage[T]) error) error {
	receiveCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		handlerErr error
		once       sync.Once
		mutex      sync.Mutex
	)
	setError := func(err error) {
		once.Do(func() {
			mutex.Lock()
			handlerErr = err
			mutex.Unlock()
			cancel()
		})
	}
	err := pubsub.client.Receive(receiveCtx, command, func(message rueidis.PubSubMessage) {
		value, decodeErr := decodeJSON[T](message.Message)
		if decodeErr != nil {
			setError(decodeErr)
			return
		}
		if handler == nil {
			return
		}
		if callbackErr := handler(PubSubMessage[T]{
			Channel: pubsub.logicalChannel(message.Channel),
			Pattern: pubsub.logicalChannel(message.Pattern),
			Value:   value,
		}); callbackErr != nil {
			setError(callbackErr)
		}
	})
	mutex.Lock()
	callbackErr := handlerErr
	mutex.Unlock()
	if callbackErr != nil {
		return callbackErr
	}
	return err
}

func (pubsub *PubSub[T]) channel(channel string) string {
	return joinKey(pubsub.prefix, channel)
}

func (pubsub *PubSub[T]) channels(channels []string) []string {
	resolved := make([]string, len(channels))
	for index, channel := range channels {
		resolved[index] = pubsub.channel(channel)
	}
	return resolved
}

func (pubsub *PubSub[T]) logicalChannel(channel string) string {
	if pubsub.prefix == "" {
		return channel
	}
	return strings.TrimPrefix(channel, pubsub.prefix+":")
}
