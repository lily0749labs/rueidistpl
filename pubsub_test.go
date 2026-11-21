package rueidistpl

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestPubSubPublishAndSubscribe 验证普通与 Sharded 发布使用相同 JSON 载荷和命名空间，
// 订阅回调收到的是去前缀后的逻辑 channel；handler 返回错误时应立即停止订阅并原样返回错误。
func TestPubSubPublishAndSubscribe(t *testing.T) {
	// Arrange：统一使用 notifications 命名空间，准备可比较对象和 handler 终止错误。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	pubsub := NewPubSub[unitUser](redis, "notifications")
	alice := unitUser{ID: 1, Name: "Alice"}
	payload := `{"id":1,"name":"Alice"}`
	handlerErr := errors.New("handler stopped")
	realPrefix := pubsub.Prefix()
	if realPrefix != "app:notifications" {
		t.Fatalf("Prefix value=%q", realPrefix)
	}

	// Receive mock 主动投递一条消息；handler 返回错误后内部 context 应被取消。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("PUBLISH", "app:notifications:created", payload)).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("SPUBLISH", "app:notifications:created", payload)).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Receive(gomock.Any(), rueidismock.Match("SUBSCRIBE", "app:notifications:created"), gomock.Any()).DoAndReturn(
			func(receiveCtx context.Context, _ rueidis.Completed, callback func(rueidis.PubSubMessage)) error {
				callback(rueidis.PubSubMessage{Channel: "app:notifications:created", Message: payload})
				return receiveCtx.Err()
			},
		),
	)

	// 发布阶段：普通与 Sharded 命令应使用相同的前缀 channel 和 JSON。
	publishReceivers, publishErr := pubsub.Publish(ctx, "created", alice)
	if publishErr != nil || publishReceivers != 2 {
		t.Fatalf("Publish receivers=%d err=%v", publishReceivers, publishErr)
	}
	publishShardReceivers, publishShardErr := pubsub.PublishShard(ctx, "created", alice)
	if publishShardErr != nil || publishShardReceivers != 1 {
		t.Fatalf("PublishShard receivers=%d err=%v", publishShardReceivers, publishShardErr)
	}
	// 订阅阶段：记录回调消息，并用可识别错误主动结束阻塞 Receive。
	var received PubSubMessage[unitUser]
	err := pubsub.Subscribe(ctx, func(message PubSubMessage[unitUser]) error {
		received = message
		return handlerErr
	}, "created")
	if !errors.Is(err, handlerErr) {
		t.Fatalf("Subscribe got %v want %v", err, handlerErr)
	}
	// 回调只应看到逻辑 channel；普通订阅没有 Pattern，载荷必须恢复为原对象。
	if received.Channel != "created" || received.Pattern != "" || received.Value != alice {
		t.Fatalf("unexpected message %+v", received)
	}
}

// TestPubSubPatternShardAndDecodeError 覆盖 PSUBSCRIBE、SSUBSCRIBE 和消息解码失败：
// 底层 Receive 错误不能被吞掉，非法 JSON 应取消本次接收并作为订阅错误返回。
func TestPubSubPatternShardAndDecodeError(t *testing.T) {
	// Arrange：前两次 Receive 返回连接错误，第三次投递非法 JSON。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	pubsub := NewPubSub[unitUser](redis, "notifications")
	receiveErr := errors.New("receive failed")

	gomock.InOrder(
		client.EXPECT().Receive(gomock.Any(), rueidismock.Match("PSUBSCRIBE", "app:notifications:order:*"), gomock.Any()).Return(receiveErr),
		client.EXPECT().Receive(gomock.Any(), rueidismock.Match("SSUBSCRIBE", "app:notifications:created"), gomock.Any()).Return(receiveErr),
		client.EXPECT().Receive(gomock.Any(), rueidismock.Match("SUBSCRIBE", "app:notifications:invalid"), gomock.Any()).DoAndReturn(
			func(receiveCtx context.Context, _ rueidis.Completed, callback func(rueidis.PubSubMessage)) error {
				callback(rueidis.PubSubMessage{Channel: "app:notifications:invalid", Message: "not-json"})
				return receiveCtx.Err()
			},
		),
	)

	// 模式订阅和 Sharded 订阅都必须原样传播 Rueidis Receive 错误。
	subscribePatternErr := pubsub.SubscribePattern(ctx, nil, "order:*")
	if !errors.Is(subscribePatternErr, receiveErr) {
		t.Fatalf("SubscribePattern got %v want %v", subscribePatternErr, receiveErr)
	}
	subscribeShardErr := pubsub.SubscribeShard(ctx, nil, "created")
	if !errors.Is(subscribeShardErr, receiveErr) {
		t.Fatalf("SubscribeShard got %v want %v", subscribeShardErr, receiveErr)
	}
	// 即使 handler 为 nil，封装仍必须先解析 JSON，脏消息不能被静默忽略。
	subscribeErr := pubsub.Subscribe(ctx, nil, "invalid")
	if subscribeErr == nil {
		t.Fatal("Subscribe should report invalid JSON")
	}
}

// TestPubSubValidation 验证发布前完成 JSON 校验，订阅前完成 channel 数量校验，
// 从而保证无效输入不会调用 Rueidis 或形成无效命令。
func TestPubSubValidation(t *testing.T) {
	// 使用无法 JSON 编码的 chan 载荷，且故意不提供客户端，证明校验发生在本地。
	ctx := context.Background()
	pubsub := &PubSub[chan int]{}
	_, publishErr := pubsub.Publish(ctx, "channel", make(chan int))
	if publishErr == nil {
		t.Fatal("Publish should reject values unsupported by JSON")
	}
	_, publishShardErr := pubsub.PublishShard(ctx, "channel", make(chan int))
	if publishShardErr == nil {
		t.Fatal("PublishShard should reject values unsupported by JSON")
	}
	// 两种订阅方式都不允许空 channel/pattern 列表。
	subscribePatternErr := pubsub.SubscribePattern(ctx, nil)
	if !errors.Is(subscribePatternErr, ErrEmptyChannels) {
		t.Fatalf("got %v want %v", subscribePatternErr, ErrEmptyChannels)
	}
	subscribeShardErr := pubsub.SubscribeShard(ctx, nil)
	if !errors.Is(subscribeShardErr, ErrEmptyChannels) {
		t.Fatalf("got %v want %v", subscribeShardErr, ErrEmptyChannels)
	}
}
