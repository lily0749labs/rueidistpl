package rueidistpl

import (
	"context"
	"errors"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestBitmapLifecycle 验证 Bitmap 的位读写、范围统计、位查找、跨 key 运算和 BitField 单值读取。
func TestBitmapLifecycle(t *testing.T) {
	// Arrange：所有逻辑 key 都必须先应用 app 前缀，BITOP 的目标和来源顺序也必须稳定。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	bitmap := redis.Bitmap("active")
	actualKey := bitmap.Key()
	if actualKey != "app:active" {
		t.Fatalf("Key value=%q", actualKey)
	}
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("SETBIT", "app:active", "7", "1")).Return(rueidismock.Result(rueidismock.RedisInt64(0))),
		client.EXPECT().Do(ctx, rueidismock.Match("GETBIT", "app:active", "7")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
		client.EXPECT().Do(ctx, rueidismock.Match("BITCOUNT", "app:active")).Return(rueidismock.Result(rueidismock.RedisInt64(3))),
		client.EXPECT().Do(ctx, rueidismock.Match("BITCOUNT", "app:active", "2", "9", "BIT")).Return(rueidismock.Result(rueidismock.RedisInt64(2))),
		client.EXPECT().Do(ctx, rueidismock.Match("BITPOS", "app:active", "1")).Return(rueidismock.Result(rueidismock.RedisInt64(7))),
		client.EXPECT().Do(ctx, rueidismock.Match("BITPOS", "app:active", "0", "2", "9", "BIT")).Return(rueidismock.Result(rueidismock.RedisInt64(8))),
		client.EXPECT().Do(ctx, rueidismock.Match("BITOP", "OR", "app:combined", "app:active", "app:staff")).Return(rueidismock.Result(rueidismock.RedisInt64(4))),
		client.EXPECT().Do(ctx, rueidismock.Match("BITFIELD_RO", "app:active", "GET", "u8", "0")).Return(
			rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(129))),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:active")).Return(rueidismock.Result(rueidismock.RedisInt64(1))),
	)

	// 位写入返回修改前的值，随后 GETBIT 必须能看到新值。
	previousBit, setErr := bitmap.Set(ctx, 7, true)
	currentBit, getErr := bitmap.Get(ctx, 7)
	if setErr != nil || previousBit || getErr != nil || !currentBit {
		t.Fatalf("Set previous=%t err=%v; Get value=%t err=%v", previousBit, setErr, currentBit, getErr)
	}

	// 全量和位区间统计分别验证无范围及 BIT 单位命令，避免错误使用默认 BYTE 单位。
	allCount, allCountErr := bitmap.Count(ctx)
	rangeCount, rangeCountErr := bitmap.CountBits(ctx, 2, 9)
	if allCountErr != nil || allCount != 3 || rangeCountErr != nil || rangeCount != 2 {
		t.Fatalf("Count=%d err=%v; CountBits=%d err=%v", allCount, allCountErr, rangeCount, rangeCountErr)
	}

	// Position 返回绝对位下标；范围查询同样显式使用 BIT 单位。
	firstSetBit, firstSetErr := bitmap.Position(ctx, true)
	firstClearBit, firstClearErr := bitmap.PositionBits(ctx, false, 2, 9)
	if firstSetErr != nil || firstSetBit != 7 || firstClearErr != nil || firstClearBit != 8 {
		t.Fatalf("Position=%d err=%v; PositionBits=%d err=%v", firstSetBit, firstSetErr, firstClearBit, firstClearErr)
	}

	// OR 运算以当前 bitmap 为第一来源，额外来源与目标 key 都自动添加前缀。
	combinedBytes, combineErr := bitmap.Combine(ctx, BitmapOr, "combined", "staff")
	if combineErr != nil || combinedBytes != 4 {
		t.Fatalf("Combine bytes=%d err=%v", combinedBytes, combineErr)
	}
	fieldValue, fieldErr := bitmap.BitFieldGet(ctx, "u8", 0)
	if fieldErr != nil || fieldValue != 129 {
		t.Fatalf("BitFieldGet value=%d err=%v", fieldValue, fieldErr)
	}
	clearErr := bitmap.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestBitmapValidationAndResponseErrors 验证负偏移、非法 BITOP 和异常 BITFIELD 响应都会在安全位置失败。
func TestBitmapValidationAndResponseErrors(t *testing.T) {
	// 负偏移与非法操作不需要客户端，用空门面证明参数校验发生在构造命令之前。
	ctx := context.Background()
	emptyBitmap := (&Redis{}).Bitmap("bits")
	setValue, setErr := emptyBitmap.Set(ctx, -1, true)
	getValue, getErr := emptyBitmap.Get(ctx, -1)
	combineCount, combineErr := emptyBitmap.Combine(ctx, BitmapOperation("BAD"), "dest")
	notCount, notErr := emptyBitmap.Combine(ctx, BitmapNot, "dest", "extra")
	if setValue || !errors.Is(setErr, ErrInvalidBitOffset) || getValue || !errors.Is(getErr, ErrInvalidBitOffset) {
		t.Fatalf("Set value=%t err=%v; Get value=%t err=%v", setValue, setErr, getValue, getErr)
	}
	if combineCount != 0 || !errors.Is(combineErr, ErrInvalidBitmapOperation) || notCount != 0 || !errors.Is(notErr, ErrInvalidBitmapOperation) {
		t.Fatalf("Combine count=%d err=%v; NOT count=%d err=%v", combineCount, combineErr, notCount, notErr)
	}

	// BITFIELD_RO 应只返回一个整数；顶层传输错误和多元素响应都不能伪装成合法零值。
	redis, client := newMockRedis(t)
	bitmap := redis.Bitmap("bits")
	wantErr := errors.New("bitfield failed")
	command := rueidismock.Match("BITFIELD_RO", "app:bits", "GET", "i8", "0")
	gomock.InOrder(
		client.EXPECT().Do(ctx, command).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, command).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisInt64(1), rueidismock.RedisInt64(2),
		))),
	)
	transportValue, transportErr := bitmap.BitFieldGet(ctx, "i8", 0)
	lengthValue, lengthErr := bitmap.BitFieldGet(ctx, "i8", 0)
	if transportValue != 0 || !errors.Is(transportErr, wantErr) {
		t.Fatalf("transport value=%d err=%v", transportValue, transportErr)
	}
	if lengthValue != 0 || lengthErr == nil {
		t.Fatalf("length value=%d err=%v", lengthValue, lengthErr)
	}
}
