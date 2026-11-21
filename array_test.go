package rueidistpl

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// TestGenericArrayLifecycle 验证 Redis 8.8+ 泛型 Array 的完整正常路径：
// 对象按 JSON 写入连续或稀疏下标，批量读取保留空槽，游标追加、环形写入和删除命令均保持原生语义。
func TestGenericArrayLifecycle(t *testing.T) {
	// Arrange：准备两个强类型对象和稳定 JSON，所有调用使用显式变量，便于断点检查中间结果。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	array := NewArray[unitUser](redis, "timeline")
	alice := unitUser{ID: 1, Name: "Alice"}
	bob := unitUser{ID: 2, Name: "Bob"}
	aliceJSON := `{"id":1,"name":"Alice"}`
	bobJSON := `{"id":2,"name":"Bob"}`
	actualKey := array.Key()
	if actualKey != "app:timeline" {
		t.Fatalf("Key value=%q", actualKey)
	}

	// 连续写入使用 ARSET；map 写入必须转换为下标升序的 ARMSET，避免 Go map 顺序导致命令漂移。
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("ARSET", "app:timeline", "2", aliceJSON, bobJSON)).Return(
			rueidismock.Result(rueidismock.RedisInt64(2)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARMSET", "app:timeline", "0", bobJSON, "5", aliceJSON)).Return(
			rueidismock.Result(rueidismock.RedisInt64(2)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARGET", "app:timeline", "2")).Return(
			rueidismock.Result(rueidismock.RedisBlobString(aliceJSON)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARMGET", "app:timeline", "5", "1", "0")).Return(
			rueidismock.Result(rueidismock.RedisArray(
				rueidismock.RedisBlobString(aliceJSON),
				rueidismock.RedisNil(),
				rueidismock.RedisBlobString(bobJSON),
			)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARGETRANGE", "app:timeline", "2", "0")).Return(
			rueidismock.Result(rueidismock.RedisArray(
				rueidismock.RedisBlobString(aliceJSON),
				rueidismock.RedisNil(),
				rueidismock.RedisBlobString(bobJSON),
			)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARSCAN", "app:timeline", "0", "9")).Return(
			rueidismock.Result(rueidismock.RedisArray(
				rueidismock.RedisInt64(0), rueidismock.RedisBlobString(bobJSON),
				rueidismock.RedisInt64(5), rueidismock.RedisBlobString(aliceJSON),
			)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARSCAN", "app:timeline", "9", "0", "LIMIT", "1")).Return(
			rueidismock.Result(rueidismock.RedisArray(
				rueidismock.RedisArray(rueidismock.RedisInt64(5), rueidismock.RedisBlobString(aliceJSON)),
			)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARINSERT", "app:timeline", aliceJSON, bobJSON)).Return(
			rueidismock.Result(rueidismock.RedisInt64(7)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARRING", "app:timeline", "3", bobJSON)).Return(
			rueidismock.Result(rueidismock.RedisInt64(2)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARLASTITEMS", "app:timeline", "2")).Return(
			rueidismock.Result(redisStrings(bobJSON, aliceJSON)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARLASTITEMS", "app:timeline", "2", "REV")).Return(
			rueidismock.Result(redisStrings(aliceJSON, bobJSON)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARLEN", "app:timeline")).Return(
			rueidismock.Result(rueidismock.RedisInt64(6)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARCOUNT", "app:timeline")).Return(
			rueidismock.Result(rueidismock.RedisInt64(3)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARNEXT", "app:timeline")).Return(
			rueidismock.Result(rueidismock.RedisInt64(8)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARSEEK", "app:timeline", "10")).Return(
			rueidismock.Result(rueidismock.RedisInt64(1)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARDEL", "app:timeline", "0", "5")).Return(
			rueidismock.Result(rueidismock.RedisInt64(2)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("ARDELRANGE", "app:timeline", "1", "3", "7", "9")).Return(
			rueidismock.Result(rueidismock.RedisInt64(3)),
		),
		client.EXPECT().Do(ctx, rueidismock.Match("DEL", "app:timeline")).Return(
			rueidismock.Result(rueidismock.RedisInt64(1)),
		),
	)

	// Act/Assert：先验证连续与离散写入返回的“新建槽位数”，它不同于逻辑长度。
	setCount, setErr := array.Set(ctx, 2, alice, bob)
	if setErr != nil || setCount != 2 {
		t.Fatalf("Set count=%d err=%v", setCount, setErr)
	}
	setManyCount, setManyErr := array.SetMany(ctx, map[int64]unitUser{5: alice, 0: bob})
	if setManyErr != nil || setManyCount != 2 {
		t.Fatalf("SetMany count=%d err=%v", setManyCount, setManyErr)
	}

	// 单下标读取直接返回强类型；多下标读取则保留请求顺序和下标 1 的空槽。
	getValue, getErr := array.Get(ctx, 2)
	if getErr != nil || getValue != alice {
		t.Fatalf("Get value=%+v err=%v", getValue, getErr)
	}
	manyItems, getManyErr := array.GetMany(ctx, 5, 1, 0)
	wantManyItems := []ArrayItem[unitUser]{
		{Index: 5, Exists: true, Value: alice},
		{Index: 1},
		{Index: 0, Exists: true, Value: bob},
	}
	if getManyErr != nil || !slices.Equal(manyItems, wantManyItems) {
		t.Fatalf("GetMany items=%+v err=%v", manyItems, getManyErr)
	}

	// 倒序 Range 的 Item.Index 必须依次为 2、1、0；空槽不能与合法零值对象混淆。
	rangeItems, rangeErr := array.Range(ctx, 2, 0)
	wantRangeItems := []ArrayItem[unitUser]{
		{Index: 2, Exists: true, Value: alice},
		{Index: 1},
		{Index: 0, Exists: true, Value: bob},
	}
	if rangeErr != nil || !slices.Equal(rangeItems, wantRangeItems) {
		t.Fatalf("Range items=%+v err=%v", rangeItems, rangeErr)
	}

	// Scan 只返回非空槽；分别验证无 LIMIT 的正序扫描和 LIMIT=1 的倒序扫描。
	scanItems, scanErr := array.Scan(ctx, 0, 9, 0)
	wantScanItems := []ArrayItem[unitUser]{
		{Index: 0, Exists: true, Value: bob},
		{Index: 5, Exists: true, Value: alice},
	}
	if scanErr != nil || !slices.Equal(scanItems, wantScanItems) {
		t.Fatalf("Scan items=%+v err=%v", scanItems, scanErr)
	}
	limitedItems, limitedErr := array.Scan(ctx, 9, 0, 1)
	wantLimitedItems := []ArrayItem[unitUser]{{Index: 5, Exists: true, Value: alice}}
	if limitedErr != nil || !slices.Equal(limitedItems, wantLimitedItems) {
		t.Fatalf("limited Scan items=%+v err=%v", limitedItems, limitedErr)
	}

	// 游标写入返回最后使用的下标；Ring 额外施加固定窗口大小。
	lastInsertIndex, insertErr := array.Insert(ctx, alice, bob)
	if insertErr != nil || lastInsertIndex != 7 {
		t.Fatalf("Insert index=%d err=%v", lastInsertIndex, insertErr)
	}
	lastRingIndex, ringErr := array.Ring(ctx, 3, bob)
	if ringErr != nil || lastRingIndex != 2 {
		t.Fatalf("Ring index=%d err=%v", lastRingIndex, ringErr)
	}

	// 最近项可选择时间顺序；两个返回切片都保留为变量，断点时可直接比较方向。
	recentValues, recentErr := array.LastItems(ctx, 2, false)
	if recentErr != nil || !slices.Equal(recentValues, []unitUser{bob, alice}) {
		t.Fatalf("LastItems values=%+v err=%v", recentValues, recentErr)
	}
	reversedValues, reversedErr := array.LastItems(ctx, 2, true)
	if reversedErr != nil || !slices.Equal(reversedValues, []unitUser{alice, bob}) {
		t.Fatalf("reversed LastItems values=%+v err=%v", reversedValues, reversedErr)
	}

	// 稀疏数组逻辑长度为 6、实际元素数为 3，两者不能混用；随后检查插入游标和定位结果。
	logicalLength, lenErr := array.Len(ctx)
	if lenErr != nil || logicalLength != 6 {
		t.Fatalf("Len value=%d err=%v", logicalLength, lenErr)
	}
	populatedCount, countErr := array.Count(ctx)
	if countErr != nil || populatedCount != 3 {
		t.Fatalf("Count value=%d err=%v", populatedCount, countErr)
	}
	nextIndex, nextErr := array.Next(ctx)
	if nextErr != nil || nextIndex != 8 {
		t.Fatalf("Next value=%d err=%v", nextIndex, nextErr)
	}
	seeked, seekErr := array.Seek(ctx, 10)
	if seekErr != nil || !seeked {
		t.Fatalf("Seek value=%t err=%v", seeked, seekErr)
	}

	// 最后分别删除离散下标、两个闭区间和整个 key，并检查 Redis 返回的实际删除数。
	deletedCount, deleteErr := array.Delete(ctx, 0, 5)
	if deleteErr != nil || deletedCount != 2 {
		t.Fatalf("Delete count=%d err=%v", deletedCount, deleteErr)
	}
	deletedRangeCount, deleteRangesErr := array.DeleteRanges(ctx,
		ArrayRange{Start: 1, End: 3},
		ArrayRange{Start: 7, End: 9},
	)
	if deleteRangesErr != nil || deletedRangeCount != 3 {
		t.Fatalf("DeleteRanges count=%d err=%v", deletedRangeCount, deleteRangesErr)
	}
	clearErr := array.Clear(ctx)
	if clearErr != nil {
		t.Fatal(clearErr)
	}
}

// TestGenericArrayEmptyInputAndEncodingErrors 验证空批次不会构造非法 Array 命令，
// ARINSERT/ARRING 的必填参数会在本地校验，JSON 无法编码时不会访问 Redis。
func TestGenericArrayEmptyInputAndEncodingErrors(t *testing.T) {
	// Arrange：不配置客户端，任何应当本地处理的分支若意外访问 Redis 都会立刻失败。
	ctx := context.Background()
	emptyArray := &Array[unitUser]{}

	// 空 Set/SetMany/Delete/DeleteRanges 都是返回 0 的 no-op；空 GetMany/LastItems 返回非 nil 空切片。
	setCount, setErr := emptyArray.Set(ctx, 0)
	setManyCount, setManyErr := emptyArray.SetMany(ctx, map[int64]unitUser{})
	getManyItems, getManyErr := emptyArray.GetMany(ctx)
	lastItems, lastItemsErr := emptyArray.LastItems(ctx, 0, false)
	deleteCount, deleteErr := emptyArray.Delete(ctx)
	deleteRangeCount, deleteRangesErr := emptyArray.DeleteRanges(ctx)
	if setErr != nil || setCount != 0 || setManyErr != nil || setManyCount != 0 {
		t.Fatalf("empty Set count=%d err=%v; SetMany count=%d err=%v", setCount, setErr, setManyCount, setManyErr)
	}
	if getManyErr != nil || getManyItems == nil || len(getManyItems) != 0 {
		t.Fatalf("empty GetMany items=%+v err=%v", getManyItems, getManyErr)
	}
	if lastItemsErr != nil || lastItems == nil || len(lastItems) != 0 {
		t.Fatalf("empty LastItems values=%+v err=%v", lastItems, lastItemsErr)
	}
	if deleteErr != nil || deleteCount != 0 || deleteRangesErr != nil || deleteRangeCount != 0 {
		t.Fatalf("empty Delete count=%d err=%v; DeleteRanges count=%d err=%v", deleteCount, deleteErr, deleteRangeCount, deleteRangesErr)
	}

	// Redis 语法要求 Insert/Ring 至少一个 value，Ring 的 size 还必须为正数。
	emptyInsertIndex, emptyInsertErr := emptyArray.Insert(ctx)
	emptyRingIndex, emptyRingErr := emptyArray.Ring(ctx, 3)
	invalidRingIndex, invalidRingErr := emptyArray.Ring(ctx, 0, unitUser{})
	if emptyInsertIndex != 0 || !errors.Is(emptyInsertErr, ErrArrayValuesRequired) {
		t.Fatalf("empty Insert index=%d err=%v", emptyInsertIndex, emptyInsertErr)
	}
	if emptyRingIndex != 0 || !errors.Is(emptyRingErr, ErrArrayValuesRequired) {
		t.Fatalf("empty Ring index=%d err=%v", emptyRingIndex, emptyRingErr)
	}
	if invalidRingIndex != 0 || !errors.Is(invalidRingErr, ErrInvalidArrayRingSize) {
		t.Fatalf("invalid Ring index=%d err=%v", invalidRingIndex, invalidRingErr)
	}

	// chan 无法编码为 JSON，四种写入入口都必须返回零结果和编码错误。
	badArray := &Array[chan int]{}
	badValue := make(chan int)
	badSetCount, badSetErr := badArray.Set(ctx, 4, badValue)
	badSetManyCount, badSetManyErr := badArray.SetMany(ctx, map[int64]chan int{7: badValue})
	badInsertIndex, badInsertErr := badArray.Insert(ctx, badValue)
	badRingIndex, badRingErr := badArray.Ring(ctx, 3, badValue)
	if badSetCount != 0 || badSetErr == nil || badSetManyCount != 0 || badSetManyErr == nil {
		t.Fatalf("bad Set count=%d err=%v; SetMany count=%d err=%v", badSetCount, badSetErr, badSetManyCount, badSetManyErr)
	}
	if badInsertIndex != 0 || badInsertErr == nil || badRingIndex != 0 || badRingErr == nil {
		t.Fatalf("bad Insert index=%d err=%v; Ring index=%d err=%v", badInsertIndex, badInsertErr, badRingIndex, badRingErr)
	}
}

// TestGenericArrayReadErrorBranches 验证单值、多值、范围和最近项读取遇到 Redis 错误、
// 响应长度异常、空槽或脏 JSON 时，不会向调用方泄漏部分解码结果。
func TestGenericArrayReadErrorBranches(t *testing.T) {
	// Arrange：为每条读取错误路径准备独立响应，顺序与后面的显式调用完全一致。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	array := NewArray[unitUser](redis, "timeline")
	wantErr := errors.New("array read failed")
	gomock.InOrder(
		client.EXPECT().Do(ctx, rueidismock.Match("ARGET", "app:timeline", "1")).Return(rueidismock.Result(rueidismock.RedisNil())),
		client.EXPECT().Do(ctx, rueidismock.Match("ARGET", "app:timeline", "2")).Return(rueidismock.Result(rueidismock.RedisBlobString(`{"id":99,"name":[]}`))),
		client.EXPECT().Do(ctx, rueidismock.Match("ARMGET", "app:timeline", "1")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("ARMGET", "app:timeline", "1", "2")).Return(rueidismock.Result(redisStrings(`{"id":1,"name":"Alice"}`))),
		client.EXPECT().Do(ctx, rueidismock.Match("ARMGET", "app:timeline", "1")).Return(rueidismock.Result(redisStrings(`{"id":99,"name":[]}`))),
		client.EXPECT().Do(ctx, rueidismock.Match("ARGETRANGE", "app:timeline", "0", "1")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("ARGETRANGE", "app:timeline", "0", "1")).Return(rueidismock.Result(redisStrings(`{"id":1,"name":"Alice"}`, `not-json`))),
		client.EXPECT().Do(ctx, rueidismock.Match("ARLASTITEMS", "app:timeline", "2")).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, rueidismock.Match("ARLASTITEMS", "app:timeline", "2")).Return(rueidismock.Result(redisStrings(`{"id":1,"name":"Alice"}`, `not-json`))),
		client.EXPECT().Do(ctx, rueidismock.Match("ARLASTITEMS", "app:timeline", "1", "REV")).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisNil()))),
		client.EXPECT().Do(ctx, rueidismock.Match("ARNEXT", "app:timeline")).Return(rueidismock.Result(rueidismock.RedisNil())),
	)

	// 单下标缺失应统一为 Rueidis Nil；脏 JSON 则返回完整的 unitUser 零值。
	missingValue, missingErr := array.Get(ctx, 1)
	dirtyValue, dirtyErr := array.Get(ctx, 2)
	if missingValue != (unitUser{}) || !IsNotFound(missingErr) {
		t.Fatalf("missing Get value=%+v err=%v", missingValue, missingErr)
	}
	if dirtyValue != (unitUser{}) || dirtyErr == nil {
		t.Fatalf("dirty Get value=%+v err=%v", dirtyValue, dirtyErr)
	}

	// GetMany 分别覆盖传输错误、响应数量不匹配和单项 JSON 解码失败，均必须返回 nil 切片。
	transportItems, transportErr := array.GetMany(ctx, 1)
	lengthItems, lengthErr := array.GetMany(ctx, 1, 2)
	dirtyItems, dirtyItemsErr := array.GetMany(ctx, 1)
	if transportItems != nil || !errors.Is(transportErr, wantErr) {
		t.Fatalf("GetMany transport items=%+v err=%v", transportItems, transportErr)
	}
	if lengthItems != nil || lengthErr == nil {
		t.Fatalf("GetMany length items=%+v err=%v", lengthItems, lengthErr)
	}
	if dirtyItems != nil || dirtyItemsErr == nil {
		t.Fatalf("GetMany dirty items=%+v err=%v", dirtyItems, dirtyItemsErr)
	}

	// Range 和 LastItems 即使前面已有合法元素，只要后续元素失败也必须丢弃整个结果。
	rangeTransportItems, rangeTransportErr := array.Range(ctx, 0, 1)
	rangeDirtyItems, rangeDirtyErr := array.Range(ctx, 0, 1)
	recentTransportValues, recentTransportErr := array.LastItems(ctx, 2, false)
	recentDirtyValues, recentDirtyErr := array.LastItems(ctx, 2, false)
	recentNilValues, recentNilErr := array.LastItems(ctx, 1, true)
	if rangeTransportItems != nil || !errors.Is(rangeTransportErr, wantErr) {
		t.Fatalf("Range transport items=%+v err=%v", rangeTransportItems, rangeTransportErr)
	}
	if rangeDirtyItems != nil || rangeDirtyErr == nil {
		t.Fatalf("Range dirty items=%+v err=%v", rangeDirtyItems, rangeDirtyErr)
	}
	if recentTransportValues != nil || !errors.Is(recentTransportErr, wantErr) {
		t.Fatalf("LastItems transport values=%+v err=%v", recentTransportValues, recentTransportErr)
	}
	if recentDirtyValues != nil || recentDirtyErr == nil || recentNilValues == nil || len(recentNilValues) != 0 || recentNilErr != nil {
		t.Fatalf("LastItems dirty values=%+v err=%v; nil values=%+v err=%v", recentDirtyValues, recentDirtyErr, recentNilValues, recentNilErr)
	}

	// ARNEXT 游标耗尽返回 Redis Nil，调用方可以继续用统一的 IsNotFound 判断。
	nextIndex, nextErr := array.Next(ctx)
	if nextIndex != 0 || !IsNotFound(nextErr) {
		t.Fatalf("Next index=%d err=%v", nextIndex, nextErr)
	}
}

// TestGenericArrayScanErrorBranches 验证 ARSCAN 的扁平及 RESP3 嵌套 index/value 协议：
// 顶层错误、数量异常、非法下标、空 value 和脏 JSON 都必须返回 nil，而不是半完成的扫描结果。
func TestGenericArrayScanErrorBranches(t *testing.T) {
	// Arrange：七次相同扫描依次注入不同错误，最后两次检查 RESP 类型及嵌套 pair 的长度校验。
	redis, client := newMockRedis(t)
	ctx := context.Background()
	array := NewArray[unitUser](redis, "timeline")
	wantErr := errors.New("array scan failed")
	plainScan := rueidismock.Match("ARSCAN", "app:timeline", "0", "9")
	gomock.InOrder(
		client.EXPECT().Do(ctx, plainScan).Return(rueidismock.ErrorResult(wantErr)),
		client.EXPECT().Do(ctx, plainScan).Return(rueidismock.Result(rueidismock.RedisArray(rueidismock.RedisInt64(1)))),
		client.EXPECT().Do(ctx, plainScan).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisBlobString("bad-index"), rueidismock.RedisBlobString(`{"id":1,"name":"Alice"}`),
		))),
		client.EXPECT().Do(ctx, plainScan).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisInt64(1), rueidismock.RedisNil(),
		))),
		client.EXPECT().Do(ctx, plainScan).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisInt64(1), rueidismock.RedisBlobString(`{"id":99,"name":[]}`),
		))),
		client.EXPECT().Do(ctx, rueidismock.Match("ARSCAN", "app:timeline", "0", "9", "LIMIT", "2")).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisInt64(1), rueidismock.RedisArray(),
		))),
		client.EXPECT().Do(ctx, plainScan).Return(rueidismock.Result(rueidismock.RedisArray(
			rueidismock.RedisArray(rueidismock.RedisInt64(1)),
		))),
	)

	// Act：每个错误响应单独保存，调试时可查看是哪一层协议校验失败。
	transportItems, transportErr := array.Scan(ctx, 0, 9, 0)
	oddItems, oddErr := array.Scan(ctx, 0, 9, 0)
	indexItems, indexErr := array.Scan(ctx, 0, 9, 0)
	nilItems, nilErr := array.Scan(ctx, 0, 9, 0)
	dirtyItems, dirtyErr := array.Scan(ctx, 0, 9, 0)
	typeItems, typeErr := array.Scan(ctx, 0, 9, 2)
	nestedItems, nestedErr := array.Scan(ctx, 0, 9, 0)

	// Assert：不管错误出现在顶层、下标或 value，调用方都只能得到 nil 切片和非 nil 错误。
	if transportItems != nil || !errors.Is(transportErr, wantErr) {
		t.Fatalf("transport Scan items=%+v err=%v", transportItems, transportErr)
	}
	cases := []struct {
		name  string
		items []ArrayItem[unitUser]
		err   error
	}{
		{name: "odd", items: oddItems, err: oddErr},
		{name: "index", items: indexItems, err: indexErr},
		{name: "nil", items: nilItems, err: nilErr},
		{name: "dirty", items: dirtyItems, err: dirtyErr},
		{name: "type", items: typeItems, err: typeErr},
		{name: "nested", items: nestedItems, err: nestedErr},
	}
	for _, testCase := range cases {
		if testCase.items != nil || testCase.err == nil {
			t.Fatalf("%s Scan items=%+v err=%v", testCase.name, testCase.items, testCase.err)
		}
	}
}

// TestDecodeArrayItem 验证底层空槽和 RESP 类型处理，确保辅助函数不会把 Redis Null 当作 JSON null 对象。
func TestDecodeArrayItem(t *testing.T) {
	// 空槽只保留下标并将 Exists 设为 false，Value 必须保持零值。
	nilMessage := rueidismock.RedisNil()
	nilItem, nilErr := decodeArrayItem[unitUser](9, &nilMessage)
	if nilErr != nil || nilItem != (ArrayItem[unitUser]{Index: 9}) {
		t.Fatalf("nil item=%+v err=%v", nilItem, nilErr)
	}

	// 非字符串 RESP Array 不能作为对象载荷，必须返回结构零值和解析错误。
	invalidMessage := rueidismock.RedisArray(rueidismock.RedisInt64(1))
	invalidItem, invalidErr := decodeArrayItem[unitUser](3, &invalidMessage)
	if invalidItem != (ArrayItem[unitUser]{}) || invalidErr == nil {
		t.Fatalf("invalid item=%+v err=%v", invalidItem, invalidErr)
	}

	// 合法 JSON 必须恢复为强类型值，并明确标记槽位存在。
	validMessage := rueidismock.RedisBlobString(`{"id":1,"name":"Alice"}`)
	validItem, validErr := decodeArrayItem[unitUser](4, &validMessage)
	wantItem := ArrayItem[unitUser]{Index: 4, Exists: true, Value: unitUser{ID: 1, Name: "Alice"}}
	if validErr != nil || validItem != wantItem {
		t.Fatalf("valid item=%+v err=%v", validItem, validErr)
	}

	// 引用 rueidis 包确认 Nil 的判定与公开兼容函数一致，避免仅依赖错误字符串。
	redisNilErr := nilMessage.Error()
	if !rueidis.IsRedisNil(redisNilErr) {
		t.Fatalf("nil error=%v", redisNilErr)
	}
}
