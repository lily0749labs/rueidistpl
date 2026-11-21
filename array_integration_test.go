package rueidistpl_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/lily0749labs/rueidistpl"
	"github.com/redis/rueidis"
)

// TestRedisArrayIntegration 使用真实 Redis 8.10.0 验证 Redis 8.8+ 原生 Array：
// 连续/稀疏写入、空槽读取、正反向范围、非空扫描、插入游标、区间删除和 Ring 窗口都走真实协议。
// 测试默认跳过，只有显式设置 RUEIDISTPL_ARRAY_INTEGRATION=1 时才会运行。
func TestRedisArrayIntegration(t *testing.T) {
	// 该用例会发送 8.8 才新增的 AR* 命令，必须与普通 6.2+ 集成测试分开开启。
	if os.Getenv("RUEIDISTPL_ARRAY_INTEGRATION") != "1" {
		t.Skip("set RUEIDISTPL_ARRAY_INTEGRATION=1 to run Redis 8.8+ Array integration test")
	}

	// 使用纳秒时间戳隔离每次执行的 key；CI 指向 8.10.0，本地可通过环境变量覆盖连接。
	prefix := fmt.Sprintf("rueidistpl:array-test:%d", time.Now().UnixNano())
	redisClient, err := rueidistpl.New(rueidistpl.Config{
		ClientOption: rueidis.ClientOption{
			InitAddress: []string{envOr("REDIS_ADDR", "127.0.0.1:16479")},
			Password:    envOr("REDIS_PASSWORD", "redis1234"),
		},
		KeyPrefix: prefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer redisClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pingErr := redisClient.Ping(ctx)
	if pingErr != nil {
		t.Fatal(pingErr)
	}

	// 普通数组和 Ring 分开使用 key，避免游标及窗口截断互相影响。
	array := rueidistpl.NewArray[integrationUser](redisClient, "users")
	ring := rueidistpl.NewArray[integrationUser](redisClient, "ring")
	defer func() {
		_ = array.Clear(ctx)
		_ = ring.Clear(ctx)
	}()
	alice := integrationUser{ID: 1, Name: "Alice"}
	bob := integrationUser{ID: 2, Name: "Bob"}
	carol := integrationUser{ID: 3, Name: "Carol"}
	dave := integrationUser{ID: 4, Name: "Dave"}

	// 先在下标 0 和 3 写入对象，制造两个真实空槽；Len 看最高下标，Count 只数非空槽。
	createdSlots, setManyErr := array.SetMany(ctx, map[int64]integrationUser{3: bob, 0: alice})
	if setManyErr != nil || createdSlots != 2 {
		t.Fatalf("SetMany created=%d err=%v", createdSlots, setManyErr)
	}
	logicalLength, lenErr := array.Len(ctx)
	populatedCount, countErr := array.Count(ctx)
	if lenErr != nil || logicalLength != 4 || countErr != nil || populatedCount != 2 {
		t.Fatalf("Len=%d err=%v; Count=%d err=%v", logicalLength, lenErr, populatedCount, countErr)
	}

	// ARMGET 返回顺序必须与请求下标一致，下标 1 通过 Exists=false 与对象零值区分。
	manyItems, getManyErr := array.GetMany(ctx, 3, 1, 0)
	wantManyItems := []rueidistpl.ArrayItem[integrationUser]{
		{Index: 3, Exists: true, Value: bob},
		{Index: 1},
		{Index: 0, Exists: true, Value: alice},
	}
	if getManyErr != nil || !slices.Equal(manyItems, wantManyItems) {
		t.Fatalf("GetMany items=%+v err=%v", manyItems, getManyErr)
	}

	// ARGETRANGE 支持 start>end 的倒序范围，且响应中必须保留下标 2、1 的两个空槽。
	rangeItems, rangeErr := array.Range(ctx, 3, 0)
	wantRangeItems := []rueidistpl.ArrayItem[integrationUser]{
		{Index: 3, Exists: true, Value: bob},
		{Index: 2},
		{Index: 1},
		{Index: 0, Exists: true, Value: alice},
	}
	if rangeErr != nil || !slices.Equal(rangeItems, wantRangeItems) {
		t.Fatalf("Range items=%+v err=%v", rangeItems, rangeErr)
	}

	// ARSCAN 不返回空槽；LIMIT=1 应只保留倒序遇到的第一个非空下标 3。
	scanItems, scanErr := array.Scan(ctx, 0, 3, 0)
	wantScanItems := []rueidistpl.ArrayItem[integrationUser]{
		{Index: 0, Exists: true, Value: alice},
		{Index: 3, Exists: true, Value: bob},
	}
	if scanErr != nil || !slices.Equal(scanItems, wantScanItems) {
		t.Fatalf("Scan items=%+v err=%v", scanItems, scanErr)
	}
	limitedItems, limitedErr := array.Scan(ctx, 3, 0, 1)
	wantLimitedItems := []rueidistpl.ArrayItem[integrationUser]{{Index: 3, Exists: true, Value: bob}}
	if limitedErr != nil || !slices.Equal(limitedItems, wantLimitedItems) {
		t.Fatalf("limited Scan items=%+v err=%v", limitedItems, limitedErr)
	}

	// 单槽缺失沿用项目统一的 IsNotFound；写入后再删除可验证新建槽数和实际删除数。
	missingValue, missingErr := array.Get(ctx, 2)
	if missingValue != (integrationUser{}) || !rueidistpl.IsNotFound(missingErr) {
		t.Fatalf("missing Get value=%+v err=%v", missingValue, missingErr)
	}
	createdContiguous, setErr := array.Set(ctx, 1, carol, dave)
	if setErr != nil || createdContiguous != 2 {
		t.Fatalf("Set created=%d err=%v", createdContiguous, setErr)
	}
	deletedRangeCount, deleteRangesErr := array.DeleteRanges(ctx, rueidistpl.ArrayRange{Start: 1, End: 2})
	if deleteRangesErr != nil || deletedRangeCount != 2 {
		t.Fatalf("DeleteRanges deleted=%d err=%v", deletedRangeCount, deleteRangesErr)
	}

	// ARSEEK 将插入游标移到 10，两个对象应使用下标 10、11，ARNEXT 随后返回 12。
	seeked, seekErr := array.Seek(ctx, 10)
	if seekErr != nil || !seeked {
		t.Fatalf("Seek value=%t err=%v", seeked, seekErr)
	}
	lastInsertIndex, insertErr := array.Insert(ctx, carol, dave)
	if insertErr != nil || lastInsertIndex != 11 {
		t.Fatalf("Insert index=%d err=%v", lastInsertIndex, insertErr)
	}
	nextIndex, nextErr := array.Next(ctx)
	if nextErr != nil || nextIndex != 12 {
		t.Fatalf("Next index=%d err=%v", nextIndex, nextErr)
	}
	recentValues, recentErr := array.LastItems(ctx, 2, false)
	reversedValues, reversedErr := array.LastItems(ctx, 2, true)
	if recentErr != nil || !slices.Equal(recentValues, []integrationUser{carol, dave}) {
		t.Fatalf("LastItems values=%+v err=%v", recentValues, recentErr)
	}
	if reversedErr != nil || !slices.Equal(reversedValues, []integrationUser{dave, carol}) {
		t.Fatalf("reversed LastItems values=%+v err=%v", reversedValues, reversedErr)
	}

	// 四次写入三格 Ring 后窗口内只应剩最后三项：Dave 覆盖下标 0，Bob/Carol 位于 1、2。
	lastRingIndex, ringErr := ring.Ring(ctx, 3, alice, bob, carol, dave)
	if ringErr != nil || lastRingIndex != 0 {
		t.Fatalf("Ring index=%d err=%v", lastRingIndex, ringErr)
	}
	ringItems, ringRangeErr := ring.Range(ctx, 0, 2)
	wantRingItems := []rueidistpl.ArrayItem[integrationUser]{
		{Index: 0, Exists: true, Value: dave},
		{Index: 1, Exists: true, Value: bob},
		{Index: 2, Exists: true, Value: carol},
	}
	if ringRangeErr != nil || !slices.Equal(ringItems, wantRingItems) {
		t.Fatalf("Ring Range items=%+v err=%v", ringItems, ringRangeErr)
	}

	// 离散删除只删除真实存在的下标，重复或空槽不会虚增返回计数。
	deletedCount, deleteErr := array.Delete(ctx, 0, 3, 99)
	if deleteErr != nil || deletedCount != 2 {
		t.Fatalf("Delete count=%d err=%v", deletedCount, deleteErr)
	}
}
