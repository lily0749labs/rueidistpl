package rueidistpl

import "testing"

var benchmarkUserSink unitUser

// BenchmarkDecodeJSON 衡量单对象读取时公共零值安全解码器的基准性能和内存分配。
func BenchmarkDecodeJSON(b *testing.B) {
	// Arrange：固定一条典型小对象 JSON，避免把测试数据构造成本计入解码指标。
	payload := `{"id":1,"name":"Alice"}`
	b.ReportAllocs()
	b.ResetTimer()

	// Act：循环调用生产解码器，并将结果写入包级 sink，防止编译器消除有效工作。
	for index := 0; index < b.N; index++ {
		decodedValue, decodeErr := decodeJSON[unitUser](payload)
		if decodeErr != nil {
			b.Fatal(decodeErr)
		}
		benchmarkUserSink = decodedValue
	}
}

// BenchmarkEncodeSetBatch 衡量泛型 Set 在一次批量写入 100 个对象前的 JSON 编码成本。
func BenchmarkEncodeSetBatch(b *testing.B) {
	// Arrange：预先创建批次，基准仅关注封装内部的逐成员编码与错误标注。
	users := make([]unitUser, 100)
	for index := range users {
		users[index] = unitUser{ID: int64(index), Name: "benchmark"}
	}
	b.ReportAllocs()
	b.ResetTimer()

	// Act：每轮从强类型批次生成 Redis SADD 所需的字符串切片。
	for index := 0; index < b.N; index++ {
		encodedValues, encodeErr := encodeSetValues(users)
		if encodeErr != nil {
			b.Fatal(encodeErr)
		}
		if len(encodedValues) != len(users) {
			b.Fatalf("encodeSetValues length=%d want=%d", len(encodedValues), len(users))
		}
	}
}
