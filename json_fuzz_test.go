package rueidistpl

import "testing"

// FuzzDecodeJSONZeroValue 用任意字节串压测公共 JSON 解码边界：
// 解码失败时不得暴露 encoding/json 已部分写入的对象，必须严格返回 T 的零值。
func FuzzDecodeJSONZeroValue(f *testing.F) {
	// Arrange：种子同时包含合法对象、类型冲突的部分解码数据、截断 JSON 和空输入。
	f.Add(`{"id":1,"name":"Alice"}`)
	f.Add(`{"id":99,"name":[]}`)
	f.Add(`{"id":1`)
	f.Add("")

	f.Fuzz(func(t *testing.T, raw string) {
		// Act：将 fuzz 输入作为 Redis 中读出的原始 JSON，保留值和错误便于断点检查。
		decodedValue, decodeErr := decodeJSON[unitUser](raw)

		// Assert：只限定错误路径的安全契约；合法 JSON 允许 encoding/json 按自身规则解码。
		if decodeErr != nil && decodedValue != (unitUser{}) {
			t.Fatalf("decodeJSON value=%+v err=%v raw=%q; want zero value on error", decodedValue, decodeErr, raw)
		}
	})
}
