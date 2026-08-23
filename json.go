package rueidistpl

import "encoding/json"

// decodeJSON 将 JSON 解码为目标类型。
// 解码失败时始终返回 T 的零值，避免调用方误用 encoding/json 留下的部分赋值结果。
func decodeJSON[T any](raw string) (T, error) {
	var value T
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}
