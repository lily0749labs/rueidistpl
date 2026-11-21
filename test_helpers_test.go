package rueidistpl

import (
	"slices"
	"strconv"
	"testing"

	"github.com/redis/rueidis"
	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

// unitUser 是各数据结构共享的可比较测试模型，便于同时验证 JSON 与强类型结果。
type unitUser struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// nonRetryableCommandMatcher 在保留原命令匹配规则的同时，确认写命令没有被标记为自动重试。
// 用于取出并删除、以及返回值会随重复执行变化的 Lua，防止网络异常时产生二次副作用。
type nonRetryableCommandMatcher struct {
	command gomock.Matcher
}

func (matcher nonRetryableCommandMatcher) Matches(value any) bool {
	command, ok := value.(rueidis.Completed)
	return ok && !command.IsRetryable() && matcher.command.Matches(value)
}

func (matcher nonRetryableCommandMatcher) String() string {
	return "non-retryable " + matcher.command.String()
}

func nonRetryable(command gomock.Matcher) gomock.Matcher {
	return nonRetryableCommandMatcher{command: command}
}

// retryableCommandMatcher 用于确认可安全重复执行的 Lua 已显式开启自动重试。
// 主要保护“比较所有权令牌后删除”这类天然幂等的恢复操作。
type retryableCommandMatcher struct {
	command gomock.Matcher
}

func (matcher retryableCommandMatcher) Matches(value any) bool {
	command, ok := value.(rueidis.Completed)
	return ok && command.IsRetryable() && matcher.command.Matches(value)
}

func (matcher retryableCommandMatcher) String() string {
	return "retryable " + matcher.command.String()
}

func retryable(command gomock.Matcher) gomock.Matcher {
	return retryableCommandMatcher{command: command}
}

// newMockRedis 创建应用前缀为 app 的门面和 Rueidis 官方 mock 客户端。
// gomock.Controller 绑定 testing.T，测试结束时会自动检查所有预期命令是否发生。
func newMockRedis(t *testing.T) (*Redis, *rueidismock.Client) {
	t.Helper()
	client := rueidismock.NewClient(gomock.NewController(t))
	return &Redis{client: client, keyPrefix: "app"}, client
}

// redisStrings 将字符串切片转换成 Redis Array 响应，用于模拟 LRANGE、HMGET 等命令。
func redisStrings(values ...string) rueidis.RedisMessage {
	messages := make([]rueidis.RedisMessage, len(values))
	for index, value := range values {
		messages[index] = rueidismock.RedisBlobString(value)
	}
	return rueidismock.RedisArray(messages...)
}

// redisStreamEntry 构造 Rueidis 能解析的 XRANGE 单条响应，字段布局固定为 data/payload。
func redisStreamEntry(id, payload string) rueidis.RedisMessage {
	return rueidismock.RedisArray(
		rueidismock.RedisBlobString(id),
		rueidismock.RedisArray(
			rueidismock.RedisBlobString(streamDataField),
			rueidismock.RedisBlobString(payload),
		),
	)
}

// matchLua 忽略脚本 SHA，只严格比较 EVALSHA 的 key 数量、key 顺序和业务参数，
// 既避免测试依赖脚本文本哈希，又能确保 Cluster key 与 Lua ARGV 没有错位。
func matchLua(keys, args []string) gomock.Matcher {
	return matchLuaArgs(keys, func(actual []string) bool { return slices.Equal(actual, args) })
}

// matchLuaArgs 允许测试在运行时检查动态 Lua 参数，例如随机生成的幂等所有权令牌。
func matchLuaArgs(keys []string, matchArgs func([]string) bool) gomock.Matcher {
	return rueidismock.MatchFn(func(command []string) bool {
		if len(command) < 3 || command[0] != "EVALSHA" {
			return false
		}
		keyCount, err := strconv.Atoi(command[2])
		if err != nil || keyCount != len(keys) || len(command) < 3+len(keys) {
			return false
		}
		return slices.Equal(command[3:3+keyCount], keys) && matchArgs(command[3+keyCount:])
	}, "Lua keys and arguments")
}

// matchIdempotencySet 捕获 Run 写入的随机所有权令牌，同时严格检查 SET NX PX 的 key 与 TTL。
func matchIdempotencySet(key, ttl string, token *string) gomock.Matcher {
	return rueidismock.MatchFn(func(command []string) bool {
		if len(command) != 6 || command[0] != "SET" || command[1] != key ||
			command[3] != "NX" || command[4] != "PX" || command[5] != ttl || len(command[2]) != 32 {
			return false
		}
		if token != nil {
			*token = command[2]
		}
		return true
	}, "SET NX PX with ownership token")
}
