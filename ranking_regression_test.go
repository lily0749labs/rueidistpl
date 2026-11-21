package rueidistpl

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

func TestRankingPageValidatesBounds(t *testing.T) {
	client := rueidismock.NewClient(gomock.NewController(t))
	ranking := NewRanking[string](&Redis{client: client}, "ranking")
	for _, bounds := range [][2]int64{{-1, 10}, {0, 0}, {0, -1}, {0, MaxRankingPageSize + 1}, {math.MaxInt64, 2}} {
		if _, err := ranking.Page(context.Background(), bounds[0], bounds[1]); !errors.Is(err, ErrInvalidRankingPage) {
			t.Errorf("Page(%d, %d) error = %v, want ErrInvalidRankingPage", bounds[0], bounds[1], err)
		}
	}
}

func TestRankingPageReadsRequestedWindow(t *testing.T) {
	client := rueidismock.NewClient(gomock.NewController(t), rueidismock.WithSlotCheck())
	ranking := NewRanking[string](&Redis{client: client, keyPrefix: "app"}, "ranking")
	valuesKey, scoresKey := ranking.Keys()
	ctx := context.Background()
	client.EXPECT().Do(ctx, rueidismock.MatchFn(func(command []string) bool {
		return len(command) == 7 && command[0] == "EVALSHA" && reflect.DeepEqual(command[2:], []string{"2", valuesKey, scoresKey, "20", "29"})
	})).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisBlobString("user-21"), rueidismock.RedisBlobString("42"), rueidismock.RedisBlobString(`"Ada"`),
	)))
	values, err := ranking.Page(ctx, 20, 10)
	if err != nil || !reflect.DeepEqual(values, []Ranked[string]{{ID: "user-21", Score: 42, Value: "Ada"}}) {
		t.Fatalf("Page() = (%#v, %v), want requested ranking entry", values, err)
	}
}

func TestCompositeKeysStayInSameSlot(t *testing.T) {
	tests := []struct{ prefix, namespace string }{
		{"app", "ranking"}, {"", ""}, {"app", "rank{}"}, {"app{}", "ranking"},
		{"", "{}"}, {"", "rank{}{tenant}"}, {"app", "{tenant}:ranking"}, {"app", "rank{"},
	}
	for _, test := range tests {
		t.Run(test.prefix+"/"+test.namespace, func(t *testing.T) {
			client := rueidismock.NewClient(gomock.NewController(t), rueidismock.WithSlotCheck())
			redis := &Redis{client: client, keyPrefix: test.prefix}
			ranking := NewRanking[string](redis, test.namespace)
			queue := NewDelayQueue[string](redis, test.namespace)
			for _, keys := range []func() (string, string){ranking.Keys, queue.Keys} {
				left, right := keys()
				// Cluster 模式的 Builder 在多 key 跨 slot 时会 panic。
				client.B().Del().Key(left, right).Build()
			}
		})
	}
}
