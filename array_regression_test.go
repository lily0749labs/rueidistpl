package rueidistpl

import (
	"context"
	"reflect"
	"testing"

	rueidismock "github.com/redis/rueidis/mock"
	"go.uber.org/mock/gomock"
)

func TestArrayLastItemsSkipsHoles(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "oldest first", true: "newest first"}[reverse], func(t *testing.T) {
			client := rueidismock.NewClient(gomock.NewController(t))
			array := NewArray[string](&Redis{client: client, keyPrefix: "app"}, "array")
			ctx := context.Background()
			command := []string{"ARLASTITEMS", "app:array", "4"}
			if reverse {
				command = append(command, "REV")
			}
			client.EXPECT().Do(ctx, rueidismock.Match(command...)).Return(rueidismock.Result(rueidismock.RedisArray(
				rueidismock.RedisNil(), rueidismock.RedisBlobString(`"a"`), rueidismock.RedisNil(), rueidismock.RedisBlobString(`""`),
			)))
			values, err := array.LastItems(ctx, 4, reverse)
			if err != nil || !reflect.DeepEqual(values, []string{"a", ""}) {
				t.Fatalf("LastItems() = (%#v, %v), want a and an empty string", values, err)
			}
		})
	}
}

func TestArrayLastItemsPreservesJSONNull(t *testing.T) {
	client := rueidismock.NewClient(gomock.NewController(t))
	array := NewArray[*string](&Redis{client: client}, "array")
	ctx := context.Background()
	client.EXPECT().Do(ctx, rueidismock.Match("ARLASTITEMS", "array", "2")).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisNil(), rueidismock.RedisBlobString("null"),
	)))
	values, err := array.LastItems(ctx, 2, false)
	if err != nil || len(values) != 1 || values[0] != nil {
		t.Fatalf("LastItems() = (%#v, %v), want one JSON null value", values, err)
	}
}

func TestArrayLastItemsRejectsInvalidJSON(t *testing.T) {
	client := rueidismock.NewClient(gomock.NewController(t))
	array := NewArray[string](&Redis{client: client}, "array")
	ctx := context.Background()
	client.EXPECT().Do(ctx, rueidismock.Match("ARLASTITEMS", "array", "2")).Return(rueidismock.Result(rueidismock.RedisArray(
		rueidismock.RedisBlobString(`"a"`), rueidismock.RedisBlobString("invalid"),
	)))
	if values, err := array.LastItems(ctx, 2, false); values != nil || err == nil {
		t.Fatalf("LastItems() = (%#v, %v), want nil and a decoding error", values, err)
	}
}
