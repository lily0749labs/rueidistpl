package rueidistpl

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestJoinKey(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{name: "both", prefix: "app", key: "users", want: "app:users"},
		{name: "empty prefix", key: "users", want: "users"},
		{name: "empty key", prefix: "app", want: "app"},
		{name: "both empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := joinKey(test.prefix, test.key); got != test.want {
				t.Fatalf("joinKey(%q, %q) = %q, want %q", test.prefix, test.key, got, test.want)
			}
		})
	}
}

func TestAtLeastMillisecond(t *testing.T) {
	tests := []struct {
		input time.Duration
		want  time.Duration
	}{
		{input: -time.Nanosecond, want: -time.Nanosecond},
		{input: 0, want: 0},
		{input: time.Nanosecond, want: time.Millisecond},
		{input: time.Millisecond, want: time.Millisecond},
		{input: time.Second, want: time.Second},
	}
	for _, test := range tests {
		if got := atLeastMillisecond(test.input); got != test.want {
			t.Errorf("atLeastMillisecond(%s) = %s, want %s", test.input, got, test.want)
		}
	}
}

func TestCompositeKeyPrefixUsesStableHashTag(t *testing.T) {
	redis := &Redis{keyPrefix: "app"}
	const want = "app:ranking:{YXBwOnJhbmtpbmc}"
	if got := compositeKeyPrefix(redis, "ranking"); got != want {
		t.Fatalf("compositeKeyPrefix() = %q, want %q", got, want)
	}
}

func TestExpirationInfo(t *testing.T) {
	tests := []struct {
		name         string
		milliseconds int64
		want         ExpirationInfo
		wantErr      bool
	}{
		{name: "ttl", milliseconds: 1500, want: ExpirationInfo{Exists: true, TTL: 1500 * time.Millisecond}},
		{name: "zero ttl", milliseconds: 0, want: ExpirationInfo{Exists: true}},
		{name: "persistent", milliseconds: -1, want: ExpirationInfo{Exists: true, Persistent: true}},
		{name: "missing", milliseconds: -2, want: ExpirationInfo{}},
		{name: "invalid", milliseconds: -3, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := expirationInfo(test.milliseconds)
			if test.wantErr {
				if !errors.Is(err, ErrInvalidExpirationResponse) {
					t.Fatalf("expirationInfo() error = %v, want ErrInvalidExpirationResponse", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expirationInfo() unexpected error: %v", err)
			}
			if got != test.want {
				t.Fatalf("expirationInfo() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestDecodeJSONReturnsZeroValueOnError(t *testing.T) {
	type payload struct {
		Name string
		Age  int
	}

	got, err := decodeJSON[payload](`{"Name":"partial","Age":"invalid"}`)
	if err == nil {
		t.Fatal("decodeJSON() error = nil, want decoding error")
	}
	if got != (payload{}) {
		t.Fatalf("decodeJSON() = %#v, want zero value", got)
	}

	want := payload{Name: "Ada", Age: 36}
	got, err = decodeJSON[payload](`{"Name":"Ada","Age":36}`)
	if err != nil {
		t.Fatalf("decodeJSON() unexpected error: %v", err)
	}
	if got != want {
		t.Fatalf("decodeJSON() = %#v, want %#v", got, want)
	}
}

func TestValidationHelpers(t *testing.T) {
	if !validGeoCoordinate(-180, -85.05112878) || !validGeoCoordinate(180, 85.05112878) {
		t.Fatal("validGeoCoordinate() rejected a boundary coordinate")
	}
	invalidCoordinates := [][2]float64{
		{-180.0001, 0},
		{180.0001, 0},
		{0, -85.05112879},
		{0, 85.05112879},
		{math.NaN(), 0},
		{0, math.Inf(1)},
	}
	for _, coordinate := range invalidCoordinates {
		if validGeoCoordinate(coordinate[0], coordinate[1]) {
			t.Errorf("validGeoCoordinate(%v, %v) = true, want false", coordinate[0], coordinate[1])
		}
	}

	for _, unit := range []GeoUnit{GeoMeters, GeoKilometers, GeoFeet, GeoMiles} {
		if !validGeoUnit(unit) {
			t.Errorf("validGeoUnit(%q) = false, want true", unit)
		}
	}
	if validGeoUnit("yards") {
		t.Fatal("validGeoUnit(\"yards\") = true, want false")
	}
}

func TestNumericArguments(t *testing.T) {
	gotVector, err := vectorArguments([]float64{1, 0.5, -2})
	if err != nil {
		t.Fatalf("vectorArguments() unexpected error: %v", err)
	}
	wantVector := []string{"VALUES", "3", "1", "0.5", "-2"}
	if !reflect.DeepEqual(gotVector, wantVector) {
		t.Fatalf("vectorArguments() = %#v, want %#v", gotVector, wantVector)
	}
	if _, err := vectorArguments(nil); !errors.Is(err, ErrVectorRequired) {
		t.Fatalf("vectorArguments(nil) error = %v, want ErrVectorRequired", err)
	}
	if _, err := vectorArguments([]float64{math.NaN()}); !errors.Is(err, ErrInvalidVectorValue) {
		t.Fatalf("vectorArguments(NaN) error = %v, want ErrInvalidVectorValue", err)
	}

	gotFloats, err := finiteFloatArguments([]float64{1.25, -3})
	if err != nil {
		t.Fatalf("finiteFloatArguments() unexpected error: %v", err)
	}
	if want := []string{"1.25", "-3"}; !reflect.DeepEqual(gotFloats, want) {
		t.Fatalf("finiteFloatArguments() = %#v, want %#v", gotFloats, want)
	}
	if _, err := finiteFloatArguments([]float64{math.Inf(-1)}); !errors.Is(err, ErrInvalidTDigestValue) {
		t.Fatalf("finiteFloatArguments(-Inf) error = %v, want ErrInvalidTDigestValue", err)
	}
}

func TestEncodeArrayValuesReportsIndex(t *testing.T) {
	got, err := encodeArrayValues([]string{"a", "b"}, 10)
	if err != nil {
		t.Fatalf("encodeArrayValues() unexpected error: %v", err)
	}
	if want := []string{`"a"`, `"b"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("encodeArrayValues() = %#v, want %#v", got, want)
	}

	_, err = encodeArrayValues([]any{"ok", make(chan int)}, 20)
	if err == nil || !strings.Contains(err.Error(), "array index 21") {
		t.Fatalf("encodeArrayValues() error = %v, want index context", err)
	}
}
