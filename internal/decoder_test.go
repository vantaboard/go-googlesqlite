package internal

import (
	"testing"
	"time"

	"github.com/vantaboard/go-googlesql/types"
)

func TestDecodeValueTimeTime(t *testing.T) {
	tm := time.Date(2026, 1, 7, 0, 0, 0, 0, time.UTC)
	v, err := DecodeValue(tm)
	if err != nil {
		t.Fatal(err)
	}
	ts, ok := v.(TimestampValue)
	if !ok {
		t.Fatalf("expected TimestampValue, got %T", v)
	}
	if !time.Time(ts).Equal(tm) {
		t.Fatalf("got %v want %v", time.Time(ts), tm)
	}

	dateType := types.DateType()
	casted, err := CastValue(dateType, v)
	if err != nil {
		t.Fatal(err)
	}
	dv, ok := casted.(DateValue)
	if !ok {
		t.Fatalf("expected DateValue after cast to DATE, got %T", casted)
	}
	s, err := dv.ToString()
	if err != nil {
		t.Fatal(err)
	}
	if s != "2026-01-07" {
		t.Fatalf("ToString: got %q want 2026-01-07", s)
	}
}

func TestDecodeValueNativeSliceInterface(t *testing.T) {
	v, err := DecodeValue([]interface{}{int64(1), int64(2), "x"})
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.(*ArrayValue)
	if !ok || arr == nil {
		t.Fatalf("expected *ArrayValue, got %T", v)
	}
	if len(arr.values) != 3 {
		t.Fatalf("len %d want 3", len(arr.values))
	}
}

func TestDecodeValueTypedInt64Slice(t *testing.T) {
	v, err := DecodeValue([]int64{10, 20})
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.(*ArrayValue)
	if !ok || arr == nil {
		t.Fatalf("expected *ArrayValue, got %T", v)
	}
	if len(arr.values) != 2 {
		t.Fatalf("len %d want 2", len(arr.values))
	}
}
