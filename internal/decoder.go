package internal

import (
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/goccy/go-json"
)

func DecodeValue(v driver.Value) (Value, error) {
	if isNullValue(v) {
		return nil, nil
	}
	switch vv := v.(type) {
	case int64:
		return IntValue(vv), nil
	case int32:
		return IntValue(int64(vv)), nil
	case int16:
		return IntValue(int64(vv)), nil
	case int8:
		return IntValue(int64(vv)), nil
	case int:
		return IntValue(int64(vv)), nil
	case uint64:
		return IntValue(int64(vv)), nil
	case uint32:
		return IntValue(int64(vv)), nil
	case uint16:
		return IntValue(int64(vv)), nil
	case uint8:
		return IntValue(int64(vv)), nil
	case uint:
		return IntValue(int64(vv)), nil
	case float64:
		return FloatValue(vv), nil
	case float32:
		return FloatValue(float64(vv)), nil
	case bool:
		return BoolValue(vv), nil
	case *big.Int:
		if vv == nil {
			return nil, nil
		}
		if vv.IsInt64() {
			return IntValue(vv.Int64()), nil
		}
		r := new(big.Rat).SetInt(vv)
		return &NumericValue{Rat: r}, nil
	case []byte:
		if len(vv) == 0 {
			return StringValue(""), nil
		}
		return decodeStringOrLayout(string(vv))
	case time.Time:
		// Native drivers (e.g. DuckDB) scan DATE/TIMESTAMP into time.Time.
		// Use TimestampValue as the widest temporal carrier; CastValue maps to
		// DATE, DATETIME, TIME, or TIMESTAMP from column metadata.
		return TimestampValue(vv), nil
	case []interface{}:
		// DuckDB LIST / array_agg results often scan as []interface{} (elements may be
		// int64, string, nested slices, etc.).
		return decodeNativeSlice(vv)
	}
	if rv := reflect.ValueOf(v); rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() != reflect.Uint8 {
		// Typed slices from native drivers (e.g. []int64, []string); []byte / []uint8
		// are handled above as wire/string payloads.
		n := rv.Len()
		out := make([]interface{}, n)
		for i := 0; i < n; i++ {
			out[i] = rv.Index(i).Interface()
		}
		return decodeNativeSlice(out)
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected value type: %T", v)
	}
	return decodeStringOrLayout(s)
}

func decodeNativeSlice(elems []interface{}) (Value, error) {
	ret := &ArrayValue{
		values: make([]Value, 0, len(elems)),
	}
	for _, elem := range elems {
		value, err := DecodeValue(elem)
		if err != nil {
			return nil, err
		}
		ret.values = append(ret.values, value)
	}
	return ret, nil
}

// decodeStringOrLayout decodes a SQLite/googlesqlengine base64+JSON wire value when present;
// otherwise treats s as a plain SQL string (DuckDB and other native drivers).
func decodeStringOrLayout(s string) (Value, error) {
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return StringValue(s), nil
	}
	var layout ValueLayout
	if err := json.Unmarshal(decoded, &layout); err != nil || layout.Header == "" {
		return StringValue(s), nil
	}
	return decodeFromValueLayout(&layout)
}

func decodeFromValueLayout(layout *ValueLayout) (Value, error) {
	switch layout.Header {
	case StringValueType:
		return StringValue(layout.Body), nil
	case BytesValueType:
		decoded, err := base64.StdEncoding.DecodeString(layout.Body)
		if err != nil {
			return nil, err
		}
		return BytesValue(decoded), nil
	case NumericValueType:
		r := new(big.Rat)
		r.SetString(layout.Body)
		return &NumericValue{Rat: r}, nil
	case BigNumericValueType:
		r := new(big.Rat)
		r.SetString(layout.Body)
		return &NumericValue{Rat: r, isBigNumeric: true}, nil
	case DateValueType:
		t, err := parseDate(layout.Body)
		if err != nil {
			return nil, err
		}
		return DateValue(t), nil
	case DatetimeValueType:
		t, err := parseDatetime(layout.Body)
		if err != nil {
			return nil, err
		}
		return DatetimeValue(t), nil
	case TimeValueType:
		t, err := parseTime(layout.Body)
		if err != nil {
			return nil, err
		}
		return TimeValue(t), nil
	case TimestampValueType:
		microsec, err := strconv.ParseInt(layout.Body, 10, 64)
		microSecondsInSecond := int64(time.Second) / int64(time.Microsecond)
		sec := microsec / microSecondsInSecond
		remainder := microsec - (sec * microSecondsInSecond)
		if err != nil {
			return nil, fmt.Errorf("failed to parse unixmicro for timestamp value %s: %w", layout.Body, err)
		}
		return TimestampValue(time.Unix(sec, remainder*int64(time.Microsecond))), nil
	case IntervalValueType:
		return parseInterval(layout.Body)
	case JsonValueType:
		return JsonValue(layout.Body), nil
	case ArrayValueType:
		var arr []interface{}
		if err := json.Unmarshal([]byte(layout.Body), &arr); err != nil {
			return nil, fmt.Errorf("failed to decode array body: %w", err)
		}
		ret := &ArrayValue{
			values: make([]Value, 0, len(arr)),
		}
		for _, elem := range arr {
			value, err := DecodeValue(elem)
			if err != nil {
				return nil, err
			}
			ret.values = append(ret.values, value)
		}
		return ret, nil
	case StructValueType:
		var structLayout StructValueLayout
		if err := json.Unmarshal([]byte(layout.Body), &structLayout); err != nil {
			return nil, err
		}
		m := map[string]Value{}
		values := make([]Value, 0, len(structLayout.Values))
		for i, data := range structLayout.Values {
			value, err := DecodeValue(data)
			if err != nil {
				return nil, err
			}
			m[structLayout.Keys[i]] = value
			values = append(values, value)
		}
		ret := &StructValue{}
		ret.keys = structLayout.Keys
		ret.values = values
		ret.m = m
		return ret, nil
	}
	return nil, fmt.Errorf("unexpected value header: %s", layout.Header)
}
