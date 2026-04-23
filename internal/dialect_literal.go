package internal

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/goccy/go-json"
)

// duckDBQuoteString returns a single-quoted SQL string with standard escaping.
func duckDBQuoteString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// duckDBSQLFromValue renders a googlesqlengine Value as DuckDB SQL literal text (for use inside generated SQL).
func duckDBSQLFromValue(v Value) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	switch vv := v.(type) {
	case IntValue:
		i64, err := vv.ToInt64()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(i64, 10), nil
	case FloatValue:
		f64, err := vv.ToFloat64()
		if err != nil {
			return "", err
		}
		return strconv.FormatFloat(f64, 'g', -1, 64), nil
	case BoolValue:
		b, err := vv.ToBool()
		if err != nil {
			return "", err
		}
		if b {
			return "TRUE", nil
		}
		return "FALSE", nil
	case StringValue:
		return duckDBQuoteString(string(vv)), nil
	case JsonValue:
		return "CAST(" + duckDBQuoteString(string(vv)) + " AS JSON)", nil
	case *SafeValue:
		return duckDBSQLFromValue(vv.value)
	case *ArrayValue:
		if len(vv.values) == 0 {
			return "[]", nil
		}
		parts := make([]string, 0, len(vv.values))
		for _, el := range vv.values {
			s, err := duckDBSQLFromValue(el)
			if err != nil {
				return "", err
			}
			parts = append(parts, s)
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	case *StructValue:
		if len(vv.keys) == 0 {
			return "{}", nil
		}
		parts := make([]string, 0, len(vv.keys))
		for i := range vv.keys {
			s, err := duckDBSQLFromValue(vv.values[i])
			if err != nil {
				return "", err
			}
			parts = append(parts, duckDBQuoteString(vv.keys[i])+": "+s)
		}
		return "{" + strings.Join(parts, ", ") + "}", nil
	case DateValue:
		ds, err := vv.ToString()
		if err != nil {
			return "", err
		}
		return "DATE " + duckDBQuoteString(ds), nil
	case DatetimeValue:
		ds, err := vv.ToString()
		if err != nil {
			return "", err
		}
		return "TIMESTAMP " + duckDBQuoteString(ds), nil
	case TimeValue:
		ds, err := vv.ToString()
		if err != nil {
			return "", err
		}
		return "TIME " + duckDBQuoteString(ds), nil
	case TimestampValue:
		ds, err := vv.ToString()
		if err != nil {
			return "", err
		}
		return "TIMESTAMP " + duckDBQuoteString(ds), nil
	case *NumericValue:
		s, err := vv.ToString()
		if err != nil {
			return "", err
		}
		return s, nil
	case BytesValue:
		if len(vv) == 0 {
			return "x''", nil
		}
		return "x'" + hex.EncodeToString([]byte(vv)) + "'", nil
	case *IntervalValue:
		s, err := vv.ToString()
		if err != nil {
			return "", err
		}
		return "INTERVAL " + duckDBQuoteString(s), nil
	default:
		return "", fmt.Errorf("duckdb literal from value: unsupported type %T", v)
	}
}

// duckDBNativeLiteralSQL converts a googlesqlengine wire literal (from LiteralFromValue: "base64...")
// into DuckDB SQL that evaluates to the same logical value. Empty string means pass through val.
func duckDBNativeLiteralSQL(val string) (string, bool) {
	s := strings.TrimSpace(val)
	if len(s) < 2 || s[0] != '"' || s[len(s)-1] != '"' {
		return "", false
	}
	inner := s[1 : len(s)-1]
	b, err := base64.StdEncoding.DecodeString(inner)
	if err != nil {
		return "", false
	}
	var layout ValueLayout
	if err := json.Unmarshal(b, &layout); err != nil || layout.Header == "" {
		return "", false
	}
	switch layout.Header {
	case StringValueType:
		return duckDBQuoteString(layout.Body), true
	case IntValueType, FloatValueType, BoolValueType:
		return layout.Body, true
	case JsonValueType:
		esc := strings.ReplaceAll(layout.Body, "'", "''")
		return "CAST('" + esc + "' AS JSON)", true
	case ArrayValueType:
		decoded, err := decodeFromValueLayout(&ValueLayout{Header: ArrayValueType, Body: layout.Body})
		if err != nil {
			return "", false
		}
		sql, err := duckDBSQLFromValue(decoded)
		if err != nil {
			return "", false
		}
		return sql, true
	case StructValueType:
		decoded, err := decodeFromValueLayout(&ValueLayout{Header: StructValueType, Body: layout.Body})
		if err != nil {
			return "", false
		}
		sql, err := duckDBSQLFromValue(decoded)
		if err != nil {
			return "", false
		}
		return sql, true
	case DateValueType, DatetimeValueType, TimeValueType, TimestampValueType, IntervalValueType, NumericValueType, BigNumericValueType, BytesValueType:
		decoded, err := decodeFromValueLayout(&ValueLayout{Header: layout.Header, Body: layout.Body})
		if err != nil {
			return "", false
		}
		sql, err := duckDBSQLFromValue(decoded)
		if err != nil {
			return "", false
		}
		return sql, true
	default:
		return "", false
	}
}

// duckDBPlainStringFromWireOrQuotedLiteral extracts a STRING payload from a Literal expression's
// Value field: googlesqlengine wire (`"base64"`) or a single-quoted SQL string (`'eDate'`).
func duckDBPlainStringFromWireOrQuotedLiteral(val string) (string, bool) {
	s := strings.TrimSpace(val)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		inner := s[1 : len(s)-1]
		b, err := base64.StdEncoding.DecodeString(inner)
		if err != nil {
			return "", false
		}
		var layout ValueLayout
		if err := json.Unmarshal(b, &layout); err != nil || layout.Header != StringValueType {
			return "", false
		}
		return layout.Body, true
	}
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return strings.ReplaceAll(s[1:len(s)-1], "''", "'"), true
	}
	return "", false
}
