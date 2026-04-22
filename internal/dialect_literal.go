package internal

import (
	"encoding/base64"
	"strings"

	"github.com/goccy/go-json"
)

// duckDBNativeLiteralSQL converts a googlesqlite wire literal (from LiteralFromValue: "base64...")
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
		return "'" + strings.ReplaceAll(layout.Body, "'", "''") + "'", true
	case IntValueType, FloatValueType, BoolValueType:
		return layout.Body, true
	case JsonValueType:
		esc := strings.ReplaceAll(layout.Body, "'", "''")
		return "CAST('" + esc + "' AS JSON)", true
	case ArrayValueType:
		// Body is JSON array text, e.g. [1,2,3]; use as DuckDB LIST literal.
		return layout.Body, true
	default:
		return "", false
	}
}
