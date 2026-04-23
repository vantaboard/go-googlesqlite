package internal

import (
	"strings"
	"testing"
	"time"
)

func TestDuckDBSQLFromValue_arrayAndStruct(t *testing.T) {
	st := &StructValue{
		keys:   []string{"enrollmentDate", "SchoolYear"},
		values: []Value{DateValue(time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)), StringValue("2024-2025")},
		m: map[string]Value{
			"enrollmentDate": DateValue(time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)),
			"SchoolYear":     StringValue("2024-2025"),
		},
	}
	arr := &ArrayValue{values: []Value{st, st}}
	got, err := duckDBSQLFromValue(arr)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, `"`) {
		t.Fatalf("expected no JSON double-quotes in DuckDB literal, got %q", got)
	}
	if !strings.Contains(got, "'enrollmentDate':") || !strings.Contains(got, "DATE '2024-08-01'") {
		t.Fatalf("unexpected struct/list literal: %q", got)
	}
}

func TestDuckDBNativeLiteralSQL_dateAndTimestampWireLiteral(t *testing.T) {
	d := DateValue(time.Date(2026, 1, 7, 0, 0, 0, 0, time.UTC))
	wireDate, err := LiteralFromValue(d)
	if err != nil {
		t.Fatal(err)
	}
	sqlDate, ok := duckDBNativeLiteralSQL(wireDate)
	if !ok {
		t.Fatalf("expected duckDBNativeLiteralSQL ok for date wire, wire=%q", wireDate)
	}
	if strings.Contains(sqlDate, `"`) {
		t.Fatalf("expected no double quotes (DuckDB identifiers), got %q", sqlDate)
	}
	if !strings.Contains(sqlDate, "2026-01-07") {
		t.Fatalf("expected ISO date in literal, got %q", sqlDate)
	}

	ts := TimestampValue(time.Date(2026, 1, 7, 12, 0, 0, 0, time.UTC))
	wireTS, err := LiteralFromValue(ts)
	if err != nil {
		t.Fatal(err)
	}
	sqlTS, ok := duckDBNativeLiteralSQL(wireTS)
	if !ok {
		t.Fatalf("expected duckDBNativeLiteralSQL ok for timestamp wire, wire=%q", wireTS)
	}
	if strings.Contains(sqlTS, `"`) {
		t.Fatalf("expected no double quotes, got %q", sqlTS)
	}
}

func TestDuckDBNativeLiteralSQL_arrayWireLiteral(t *testing.T) {
	st := &StructValue{
		keys:   []string{"k"},
		values: []Value{IntValue(7)},
		m:      map[string]Value{"k": IntValue(7)},
	}
	arr := &ArrayValue{values: []Value{st}}
	wire, err := LiteralFromValue(arr)
	if err != nil {
		t.Fatal(err)
	}
	sql, ok := duckDBNativeLiteralSQL(wire)
	if !ok {
		t.Fatalf("expected duckDBNativeLiteralSQL ok for array wire, wire=%q", wire)
	}
	if strings.Contains(sql, `"`) {
		t.Fatalf("expected no JSON double-quotes, got %q", sql)
	}
	if !strings.HasPrefix(sql, "[{") || !strings.Contains(sql, "'k': 7") {
		t.Fatalf("unexpected sql %q", sql)
	}
}
