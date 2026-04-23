// Package pprofserver exposes standard net/http/pprof endpoints for heap, CPU, mutex, and block
// profiling while running heavy GoogleSQL workloads (engine benchmarks, integration tests, or
// downstream binaries such as bigquery-emulator).
//
// Quick heap capture (while the process is under load):
//
//	curl -sS 'http://127.0.0.1:6061/debug/pprof/heap' -o heap.prof
//	go tool pprof -http=:0 heap.prof
//
// Environment (optional, read when StartFromEnv runs or when Start is first called with a non-empty addr):
//
//	GOOGLESQL_ENGINE_PPROF_ADDR   listen address, e.g. 127.0.0.1:6061 (StartFromEnv)
//	GOOGLESQL_ENGINE_PPROF_MUTEX  set to "1" to enable mutex profiling (runtime.SetMutexProfileFraction)
//	GOOGLESQL_ENGINE_PPROF_BLOCK  set to "1" to enable block profiling (runtime.SetBlockProfileRate)
//
// Pairing heap with DuckDB (googlesqlengineduck): set GOOGLESQL_ENGINE_DUCK_EXPLAIN_ANALYZE=before|after
// and/or GOOGLESQL_ENGINE_LOG_SQL_CORRELATION=1; see go-googlesql-engine internal/duckdb_explain.go.
// Log lines include correlation_id and pprof_heap_hint; capture heap while the query runs, then use go tool pprof.
package pprofserver

import (
	"log"
	"net/http"
	_ "net/http/pprof" // registers handlers on DefaultServeMux
	"os"
	"runtime"
	"strings"
	"sync/atomic"
)

var started atomic.Bool

// StartFromEnv calls Start with GOOGLESQL_ENGINE_PPROF_ADDR (trimmed). No-op if unset.
func StartFromEnv() {
	Start(strings.TrimSpace(os.Getenv("GOOGLESQL_ENGINE_PPROF_ADDR")))
}

// Start serves pprof on addr (for example "127.0.0.1:6061" or "localhost:0" for any free port).
// Empty addr is a no-op. Only the first successful Start with a non-empty addr takes effect.
func Start(addr string) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return
	}
	if !started.CompareAndSwap(false, true) {
		return
	}
	applyOptionalRuntimeProfiles()
	go func() {
		log.Printf("[pprofserver] listening on http://%s/debug/pprof/ (heap: /debug/pprof/heap)\n", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Printf("[pprofserver] server exited: %v\n", err)
		}
	}()
}

func applyOptionalRuntimeProfiles() {
	if strings.TrimSpace(os.Getenv("GOOGLESQL_ENGINE_PPROF_MUTEX")) == "1" {
		runtime.SetMutexProfileFraction(5)
	}
	if strings.TrimSpace(os.Getenv("GOOGLESQL_ENGINE_PPROF_BLOCK")) == "1" {
		runtime.SetBlockProfileRate(1)
	}
}
