package pprofserver

import "testing"

func TestStart_emptyIsNoOp(t *testing.T) {
	t.Parallel()
	Start("")
}

func TestStartFromEnv_emptyAddr(t *testing.T) {
	t.Setenv("GOOGLESQL_ENGINE_PPROF_ADDR", "")
	StartFromEnv()
}
