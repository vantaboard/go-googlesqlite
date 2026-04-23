package internal

import "testing"

// database/sql can call driver.Stmt Close on a typed-nil *DMLStmt / *QueryStmt during teardown;
// Close must be a no-op without panicking.
func TestDriverStmtCloseNilSafe(t *testing.T) {
	t.Run("DMLStmt", func(t *testing.T) {
		var s *DMLStmt
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("QueryStmt", func(t *testing.T) {
		var s *QueryStmt
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
