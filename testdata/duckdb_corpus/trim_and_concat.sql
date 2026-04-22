-- Dual-backend corpus fragment (see duckdb_corpus_test.go).
SELECT TRIM('  x  ') AS t, CONCAT('a', 'b') AS c FROM (SELECT 1) AS _t ORDER BY t, c;
