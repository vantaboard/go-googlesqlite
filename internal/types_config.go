package internal

import "os"

// TransformConfig provides configuration for transformations
type TransformConfig struct {
	Debug   bool
	Dialect Dialect
}

// DefaultTransformConfig returns a default configuration
func DefaultTransformConfig() *TransformConfig {
	debug := os.Getenv("GOOGLESQLITE_DEBUG") == "true"
	return &TransformConfig{
		Debug:   debug,
		Dialect: SQLiteDialect{},
	}
}

func effectiveDialect(cfg *TransformConfig) Dialect {
	if cfg == nil || cfg.Dialect == nil {
		return SQLiteDialect{}
	}
	return cfg.Dialect
}
