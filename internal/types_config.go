package internal

// TransformConfig provides configuration for transformations
type TransformConfig struct {
	Debug bool
}

// DefaultTransformConfig returns a default configuration
func DefaultTransformConfig(debug bool) *TransformConfig {
	return &TransformConfig{
		Debug: debug,
	}
}
