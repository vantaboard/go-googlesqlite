package pureanalyzer

import "errors"

// ErrUnsupportedFeature is returned when the query uses syntax or semantics
// not implemented by the pure-Go analyzer subset. Callers may fall back to CGO.
var ErrUnsupportedFeature = errors.New("pureanalyzer: unsupported feature")
