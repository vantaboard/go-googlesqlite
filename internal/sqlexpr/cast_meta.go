package sqlexpr

import "github.com/vantaboard/go-googlesql/types"

// CastMetadata carries type information for native CAST emission (Dialect.MaybeEmitNativeCast).
type CastMetadata struct {
	FromType        types.Type
	ToType          types.Type
	SafeCast        bool
	ReturnNullOnErr bool
}
