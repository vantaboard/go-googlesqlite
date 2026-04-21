package internal

import (
	"context"
	"log/slog"
	"time"

	"github.com/vantaboard/go-googlesql"
)

type (
	analyzerKey    struct{}
	namePathKey    struct{}
	nodeMapKey     struct{}
	funcMapKey     struct{}
	currentTimeKey struct{}
	loggerKey      struct{}
)

// WithLogger attaches a slog.Logger to the context for structured logging in the query pipeline.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, loggerKey{}, l)
}

// Logger returns the slog.Logger from context, or slog.Default() if none was set.
func Logger(ctx context.Context) *slog.Logger {
	l, _ := ctx.Value(loggerKey{}).(*slog.Logger)
	if l == nil {
		return slog.Default()
	}
	return l
}

func analyzerFromContext(ctx context.Context) *Analyzer {
	value := ctx.Value(analyzerKey{})
	if value == nil {
		return nil
	}
	return value.(*Analyzer)
}

func withAnalyzer(ctx context.Context, analyzer *Analyzer) context.Context {
	return context.WithValue(ctx, analyzerKey{}, analyzer)
}

func namePathFromContext(ctx context.Context) *NamePath {
	value := ctx.Value(namePathKey{})
	if value == nil {
		return nil
	}
	return value.(*NamePath)
}

func withNamePath(ctx context.Context, namePath *NamePath) context.Context {
	return context.WithValue(ctx, namePathKey{}, namePath)
}

func withNodeMap(ctx context.Context, m *googlesql.NodeMap) context.Context {
	return context.WithValue(ctx, nodeMapKey{}, m)
}

func nodeMapFromContext(ctx context.Context) *googlesql.NodeMap {
	value := ctx.Value(nodeMapKey{})
	if value == nil {
		return nil
	}
	return value.(*googlesql.NodeMap)
}

func withFuncMap(ctx context.Context, m map[string]*FunctionSpec) context.Context {
	return context.WithValue(ctx, funcMapKey{}, m)
}

func funcMapFromContext(ctx context.Context) map[string]*FunctionSpec {
	value := ctx.Value(funcMapKey{})
	if value == nil {
		return nil
	}
	return value.(map[string]*FunctionSpec)
}

func WithCurrentTime(ctx context.Context, now time.Time) context.Context {
	return context.WithValue(ctx, currentTimeKey{}, &now)
}

func CurrentTime(ctx context.Context) *time.Time {
	value := ctx.Value(currentTimeKey{})
	if value == nil {
		return nil
	}
	return value.(*time.Time)
}
