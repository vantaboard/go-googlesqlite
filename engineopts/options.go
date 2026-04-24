// Package engineopts holds production GoogleSQL analyzer and language options for
// go-googlesql-engine. It lives in a separate package so internal/pureanalyzer can
// share the same options without an import cycle with internal.
package engineopts

import (
	"github.com/vantaboard/go-googlesql"
	ast "github.com/vantaboard/go-googlesql/resolved_ast"
	"github.com/vantaboard/go-googlesql/types"
)

// NewAnalyzerOptions returns analyzer options matching go-googlesql-engine's
// production Analyzer (see internal/analyzer.go).
func NewAnalyzerOptions() (*googlesql.AnalyzerOptions, error) {
	langOpt := googlesql.NewLanguageOptions()
	langOpt.SetNameResolutionMode(googlesql.NameResolutionDefault)
	langOpt.SetProductMode(types.ProductInternal)
	langOpt.SetEnabledLanguageFeatures([]googlesql.LanguageFeature{
		googlesql.FeatureAnalyticFunctions,
		googlesql.FeatureNamedArguments,
		googlesql.FeatureNumericType,
		googlesql.FeatureBignumericType,
		googlesql.FeatureV13DecimalAlias,
		googlesql.FeatureCreateTableNotNull,
		googlesql.FeatureParameterizedTypes,
		googlesql.FeatureTablesample,
		googlesql.FeatureTimestampNanos,
		googlesql.FeatureV11HavingInAggregate,
		googlesql.FeatureV11NullHandlingModifierInAggregate,
		googlesql.FeatureV11NullHandlingModifierInAnalytic,
		googlesql.FeatureV11OrderByCollate,
		googlesql.FeatureV11SelectStarExceptReplace,
		googlesql.FeatureV12SafeFunctionCall,
		googlesql.FeatureJsonType,
		googlesql.FeatureJsonArrayFunctions,
		googlesql.FeatureJsonConstructorFunctions,
		googlesql.FeatureJsonMutatorFunctions,
		googlesql.FeatureJsonKeysFunction,
		googlesql.FeatureJsonLaxValueExtractionFunctions,
		googlesql.FeatureJsonStrictNumberParsing,
		googlesql.FeatureV13IsDistinct,
		googlesql.FeatureV13FormatInCast,
		googlesql.FeatureV13DateArithmetics,
		googlesql.FeatureV11OrderByInAggregate,
		googlesql.FeatureV11LimitInAggregate,
		googlesql.FeatureV13DateTimeConstructors,
		googlesql.FeatureV13ExtendedDateTimeSignatures,
		googlesql.FeatureV12CivilTime,
		googlesql.FeatureV12WeekWithWeekday,
		googlesql.FeatureIntervalType,
		googlesql.FeatureGroupByRollup,
		googlesql.FeatureV13NullsFirstLastInOrderBy,
		googlesql.FeatureV13Qualify,
		googlesql.FeatureV13AllowDashesInTableName,
		googlesql.FeatureGeography,
		googlesql.FeatureV13ExtendedGeographyParsers,
		googlesql.FeatureTemplateFunctions,
		googlesql.FeatureV11WithOnSubquery,
		googlesql.FeatureV13Pivot,
		googlesql.FeatureV13Unpivot,
		googlesql.FeatureDMLUpdateWithJoin,
		googlesql.FeatureV13OmitInsertColumnList,
		googlesql.FeatureV13WithRecursive,
		googlesql.FeatureV12GroupByArray,
		googlesql.FeatureV12GroupByStruct,
		googlesql.FeatureV14GroupByAll,
		googlesql.LanguageFeature(14027),
		googlesql.LanguageFeature(14028),
		googlesql.LanguageFeature(14029),
		googlesql.LanguageFeature(14031),
		googlesql.LanguageFeature(14032),
		googlesql.LanguageFeature(14033),
	})
	langOpt.SetSupportedStatementKinds([]ast.Kind{
		ast.BeginStmt,
		ast.CommitStmt,
		ast.MergeStmt,
		ast.QueryStmt,
		ast.InsertStmt,
		ast.UpdateStmt,
		ast.DeleteStmt,
		ast.DropStmt,
		ast.TruncateStmt,
		ast.CreateSchemaStmt,
		ast.CreateTableStmt,
		ast.CreateTableAsSelectStmt,
		ast.CreateProcedureStmt,
		ast.CreateFunctionStmt,
		ast.CreateTableFunctionStmt,
		ast.CreateViewStmt,
		ast.DropFunctionStmt,
		ast.DropRowAccessPolicyStmt,
		ast.CreateRowAccessPolicyStmt,
	})
	if err := langOpt.EnableReservableKeyword("QUALIFY", true); err != nil {
		return nil, err
	}
	opt := googlesql.NewAnalyzerOptions()
	opt.SetAllowUndeclaredParameters(true)
	opt.SetLanguage(langOpt)
	opt.SetParseLocationRecordType(googlesql.ParseLocationRecordFullNodeScope)
	return opt, nil
}
