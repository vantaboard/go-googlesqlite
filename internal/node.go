package internal

import (
	ast "github.com/goccy/go-zetasql/resolved_ast"
)

func newNode(node ast.Node) Formatter {
	if node == nil {
		return nil
	}
	
	return nil
}

type LiteralNode struct {
	node *ast.LiteralNode
}

type ParameterNode struct {
	node *ast.ParameterNode
}

type ExpressionColumnNode struct {
	node *ast.ExpressionColumnNode
}

type ColumnRefNode struct {
	node *ast.ColumnRefNode
}

type ConstantNode struct {
	node *ast.ConstantNode
}

type SystemVariableNode struct {
	node *ast.SystemVariableNode
}

type InlineLambdaNode struct {
	node *ast.InlineLambdaNode
}

type FilterFieldArgNode struct {
	node *ast.FilterFieldArgNode
}

type FilterFieldNode struct {
	node *ast.FilterFieldNode
}

type FunctionCallNode struct {
	node *ast.FunctionCallNode
}

type AggregateFunctionCallNode struct {
	node *ast.AggregateFunctionCallNode
}

type AnalyticFunctionCallNode struct {
	node *ast.AnalyticFunctionCallNode
}

type ExtendedCastElementNode struct {
	node *ast.ExtendedCastElementNode
}

type ExtendedCastNode struct {
	node *ast.ExtendedCastNode
}

type CastNode struct {
	node *ast.CastNode
}

type MakeStructNode struct {
	node *ast.MakeStructNode
}

type MakeProtoNode struct {
	node *ast.MakeProtoNode
}

type MakeProtoFieldNode struct {
	node *ast.MakeProtoFieldNode
}

type GetStructFieldNode struct {
	node *ast.GetStructFieldNode
}

type GetProtoFieldNode struct {
	node *ast.GetProtoFieldNode
}

type GetJsonFieldNode struct {
	node *ast.GetJsonFieldNode
}

type FlattenNode struct {
	node *ast.FlattenNode
}

type FlattenedArgNode struct {
	node *ast.FlattenedArgNode
}

type ReplaceFieldItemNode struct {
	node *ast.ReplaceFieldItemNode
}

type ReplaceFieldNode struct {
	node *ast.ReplaceFieldNode
}

type SubqueryExprNode struct {
	node *ast.SubqueryExprNode
}

type LetExprNode struct {
	node *ast.LetExprNode
}

type ModelNode struct {
	node *ast.ModelNode
}

type ConnectionNode struct {
	node *ast.ConnectionNode
}

type DescriptorNode struct {
	node *ast.DescriptorNode
}

type SingleRowScanNode struct {
	node *ast.SingleRowScanNode
}

type TableScanNode struct {
	node *ast.TableScanNode
}

type JoinScanNode struct {
	node *ast.JoinScanNode
}

type ArrayScanNode struct {
	node *ast.ArrayScanNode
}

type ColumnHolderNode struct {
	node *ast.ColumnHolderNode
}

type FilterScanNode struct {
	node *ast.FilterScanNode
}

type GroupingSetNode struct {
	node *ast.GroupingSetNode
}

type AggregateScanNode struct {
	node *ast.AggregateScanNode
}

type AnonymizedAggregateScanNode struct {
	node *ast.AnonymizedAggregateScanNode
}

type SetOperationItemNode struct {
	node *ast.SetOperationItemNode
}

type SetOperationScanNode struct {
	node *ast.SetOperationScanNode
}

type OrderByScanNode struct {
	node *ast.OrderByScanNode
}

type LimitOffsetScanNode struct {
	node *ast.LimitOffsetScanNode
}

type WithRefScanNode struct {
	node *ast.WithRefScanNode
}

type AnalyticScanNode struct {
	node *ast.AnalyticScanNode
}

type SampleScanNode struct {
	node *ast.SampleScanNode
}

type ComputedColumnNode struct {
	node *ast.ComputedColumnNode
}

type OrderByItemNode struct {
	node *ast.OrderByItemNode
}

type ColumnAnnotationsNode struct {
	node *ast.ColumnAnnotationsNode
}

type GeneratedColumnInfoNode struct {
	node *ast.GeneratedColumnInfoNode
}

type ColumnDefaultValueNode struct {
	node *ast.ColumnDefaultValueNode
}

type ColumnDefinitionNode struct {
	node *ast.ColumnDefinitionNode
}

type PrimaryKeyNode struct {
	node *ast.PrimaryKeyNode
}

type ForeignKeyNode struct {
	node *ast.ForeignKeyNode
}

type CheckConstraintNode struct {
	node *ast.CheckConstraintNode
}

type OutputColumnNode struct {
	node *ast.OutputColumnNode
}

type ProjectScanNode struct {
	node *ast.ProjectScanNode
}

type TVFScanNode struct {
	node *ast.TVFScanNode
}

type GroupRowsScanNode struct {
	node *ast.GroupRowsScanNode
}

type FunctionArgumentNode struct {
	node *ast.FunctionArgumentNode
}

type ExplainStmtNode struct {
	node *ast.ExplainStmtNode
}

type QueryStmtNode struct {
	node *ast.QueryStmtNode
}

type CreateDatabaseStmtNode struct {
	node *ast.CreateDatabaseStmtNode
}

type IndexItemNode struct {
	node *ast.IndexItemNode
}

type UnnestItemNode struct {
	node *ast.UnnestItemNode
}

type CreateIndexStmtNode struct {
	node *ast.CreateIndexStmtNode
}

type CreateSchemaStmtNode struct {
	node *ast.CreateSchemaStmtNode
}

type CreateTableStmtNode struct {
	node *ast.CreateTableStmtNode
}

type CreateTableAsSelectStmtNode struct {
	node *ast.CreateTableAsSelectStmtNode
}

type CreateModelStmtNode struct {
	node *ast.CreateModelStmtNode
}

type CreateViewStmtNode struct {
	node *ast.CreateViewStmtNode
}

type WithPartitionColumnsNode struct {
	node *ast.WithPartitionColumnsNode
}

type CreateSnapshotTableStmtNode struct {
	node *ast.CreateSnapshotTableStmtNode
}

type CreateExternalTableStmtNode struct {
	node *ast.CreateExternalTableStmtNode
}

type ExportModelStmtNode struct {
	node *ast.ExportModelStmtNode
}

type ExportDataStmtNode struct {
	node *ast.ExportDataStmtNode
}

type DefineTableStmtNode struct {
	node *ast.DefineTableStmtNode
}

type DescribeStmtNode struct {
	node *ast.DescribeStmtNode
}

type ShowStmtNode struct {
	node *ast.ShowStmtNode
}

type BeginStmtNode struct {
	node *ast.BeginStmtNode
}

type SetTransactionStmtNode struct {
	node *ast.SetTransactionStmtNode
}

type CommitStmtNode struct {
	node *ast.CommitStmtNode
}

type RollbackStmtNode struct {
	node *ast.RollbackStmtNode
}

type StartBatchStmtNode struct {
	node *ast.StartBatchStmtNode
}

type RunBatchStmtNode struct {
	node *ast.RunBatchStmtNode
}

type AbortBatchStmtNode struct {
	node *ast.AbortBatchStmtNode
}

type DropStmtNode struct {
	node *ast.DropStmtNode
}

type DropMaterializedViewStmtNode struct {
	node *ast.DropMaterializedViewStmtNode
}

type DropSnapshotTableStmtNode struct {
	node *ast.DropSnapshotTableStmtNode
}

type RecursiveRefScanNode struct {
	node *ast.RecursiveRefScanNode
}

type RecursiveScanNode struct {
	node *ast.RecursiveScanNode
}

type WithScanNode struct {
	node *ast.WithScanNode
}

type WithEntryNode struct {
	node *ast.WithEntryNode
}

type OptionNode struct {
	node *ast.OptionNode
}

type WindowPartitioningNode struct {
	node *ast.WindowPartitioningNode
}

type WindowOrderingNode struct {
	node *ast.WindowOrderingNode
}

type WindowFrameNode struct {
	node *ast.WindowFrameNode
}

type AnalyticFunctionGroupNode struct {
	node *ast.AnalyticFunctionGroupNode
}

type WindowFrameExprNode struct {
	node *ast.WindowFrameExprNode
}

type DMLValueNode struct {
	node *ast.DMLValueNode
}

type DMLDefaultNode struct {
	node *ast.DMLDefaultNode
}

type AssertStmtNode struct {
	node *ast.AssertStmtNode
}

type AssertRowsModifiedNode struct {
	node *ast.AssertRowsModifiedNode
}

type InsertRowNode struct {
	node *ast.InsertRowNode
}

type InsertStmtNode struct {
	node *ast.InsertStmtNode
}

type DeleteStmtNode struct {
	node *ast.DeleteStmtNode
}

type UpdateItemNode struct {
	node *ast.UpdateItemNode
}

type UpdateArrayItemNode struct {
	node *ast.UpdateArrayItemNode
}

type UpdateStmtNode struct {
	node *ast.UpdateStmtNode
}

type MergeWhenNode struct {
	node *ast.MergeWhenNode
}

type MergeStmtNode struct {
	node *ast.MergeStmtNode
}

type TruncateStmtNode struct {
	node *ast.TruncateStmtNode
}

type ObjectUnitNode struct {
	node *ast.ObjectUnitNode
}

type PrivilegeNode struct {
	node *ast.PrivilegeNode
}

type GrantStmtNode struct {
	node *ast.GrantStmtNode
}

type RevokeStmtNode struct {
	node *ast.RevokeStmtNode
}

type AlterDatabaseStmtNode struct {
	node *ast.AlterDatabaseStmtNode
}

type AlterMaterializedViewStmtNode struct {
	node *ast.AlterMaterializedViewStmtNode
}

type AlterSchemaStmtNode struct {
	node *ast.AlterSchemaStmtNode
}

type AlterTableStmtNode struct {
	node *ast.AlterTableStmtNode
}

type AlterViewStmtNode struct {
	node *ast.AlterViewStmtNode
}

type SetOptionsActionNode struct {
	node *ast.SetOptionsActionNode
}

type AddColumnActionNode struct {
	node *ast.AddColumnActionNode
}

type AddConstraintActionNode struct {
	node *ast.AddConstraintActionNode
}

type DropConstraintActionNode struct {
	node *ast.DropConstraintActionNode
}

type DropPrimaryKeyActionNode struct {
	node *ast.DropPrimaryKeyActionNode
}

type AlterColumnOptionsActionNode struct {
	node *ast.AlterColumnOptionsActionNode
}

type AlterColumnDropNotNullActionNode struct {
	node *ast.AlterColumnDropNotNullActionNode
}

type AlterColumnSetDataTypeActionNode struct {
	node *ast.AlterColumnSetDataTypeActionNode
}

type AlterColumnSetDefaultActionNode struct {
	node *ast.AlterColumnSetDefaultActionNode
}

type AlterColumnDropDefaultActionNode struct {
	node *ast.AlterColumnDropDefaultActionNode
}

type DropColumnActionNode struct {
	node *ast.DropColumnActionNode
}

type RenameColumnActionNode struct {
	node *ast.RenameColumnActionNode
}

type SetAsActionNode struct {
	node *ast.SetAsActionNode
}

type SetCollateClauseNode struct {
	node *ast.SetCollateClauseNode
}

type AlterTableSetOptionsStmtNode struct {
	node *ast.AlterTableSetOptionsStmtNode
}

type RenameStmtNode struct {
	node *ast.RenameStmtNode
}

type CreatePrivilegeRestrictionStmtNode struct {
	node *ast.CreatePrivilegeRestrictionStmtNode
}

type CreateRowAccessPolicyStmtNode struct {
	node *ast.CreateRowAccessPolicyStmtNode
}

type DropPrivilegeRestrictionStmtNode struct {
	node *ast.DropPrivilegeRestrictionStmtNode
}

type DropRowAccessPolicyStmtNode struct {
	node *ast.DropRowAccessPolicyStmtNode
}

type DropSearchIndexStmtNode struct {
	node *ast.DropSearchIndexStmtNode
}

type GrantToActionNode struct {
	node *ast.GrantToActionNode
}

type RestrictToActionNode struct {
	node *ast.RestrictToActionNode
}

type AddToRestricteeListActionNode struct {
	node *ast.AddToRestricteeListActionNode
}

type RemoveFromRestricteeListActionNode struct {
	node *ast.RemoveFromRestricteeListActionNode
}

type FilterUsingActionNode struct {
	node *ast.FilterUsingActionNode
}

type RevokeFromActionNode struct {
	node *ast.RevokeFromActionNode
}

type RenameToActionNode struct {
	node *ast.RenameToActionNode
}

type AlterPrivilegeRestrictionStmtNode struct {
	node *ast.AlterPrivilegeRestrictionStmtNode
}

type AlterRowAccessPolicyStmtNode struct {
	node *ast.AlterRowAccessPolicyStmtNode
}

type AlterAllRowAccessPoliciesStmtNode struct {
	node *ast.AlterAllRowAccessPoliciesStmtNode
}

type CreateConstantStmtNode struct {
	node *ast.CreateConstantStmtNode
}

type CreateFunctionStmtNode struct {
	node *ast.CreateFunctionStmtNode
}

type ArgumentDefNode struct {
	node *ast.ArgumentDefNode
}

type ArgumentRefNode struct {
	node *ast.ArgumentRefNode
}

type CreateTableFunctionStmtNode struct {
	node *ast.CreateTableFunctionStmtNode
}

type RelationArgumentScanNode struct {
	node *ast.RelationArgumentScanNode
}

type ArgumentListNode struct {
	node *ast.ArgumentListNode
}

type FunctionSignatureHolderNode struct {
	node *ast.FunctionSignatureHolderNode
}

type DropFunctionStmtNode struct {
	node *ast.DropFunctionStmtNode
}

type DropTableFunctionStmtNode struct {
	node *ast.DropTableFunctionStmtNode
}

type CallStmtNode struct {
	node *ast.CallStmtNode
}

type ImportStmtNode struct {
	node *ast.ImportStmtNode
}

type ModuleStmtNode struct {
	node *ast.ModuleStmtNode
}

type AggregateHavingModifierNode struct {
	node *ast.AggregateHavingModifierNode
}

type CreateMaterializedViewStmtNode struct {
	node *ast.CreateMaterializedViewStmtNode
}

type CreateProcedureStmtNode struct {
	node *ast.CreateProcedureStmtNode
}

type ExecuteImmediateArgumentNode struct {
	node *ast.ExecuteImmediateArgumentNode
}

type ExecuteImmediateStmtNode struct {
	node *ast.ExecuteImmediateStmtNode
}

type AssignmentStmtNode struct {
	node *ast.AssignmentStmtNode
}

type CreateEntityStmtNode struct {
	node *ast.CreateEntityStmtNode
}

type AlterEntityStmtNode struct {
	node *ast.AlterEntityStmtNode
}

type PivotColumnNode struct {
	node *ast.PivotColumnNode
}

type PivotScanNode struct {
	node *ast.PivotScanNode
}

type ReturningClauseNode struct {
	node *ast.ReturningClauseNode
}

type UnpivotArgNode struct {
	node *ast.UnpivotArgNode
}

type UnpivotScanNode struct {
	node *ast.UnpivotScanNode
}

type CloneDataStmtNode struct {
	node *ast.CloneDataStmtNode
}

type TableAndColumnInfoNode struct {
	node *ast.TableAndColumnInfoNode
}

type AnalyzeStmtNode struct {
	node *ast.AnalyzeStmtNode
}

type AuxLoadDataStmtNode struct {
	node *ast.AuxLoadDataStmtNode
}

func newLiteralNode(n *ast.LiteralNode) *LiteralNode {
	return &LiteralNode{node: n}
}

func newParameterNode(n *ast.ParameterNode) *ParameterNode {
	return &ParameterNode{node: n}
}

func newExpressionColumnNode(n *ast.ExpressionColumnNode) *ExpressionColumnNode {
	return &ExpressionColumnNode{node: n}
}

func newColumnRefNode(n *ast.ColumnRefNode) *ColumnRefNode {
	return &ColumnRefNode{node: n}
}

func newConstantNode(n *ast.ConstantNode) *ConstantNode {
	return &ConstantNode{node: n}
}

func newSystemVariableNode(n *ast.SystemVariableNode) *SystemVariableNode {
	return &SystemVariableNode{node: n}
}

func newInlineLambdaNode(n *ast.InlineLambdaNode) *InlineLambdaNode {
	return &InlineLambdaNode{node: n}
}

func newFilterFieldArgNode(n *ast.FilterFieldArgNode) *FilterFieldArgNode {
	return &FilterFieldArgNode{node: n}
}

func newFilterFieldNode(n *ast.FilterFieldNode) *FilterFieldNode {
	return &FilterFieldNode{node: n}
}

func newFunctionCallNode(n *ast.FunctionCallNode) *FunctionCallNode {
	return &FunctionCallNode{node: n}
}

func newAggregateFunctionCallNode(n *ast.AggregateFunctionCallNode) *AggregateFunctionCallNode {
	return &AggregateFunctionCallNode{node: n}
}

func newAnalyticFunctionCallNode(n *ast.AnalyticFunctionCallNode) *AnalyticFunctionCallNode {
	return &AnalyticFunctionCallNode{node: n}
}

func newExtendedCastElementNode(n *ast.ExtendedCastElementNode) *ExtendedCastElementNode {
	return &ExtendedCastElementNode{node: n}
}

func newExtendedCastNode(n *ast.ExtendedCastNode) *ExtendedCastNode {
	return &ExtendedCastNode{node: n}
}

func newCastNode(n *ast.CastNode) *CastNode {
	return &CastNode{node: n}
}

func newMakeStructNode(n *ast.MakeStructNode) *MakeStructNode {
	return &MakeStructNode{node: n}
}

func newMakeProtoNode(n *ast.MakeProtoNode) *MakeProtoNode {
	return &MakeProtoNode{node: n}
}

func newMakeProtoFieldNode(n *ast.MakeProtoFieldNode) *MakeProtoFieldNode {
	return &MakeProtoFieldNode{node: n}
}

func newGetStructFieldNode(n *ast.GetStructFieldNode) *GetStructFieldNode {
	return &GetStructFieldNode{node: n}
}

func newGetProtoFieldNode(n *ast.GetProtoFieldNode) *GetProtoFieldNode {
	return &GetProtoFieldNode{node: n}
}

func newGetJsonFieldNode(n *ast.GetJsonFieldNode) *GetJsonFieldNode {
	return &GetJsonFieldNode{node: n}
}

func newFlattenNode(n *ast.FlattenNode) *FlattenNode {
	return &FlattenNode{node: n}
}

func newFlattenedArgNode(n *ast.FlattenedArgNode) *FlattenedArgNode {
	return &FlattenedArgNode{node: n}
}

func newReplaceFieldItemNode(n *ast.ReplaceFieldItemNode) *ReplaceFieldItemNode {
	return &ReplaceFieldItemNode{node: n}
}

func newReplaceFieldNode(n *ast.ReplaceFieldNode) *ReplaceFieldNode {
	return &ReplaceFieldNode{node: n}
}

func newSubqueryExprNode(n *ast.SubqueryExprNode) *SubqueryExprNode {
	return &SubqueryExprNode{node: n}
}

func newLetExprNode(n *ast.LetExprNode) *LetExprNode {
	return &LetExprNode{node: n}
}

func newModelNode(n *ast.ModelNode) *ModelNode {
	return &ModelNode{node: n}
}

func newConnectionNode(n *ast.ConnectionNode) *ConnectionNode {
	return &ConnectionNode{node: n}
}

func newDescriptorNode(n *ast.DescriptorNode) *DescriptorNode {
	return &DescriptorNode{node: n}
}

func newSingleRowScanNode(n *ast.SingleRowScanNode) *SingleRowScanNode {
	return &SingleRowScanNode{node: n}
}

func newTableScanNode(n *ast.TableScanNode) *TableScanNode {
	return &TableScanNode{node: n}
}

func newJoinScanNode(n *ast.JoinScanNode) *JoinScanNode {
	return &JoinScanNode{node: n}
}

func newArrayScanNode(n *ast.ArrayScanNode) *ArrayScanNode {
	return &ArrayScanNode{node: n}
}

func newColumnHolderNode(n *ast.ColumnHolderNode) *ColumnHolderNode {
	return &ColumnHolderNode{node: n}
}

func newFilterScanNode(n *ast.FilterScanNode) *FilterScanNode {
	return &FilterScanNode{node: n}
}

func newGroupingSetNode(n *ast.GroupingSetNode) *GroupingSetNode {
	return &GroupingSetNode{node: n}
}

func newAggregateScanNode(n *ast.AggregateScanNode) *AggregateScanNode {
	return &AggregateScanNode{node: n}
}

func newAnonymizedAggregateScanNode(n *ast.AnonymizedAggregateScanNode) *AnonymizedAggregateScanNode {
	return &AnonymizedAggregateScanNode{node: n}
}

func newSetOperationItemNode(n *ast.SetOperationItemNode) *SetOperationItemNode {
	return &SetOperationItemNode{node: n}
}

func newSetOperationScanNode(n *ast.SetOperationScanNode) *SetOperationScanNode {
	return &SetOperationScanNode{node: n}
}

func newOrderByScanNode(n *ast.OrderByScanNode) *OrderByScanNode {
	return &OrderByScanNode{node: n}
}

func newLimitOffsetScanNode(n *ast.LimitOffsetScanNode) *LimitOffsetScanNode {
	return &LimitOffsetScanNode{node: n}
}

func newWithRefScanNode(n *ast.WithRefScanNode) *WithRefScanNode {
	return &WithRefScanNode{node: n}
}

func newAnalyticScanNode(n *ast.AnalyticScanNode) *AnalyticScanNode {
	return &AnalyticScanNode{node: n}
}

func newSampleScanNode(n *ast.SampleScanNode) *SampleScanNode {
	return &SampleScanNode{node: n}
}

func newComputedColumnNode(n *ast.ComputedColumnNode) *ComputedColumnNode {
	return &ComputedColumnNode{node: n}
}

func newOrderByItemNode(n *ast.OrderByItemNode) *OrderByItemNode {
	return &OrderByItemNode{node: n}
}

func newColumnAnnotationsNode(n *ast.ColumnAnnotationsNode) *ColumnAnnotationsNode {
	return &ColumnAnnotationsNode{node: n}
}

func newGeneratedColumnInfoNode(n *ast.GeneratedColumnInfoNode) *GeneratedColumnInfoNode {
	return &GeneratedColumnInfoNode{node: n}
}

func newColumnDefaultValueNode(n *ast.ColumnDefaultValueNode) *ColumnDefaultValueNode {
	return &ColumnDefaultValueNode{node: n}
}

func newColumnDefinitionNode(n *ast.ColumnDefinitionNode) *ColumnDefinitionNode {
	return &ColumnDefinitionNode{node: n}
}

func newPrimaryKeyNode(n *ast.PrimaryKeyNode) *PrimaryKeyNode {
	return &PrimaryKeyNode{node: n}
}

func newForeignKeyNode(n *ast.ForeignKeyNode) *ForeignKeyNode {
	return &ForeignKeyNode{node: n}
}

func newCheckConstraintNode(n *ast.CheckConstraintNode) *CheckConstraintNode {
	return &CheckConstraintNode{node: n}
}

func newOutputColumnNode(n *ast.OutputColumnNode) *OutputColumnNode {
	return &OutputColumnNode{node: n}
}

func newProjectScanNode(n *ast.ProjectScanNode) *ProjectScanNode {
	return &ProjectScanNode{node: n}
}

func newTVFScanNode(n *ast.TVFScanNode) *TVFScanNode {
	return &TVFScanNode{node: n}
}

func newGroupRowsScanNode(n *ast.GroupRowsScanNode) *GroupRowsScanNode {
	return &GroupRowsScanNode{node: n}
}

func newFunctionArgumentNode(n *ast.FunctionArgumentNode) *FunctionArgumentNode {
	return &FunctionArgumentNode{node: n}
}

func newExplainStmtNode(n *ast.ExplainStmtNode) *ExplainStmtNode {
	return &ExplainStmtNode{node: n}
}

func newQueryStmtNode(n *ast.QueryStmtNode) *QueryStmtNode {
	return &QueryStmtNode{node: n}
}

func newCreateDatabaseStmtNode(n *ast.CreateDatabaseStmtNode) *CreateDatabaseStmtNode {
	return &CreateDatabaseStmtNode{node: n}
}

func newIndexItemNode(n *ast.IndexItemNode) *IndexItemNode {
	return &IndexItemNode{node: n}
}

func newUnnestItemNode(n *ast.UnnestItemNode) *UnnestItemNode {
	return &UnnestItemNode{node: n}
}

func newCreateIndexStmtNode(n *ast.CreateIndexStmtNode) *CreateIndexStmtNode {
	return &CreateIndexStmtNode{node: n}
}

func newCreateSchemaStmtNode(n *ast.CreateSchemaStmtNode) *CreateSchemaStmtNode {
	return &CreateSchemaStmtNode{node: n}
}

func newCreateTableStmtNode(n *ast.CreateTableStmtNode) *CreateTableStmtNode {
	return &CreateTableStmtNode{node: n}
}

func newCreateTableAsSelectStmtNode(n *ast.CreateTableAsSelectStmtNode) *CreateTableAsSelectStmtNode {
	return &CreateTableAsSelectStmtNode{node: n}
}

func newCreateModelStmtNode(n *ast.CreateModelStmtNode) *CreateModelStmtNode {
	return &CreateModelStmtNode{node: n}
}

func newCreateViewStmtNode(n *ast.CreateViewStmtNode) *CreateViewStmtNode {
	return &CreateViewStmtNode{node: n}
}

func newWithPartitionColumnsNode(n *ast.WithPartitionColumnsNode) *WithPartitionColumnsNode {
	return &WithPartitionColumnsNode{node: n}
}

func newCreateSnapshotTableStmtNode(n *ast.CreateSnapshotTableStmtNode) *CreateSnapshotTableStmtNode {
	return &CreateSnapshotTableStmtNode{node: n}
}

func newCreateExternalTableStmtNode(n *ast.CreateExternalTableStmtNode) *CreateExternalTableStmtNode {
	return &CreateExternalTableStmtNode{node: n}
}

func newExportModelStmtNode(n *ast.ExportModelStmtNode) *ExportModelStmtNode {
	return &ExportModelStmtNode{node: n}
}

func newExportDataStmtNode(n *ast.ExportDataStmtNode) *ExportDataStmtNode {
	return &ExportDataStmtNode{node: n}
}

func newDefineTableStmtNode(n *ast.DefineTableStmtNode) *DefineTableStmtNode {
	return &DefineTableStmtNode{node: n}
}

func newDescribeStmtNode(n *ast.DescribeStmtNode) *DescribeStmtNode {
	return &DescribeStmtNode{node: n}
}

func newShowStmtNode(n *ast.ShowStmtNode) *ShowStmtNode {
	return &ShowStmtNode{node: n}
}

func newBeginStmtNode(n *ast.BeginStmtNode) *BeginStmtNode {
	return &BeginStmtNode{node: n}
}

func newSetTransactionStmtNode(n *ast.SetTransactionStmtNode) *SetTransactionStmtNode {
	return &SetTransactionStmtNode{node: n}
}

func newCommitStmtNode(n *ast.CommitStmtNode) *CommitStmtNode {
	return &CommitStmtNode{node: n}
}

func newRollbackStmtNode(n *ast.RollbackStmtNode) *RollbackStmtNode {
	return &RollbackStmtNode{node: n}
}

func newStartBatchStmtNode(n *ast.StartBatchStmtNode) *StartBatchStmtNode {
	return &StartBatchStmtNode{node: n}
}

func newRunBatchStmtNode(n *ast.RunBatchStmtNode) *RunBatchStmtNode {
	return &RunBatchStmtNode{node: n}
}

func newAbortBatchStmtNode(n *ast.AbortBatchStmtNode) *AbortBatchStmtNode {
	return &AbortBatchStmtNode{node: n}
}

func newDropStmtNode(n *ast.DropStmtNode) *DropStmtNode {
	return &DropStmtNode{node: n}
}

func newDropMaterializedViewStmtNode(n *ast.DropMaterializedViewStmtNode) *DropMaterializedViewStmtNode {
	return &DropMaterializedViewStmtNode{node: n}
}

func newDropSnapshotTableStmtNode(n *ast.DropSnapshotTableStmtNode) *DropSnapshotTableStmtNode {
	return &DropSnapshotTableStmtNode{node: n}
}

func newRecursiveRefScanNode(n *ast.RecursiveRefScanNode) *RecursiveRefScanNode {
	return &RecursiveRefScanNode{node: n}
}

func newRecursiveScanNode(n *ast.RecursiveScanNode) *RecursiveScanNode {
	return &RecursiveScanNode{node: n}
}

func newWithScanNode(n *ast.WithScanNode) *WithScanNode {
	return &WithScanNode{node: n}
}

func newWithEntryNode(n *ast.WithEntryNode) *WithEntryNode {
	return &WithEntryNode{node: n}
}

func newOptionNode(n *ast.OptionNode) *OptionNode {
	return &OptionNode{node: n}
}

func newWindowPartitioningNode(n *ast.WindowPartitioningNode) *WindowPartitioningNode {
	return &WindowPartitioningNode{node: n}
}

func newWindowOrderingNode(n *ast.WindowOrderingNode) *WindowOrderingNode {
	return &WindowOrderingNode{node: n}
}

func newWindowFrameNode(n *ast.WindowFrameNode) *WindowFrameNode {
	return &WindowFrameNode{node: n}
}

func newAnalyticFunctionGroupNode(n *ast.AnalyticFunctionGroupNode) *AnalyticFunctionGroupNode {
	return &AnalyticFunctionGroupNode{node: n}
}

func newWindowFrameExprNode(n *ast.WindowFrameExprNode) *WindowFrameExprNode {
	return &WindowFrameExprNode{node: n}
}

func newDMLValueNode(n *ast.DMLValueNode) *DMLValueNode {
	return &DMLValueNode{node: n}
}

func newDMLDefaultNode(n *ast.DMLDefaultNode) *DMLDefaultNode {
	return &DMLDefaultNode{node: n}
}

func newAssertStmtNode(n *ast.AssertStmtNode) *AssertStmtNode {
	return &AssertStmtNode{node: n}
}

func newAssertRowsModifiedNode(n *ast.AssertRowsModifiedNode) *AssertRowsModifiedNode {
	return &AssertRowsModifiedNode{node: n}
}

func newInsertRowNode(n *ast.InsertRowNode) *InsertRowNode {
	return &InsertRowNode{node: n}
}

func newInsertStmtNode(n *ast.InsertStmtNode) *InsertStmtNode {
	return &InsertStmtNode{node: n}
}

func newDeleteStmtNode(n *ast.DeleteStmtNode) *DeleteStmtNode {
	return &DeleteStmtNode{node: n}
}

func newUpdateItemNode(n *ast.UpdateItemNode) *UpdateItemNode {
	return &UpdateItemNode{node: n}
}

func newUpdateArrayItemNode(n *ast.UpdateArrayItemNode) *UpdateArrayItemNode {
	return &UpdateArrayItemNode{node: n}
}

func newUpdateStmtNode(n *ast.UpdateStmtNode) *UpdateStmtNode {
	return &UpdateStmtNode{node: n}
}

func newMergeWhenNode(n *ast.MergeWhenNode) *MergeWhenNode {
	return &MergeWhenNode{node: n}
}

func newMergeStmtNode(n *ast.MergeStmtNode) *MergeStmtNode {
	return &MergeStmtNode{node: n}
}

func newTruncateStmtNode(n *ast.TruncateStmtNode) *TruncateStmtNode {
	return &TruncateStmtNode{node: n}
}

func newObjectUnitNode(n *ast.ObjectUnitNode) *ObjectUnitNode {
	return &ObjectUnitNode{node: n}
}

func newPrivilegeNode(n *ast.PrivilegeNode) *PrivilegeNode {
	return &PrivilegeNode{node: n}
}

func newGrantStmtNode(n *ast.GrantStmtNode) *GrantStmtNode {
	return &GrantStmtNode{node: n}
}

func newRevokeStmtNode(n *ast.RevokeStmtNode) *RevokeStmtNode {
	return &RevokeStmtNode{node: n}
}

func newAlterDatabaseStmtNode(n *ast.AlterDatabaseStmtNode) *AlterDatabaseStmtNode {
	return &AlterDatabaseStmtNode{node: n}
}

func newAlterMaterializedViewStmtNode(n *ast.AlterMaterializedViewStmtNode) *AlterMaterializedViewStmtNode {
	return &AlterMaterializedViewStmtNode{node: n}
}

func newAlterSchemaStmtNode(n *ast.AlterSchemaStmtNode) *AlterSchemaStmtNode {
	return &AlterSchemaStmtNode{node: n}
}

func newAlterTableStmtNode(n *ast.AlterTableStmtNode) *AlterTableStmtNode {
	return &AlterTableStmtNode{node: n}
}

func newAlterViewStmtNode(n *ast.AlterViewStmtNode) *AlterViewStmtNode {
	return &AlterViewStmtNode{node: n}
}

func newSetOptionsActionNode(n *ast.SetOptionsActionNode) *SetOptionsActionNode {
	return &SetOptionsActionNode{node: n}
}

func newAddColumnActionNode(n *ast.AddColumnActionNode) *AddColumnActionNode {
	return &AddColumnActionNode{node: n}
}

func newAddConstraintActionNode(n *ast.AddConstraintActionNode) *AddConstraintActionNode {
	return &AddConstraintActionNode{node: n}
}

func newDropConstraintActionNode(n *ast.DropConstraintActionNode) *DropConstraintActionNode {
	return &DropConstraintActionNode{node: n}
}

func newDropPrimaryKeyActionNode(n *ast.DropPrimaryKeyActionNode) *DropPrimaryKeyActionNode {
	return &DropPrimaryKeyActionNode{node: n}
}

func newAlterColumnOptionsActionNode(n *ast.AlterColumnOptionsActionNode) *AlterColumnOptionsActionNode {
	return &AlterColumnOptionsActionNode{node: n}
}

func newAlterColumnDropNotNullActionNode(n *ast.AlterColumnDropNotNullActionNode) *AlterColumnDropNotNullActionNode {
	return &AlterColumnDropNotNullActionNode{node: n}
}

func newAlterColumnSetDataTypeActionNode(n *ast.AlterColumnSetDataTypeActionNode) *AlterColumnSetDataTypeActionNode {
	return &AlterColumnSetDataTypeActionNode{node: n}
}

func newAlterColumnSetDefaultActionNode(n *ast.AlterColumnSetDefaultActionNode) *AlterColumnSetDefaultActionNode {
	return &AlterColumnSetDefaultActionNode{node: n}
}

func newAlterColumnDropDefaultActionNode(n *ast.AlterColumnDropDefaultActionNode) *AlterColumnDropDefaultActionNode {
	return &AlterColumnDropDefaultActionNode{node: n}
}

func newDropColumnActionNode(n *ast.DropColumnActionNode) *DropColumnActionNode {
	return &DropColumnActionNode{node: n}
}

func newRenameColumnActionNode(n *ast.RenameColumnActionNode) *RenameColumnActionNode {
	return &RenameColumnActionNode{node: n}
}

func newSetAsActionNode(n *ast.SetAsActionNode) *SetAsActionNode {
	return &SetAsActionNode{node: n}
}

func newSetCollateClauseNode(n *ast.SetCollateClauseNode) *SetCollateClauseNode {
	return &SetCollateClauseNode{node: n}
}

func newAlterTableSetOptionsStmtNode(n *ast.AlterTableSetOptionsStmtNode) *AlterTableSetOptionsStmtNode {
	return &AlterTableSetOptionsStmtNode{node: n}
}

func newRenameStmtNode(n *ast.RenameStmtNode) *RenameStmtNode {
	return &RenameStmtNode{node: n}
}

func newCreatePrivilegeRestrictionStmtNode(n *ast.CreatePrivilegeRestrictionStmtNode) *CreatePrivilegeRestrictionStmtNode {
	return &CreatePrivilegeRestrictionStmtNode{node: n}
}

func newCreateRowAccessPolicyStmtNode(n *ast.CreateRowAccessPolicyStmtNode) *CreateRowAccessPolicyStmtNode {
	return &CreateRowAccessPolicyStmtNode{node: n}
}

func newDropPrivilegeRestrictionStmtNode(n *ast.DropPrivilegeRestrictionStmtNode) *DropPrivilegeRestrictionStmtNode {
	return &DropPrivilegeRestrictionStmtNode{node: n}
}

func newDropRowAccessPolicyStmtNode(n *ast.DropRowAccessPolicyStmtNode) *DropRowAccessPolicyStmtNode {
	return &DropRowAccessPolicyStmtNode{node: n}
}

func newDropSearchIndexStmtNode(n *ast.DropSearchIndexStmtNode) *DropSearchIndexStmtNode {
	return &DropSearchIndexStmtNode{node: n}
}

func newGrantToActionNode(n *ast.GrantToActionNode) *GrantToActionNode {
	return &GrantToActionNode{node: n}
}

func newRestrictToActionNode(n *ast.RestrictToActionNode) *RestrictToActionNode {
	return &RestrictToActionNode{node: n}
}

func newAddToRestricteeListActionNode(n *ast.AddToRestricteeListActionNode) *AddToRestricteeListActionNode {
	return &AddToRestricteeListActionNode{node: n}
}

func newRemoveFromRestricteeListActionNode(n *ast.RemoveFromRestricteeListActionNode) *RemoveFromRestricteeListActionNode {
	return &RemoveFromRestricteeListActionNode{node: n}
}

func newFilterUsingActionNode(n *ast.FilterUsingActionNode) *FilterUsingActionNode {
	return &FilterUsingActionNode{node: n}
}

func newRevokeFromActionNode(n *ast.RevokeFromActionNode) *RevokeFromActionNode {
	return &RevokeFromActionNode{node: n}
}

func newRenameToActionNode(n *ast.RenameToActionNode) *RenameToActionNode {
	return &RenameToActionNode{node: n}
}

func newAlterPrivilegeRestrictionStmtNode(n *ast.AlterPrivilegeRestrictionStmtNode) *AlterPrivilegeRestrictionStmtNode {
	return &AlterPrivilegeRestrictionStmtNode{node: n}
}

func newAlterRowAccessPolicyStmtNode(n *ast.AlterRowAccessPolicyStmtNode) *AlterRowAccessPolicyStmtNode {
	return &AlterRowAccessPolicyStmtNode{node: n}
}

func newAlterAllRowAccessPoliciesStmtNode(n *ast.AlterAllRowAccessPoliciesStmtNode) *AlterAllRowAccessPoliciesStmtNode {
	return &AlterAllRowAccessPoliciesStmtNode{node: n}
}

func newCreateConstantStmtNode(n *ast.CreateConstantStmtNode) *CreateConstantStmtNode {
	return &CreateConstantStmtNode{node: n}
}

func newCreateFunctionStmtNode(n *ast.CreateFunctionStmtNode) *CreateFunctionStmtNode {
	return &CreateFunctionStmtNode{node: n}
}

func newArgumentDefNode(n *ast.ArgumentDefNode) *ArgumentDefNode {
	return &ArgumentDefNode{node: n}
}

func newArgumentRefNode(n *ast.ArgumentRefNode) *ArgumentRefNode {
	return &ArgumentRefNode{node: n}
}

func newCreateTableFunctionStmtNode(n *ast.CreateTableFunctionStmtNode) *CreateTableFunctionStmtNode {
	return &CreateTableFunctionStmtNode{node: n}
}

func newRelationArgumentScanNode(n *ast.RelationArgumentScanNode) *RelationArgumentScanNode {
	return &RelationArgumentScanNode{node: n}
}

func newArgumentListNode(n *ast.ArgumentListNode) *ArgumentListNode {
	return &ArgumentListNode{node: n}
}

func newFunctionSignatureHolderNode(n *ast.FunctionSignatureHolderNode) *FunctionSignatureHolderNode {
	return &FunctionSignatureHolderNode{node: n}
}

func newDropFunctionStmtNode(n *ast.DropFunctionStmtNode) *DropFunctionStmtNode {
	return &DropFunctionStmtNode{node: n}
}

func newDropTableFunctionStmtNode(n *ast.DropTableFunctionStmtNode) *DropTableFunctionStmtNode {
	return &DropTableFunctionStmtNode{node: n}
}

func newCallStmtNode(n *ast.CallStmtNode) *CallStmtNode {
	return &CallStmtNode{node: n}
}

func newImportStmtNode(n *ast.ImportStmtNode) *ImportStmtNode {
	return &ImportStmtNode{node: n}
}

func newModuleStmtNode(n *ast.ModuleStmtNode) *ModuleStmtNode {
	return &ModuleStmtNode{node: n}
}

func newAggregateHavingModifierNode(n *ast.AggregateHavingModifierNode) *AggregateHavingModifierNode {
	return &AggregateHavingModifierNode{node: n}
}

func newCreateMaterializedViewStmtNode(n *ast.CreateMaterializedViewStmtNode) *CreateMaterializedViewStmtNode {
	return &CreateMaterializedViewStmtNode{node: n}
}

func newCreateProcedureStmtNode(n *ast.CreateProcedureStmtNode) *CreateProcedureStmtNode {
	return &CreateProcedureStmtNode{node: n}
}

func newExecuteImmediateArgumentNode(n *ast.ExecuteImmediateArgumentNode) *ExecuteImmediateArgumentNode {
	return &ExecuteImmediateArgumentNode{node: n}
}

func newExecuteImmediateStmtNode(n *ast.ExecuteImmediateStmtNode) *ExecuteImmediateStmtNode {
	return &ExecuteImmediateStmtNode{node: n}
}

func newAssignmentStmtNode(n *ast.AssignmentStmtNode) *AssignmentStmtNode {
	return &AssignmentStmtNode{node: n}
}

func newCreateEntityStmtNode(n *ast.CreateEntityStmtNode) *CreateEntityStmtNode {
	return &CreateEntityStmtNode{node: n}
}

func newAlterEntityStmtNode(n *ast.AlterEntityStmtNode) *AlterEntityStmtNode {
	return &AlterEntityStmtNode{node: n}
}

func newPivotColumnNode(n *ast.PivotColumnNode) *PivotColumnNode {
	return &PivotColumnNode{node: n}
}

func newPivotScanNode(n *ast.PivotScanNode) *PivotScanNode {
	return &PivotScanNode{node: n}
}

func newReturningClauseNode(n *ast.ReturningClauseNode) *ReturningClauseNode {
	return &ReturningClauseNode{node: n}
}

func newUnpivotArgNode(n *ast.UnpivotArgNode) *UnpivotArgNode {
	return &UnpivotArgNode{node: n}
}

func newUnpivotScanNode(n *ast.UnpivotScanNode) *UnpivotScanNode {
	return &UnpivotScanNode{node: n}
}

func newCloneDataStmtNode(n *ast.CloneDataStmtNode) *CloneDataStmtNode {
	return &CloneDataStmtNode{node: n}
}

func newTableAndColumnInfoNode(n *ast.TableAndColumnInfoNode) *TableAndColumnInfoNode {
	return &TableAndColumnInfoNode{node: n}
}

func newAnalyzeStmtNode(n *ast.AnalyzeStmtNode) *AnalyzeStmtNode {
	return &AnalyzeStmtNode{node: n}
}

func newAuxLoadDataStmtNode(n *ast.AuxLoadDataStmtNode) *AuxLoadDataStmtNode {
	return &AuxLoadDataStmtNode{node: n}
}
