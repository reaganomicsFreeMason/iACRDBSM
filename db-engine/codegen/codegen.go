package codegen

import (
	"fmt"
	"iACRDBSM/db-engine/ast"
	"strconv"
)

const bigcap = 500

var insns []ByteCodeOp

/*GenByteCode :
Walks along the AST created by the SQL parser and generates
bytecode that will be executed by the virtual
machine to carry out the sql command. The bytecode langauge
is defined in codegen/ops.go
*/
func GenByteCode(stmt *ast.SqlStmt) ([]ByteCodeOp, error) {
	insns = make([]ByteCodeOp, 0, bigcap)
	if stmt.CreateTable != nil {
		visitCreateTable(*stmt.CreateTable)
	} else if stmt.Select != nil {
		visitSelect(*stmt.Select)
	} else if stmt.Insert != nil {
		visitInsert(*stmt.Insert)
	} else if stmt.Update != nil {
		visitUpdate(*stmt.Update)
	} else if stmt.Delete != nil {
		visitDelete(*stmt.Delete)
	} else if stmt.AlterTable != nil {
		visitAlterTable(*stmt.AlterTable)
	} else if stmt.DropTable != nil {
		visitDropTable(*stmt.DropTable)
	} else if stmt.TruncateTable != nil {
		visitTruncateTable(*stmt.TruncateTable)
	}
	insns = append(insns, ClearOp{})
	return insns, nil
}

// Compile a select statement into bytecode
func visitSelect(stmt ast.SelectStmt) {
	tableNames := stmt.TableNames
	// TODO: Handle joins
	insns = append(insns, GetTableOp{tableNames[0]})
	// Generate insns to add columns we want
	for _, colName := range stmt.ColNames {
		insns = append(insns, AddColumnOp{colName})
	}
	// Generate insns for conditions in WHERE clause
	visitConditions(stmt.Conditions)

	insns = append(insns, DisplayOp{})
}

func visitCreateTable(stmt ast.CreateTableStmt) {
	tableName := stmt.TableName
	colInfos := stmt.ColTypeInfos
	colNames := make([]string, 0, bigcap)
	colTypes := make([]string, 0, bigcap)
	for _, colInfo := range colInfos {
		colNames = append(colNames, colInfo.ColName)
		colTypes = append(colTypes, colInfo.ColType)
	}
	insns = append(insns, MakeTableOp{tableName, colNames, colTypes})
}

func visitInsert(stmt ast.InsertStmt) {
	tableName := stmt.TableName
	insns = append(insns, GetTableOp{tableName})
	insns = append(insns, InsertOp{stmt.ColNames, stmt.ValNames})
}

func visitUpdate(stmt ast.UpdateStmt) {
	tableName := stmt.TableName
	insns = append(insns, GetTableOp{tableName})

	// Want to filter out rows we dont first
	visitConditions(stmt.Conditions)

	// Then generate update table instructions
	for _, colSetVal := range stmt.ColSetVals {
		colName := colSetVal.ColName
		colVal := colSetVal.ColVal
		colValStr := castValToString(colVal)

		insns = append(insns, UpdateTableOp{colName, colValStr})
	}
}

func visitDelete(stmt ast.DeleteStmt) {
	tableName := stmt.TableName
	insns = append(insns, GetTableOp{tableName})

	// Want to filter out the rows from the working set
	// by the conditions
	// Want to filter out rows we dont first
	visitConditions(stmt.Conditions)

	// Now delete the remaining rows in the working set
	insns = append(insns, DeleteRowsOp{})
}

func visitAlterTable(stmt ast.AlterTableStmt) {
	tableName := stmt.TableName
	insns = append(insns, GetTableOp{tableName})
	if stmt.AlterExpr.AddColumnStmt != nil {
		visitAddColumn(*stmt.AlterExpr.AddColumnStmt)
	} else if stmt.AlterExpr.DropColumnStmt != nil {
		visitDropColumn(*stmt.AlterExpr.DropColumnStmt)
	}

}

func visitAddColumn(stmt ast.AddColumnStmt) {
	colName := stmt.ColTypeInfo.ColName
	colType := stmt.ColTypeInfo.ColType
	insns = append(insns, InsertColumnOp{colName, colType})
}

func visitDropColumn(stmt ast.DropColumnStmt) {
	colName := stmt.ColumnName
	insns = append(insns, DeleteColFromTableOp{colName})
}

func visitDropTable(stmt ast.DropTableStmt) {
	tableName := stmt.TableName
	insns = append(insns, GetTableOp{tableName})
	insns = append(insns, DeleteTableOp{tableName})
}

func visitTruncateTable(stmt ast.TruncateTableStmt) {
	tableName := stmt.TableName
	insns = append(insns, GetTableOp{tableName})
	insns = append(insns, DeleteRowsOp{})
}

func visitConditions(condList []*ast.EqCondition) {
	for _, cond := range condList {
		colName := cond.ColName
		colVal := cond.ColValue
		colValStr := castValToString(colVal)
		insns = append(insns, FilterOp{colName, colValStr})
	}
}

func castValToString(colValStruct *ast.ColValue) string {
	// This is silly, but the virtual machine
	// expects all values to be strings, so we cast our
	// value back to a string.
	var colVal string
	if colValStruct.String != nil {
		colVal = *colValStruct.String
	} else if colValStruct.Int != nil {
		colVal = strconv.Itoa(*colValStruct.Int)
	} else if colValStruct.Float != nil {
		colVal = fmt.Sprintf("%f", *colValStruct.Float)
	}
	return colVal
}
