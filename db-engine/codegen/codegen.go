package codegen

import (
	"iACRDBSM/db-engine/parser"
)

const bigcap = 500

var insns []ByteCodeOp

/*GenByteCode :
Walks along the AST created by the SQL parser and generates
bytecode that will be executed by the virtual
machine to carry out the sql command. The bytecode langauge
is defined in codegen/ops.go
*/
func GenByteCode(stmt *parser.SqlStmt) ([]ByteCodeOp, error) {
	insns = make([]ByteCodeOp, 0, bigcap)
	if stmt.CreateTable != nil {
		visitCreateTable(*stmt.CreateTable)
	} else if stmt.Select != nil {
		visitSelect(*stmt.Select)
	} else if stmt.Insert != nil {
		visitInsert(*stmt.Insert)
	} else if stmt.Update != nil {
		visitUpdate(*stmt.Update)
	}
	return insns, nil
}

// Compile a select statement into bytecode
func visitSelect(stmt parser.SelectStmt) {
	tableNames := stmt.TableNames
	// TODO: Handle joins
	insns = append(insns, GetTableOp{tableNames[0]})
	// Generate insns to add columns we want
	for _, colName := range stmt.ColNames {
		insns = append(insns, AddColumnOp{colName})
	}
	// Generate insns for conditions in WHERE clause
	for _, cond := range stmt.Conditions {
		colName := cond.ColName
		valName := cond.ValName
		insns = append(insns, FilterOp{colName, valName})
	}

	insns = append(insns, DisplayOp{})
}

func visitCreateTable(stmt parser.CreateTableStmt) {
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

func visitInsert(stmt parser.InsertStmt) {
	tableName := stmt.TableName
	insns = append(insns, GetTableOp{tableName})
	insns = append(insns, InsertOp{stmt.ColNames, stmt.ValNames})
}

func visitUpdate(stmt parser.UpdateStmt) {
	tableName := stmt.TableName
	insns = append(insns, GetTableOp{tableName})

	// Want to filter out rows we dont first
	for _, cond := range stmt.Conditions {
		colName := cond.ColName
		valName := cond.ValName
		insns = append(insns, FilterOp{colName, valName})
	}

	// Then generate update table instructions
	for _, colSetVal := range stmt.ColSetVals {
		colName := colSetVal.ColName
		colVal := colSetVal.ColVal
		insns = append(insns, UpdateTableOp{colName, colVal})
	}
}
