package parser

import (
	"fmt"
	"iACRDBSM/db-engine/ast"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

func setup() {
	err := InitParser()
	if err != nil {
		fmt.Println("Error creating parser: " + err.Error())
		os.Exit(1)
	}
}

func TestSelect(t *testing.T) {
	InitParser()
	ast := returnAST(t, "SELECT col1, col2, col3, FROM t1 WHERE col1 = \"v1\", col2 = \"v2\", col3 = 2,")
	// Print column names
	assert.Equal(t, "t1", ast.Select.TableName)
	assert.Equal(t, "col2", ast.Select.ColNames[1])
	assert.Equal(t, 2, *ast.Select.Conditions[2].ColValue.Int)
}

func returnAST(t *testing.T, sqlStmt string) *ast.SqlStmt {
	ast := &ast.SqlStmt{}
	err := SQLParser.ParseString(sqlStmt, ast)
	if err != nil {
		t.Log("Parse Error " + err.Error())
	}
	return ast
}

func TestCreateTable(t *testing.T) {
	ast := returnAST(t, "CREATE TABLE name (col1 int, col2 float, col3 string)")
	assert.Equal(t, "name", ast.CreateTable.TableName, "Tablename name doesn't match")
	assert.Equal(t, "col1", ast.CreateTable.ColTypeInfos[0].ColName)
	assert.Equal(t, "int", ast.CreateTable.ColTypeInfos[0].ColType)
}

func TestInsert(t *testing.T) {
	ast := returnAST(t, "INSERT INTO tablename (col1, col2, col3,) VALUES (1, 2, 3,)")
	assert.Equal(t, "tablename", ast.Insert.TableName)
	assert.Equal(t, "col1", ast.Insert.ColNames[0])
	assert.Equal(t, 1, *ast.Insert.Vals[0].Int)
}

func TestUpdate(t *testing.T) {
	ast := returnAST(t, "UPDATE tablename SET col1 = 2, col2 = 3, col3 = 4, WHERE col1 = 3,")
	assert.Equal(t, "tablename", ast.Update.TableName)
}

func TestDelete(t *testing.T) {
	ast := returnAST(t, "DELETE FROM t WHERE c = 2,")
	assert.Equal(t, "t", ast.Delete.TableName)
	assert.Equal(t, "c", ast.Delete.Conditions[0].ColName)
	assert.Equal(t, 2, *ast.Delete.Conditions[0].ColValue.Int)
}

func TestAlterTableAdd(t *testing.T) {
	ast := returnAST(t, "ALTER TABLE t ADD c int")
	assert.Equal(t, "t", ast.AlterTable.TableName)
	assert.Equal(t, "c", ast.AlterTable.AlterExpr.AddColumnStmt.ColTypeInfo.ColName)
	assert.Equal(t, "int", ast.AlterTable.AlterExpr.AddColumnStmt.ColTypeInfo.ColType)
}

func TestAlterTableDrop(t *testing.T) {
	ast := returnAST(t, "ALTER TABLE t DROP COLUMN c")
	assert.Equal(t, "t", ast.AlterTable.TableName)
	assert.Equal(t, "c", ast.AlterTable.AlterExpr.DropColumnStmt.ColumnName)
}

func TestDropTableOp(t *testing.T) {
	ast := returnAST(t, "DROP TABLE t")
	assert.Equal(t, "t", ast.DropTable.TableName)
}

func TestTruncateTableOp(t *testing.T) {
	ast := returnAST(t, "TRUNCATE TABLE t")
	assert.Equal(t, "t", ast.TruncateTable.TableName)
}

func TestSelectStarOp(t *testing.T) {
	ast := returnAST(t, "SELECT ALL, FROM t")
	assert.Equal(t, "ALL", ast.Select.ColNames[0])
}
