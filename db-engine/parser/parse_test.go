package parser

import (
	"fmt"
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
	ast := &SqlStmt{}
	err := SQLParser.ParseString("SELECT col1, col2, col3, FROM t1, t2, t3, WHERE col1 = v1, col2 = v2, col3 = v3,", ast)
	assert.NoError(t, err, "Parse Error")
	// Print column names
	for _, col := range ast.Select.ColNames {
		fmt.Println(col)
		t.Log(col)
	}

	// Print table names
	for _, tbl := range ast.Select.TableNames {
		fmt.Println(tbl)
		t.Log(tbl)
	}

	// Print where conditions
	for _, cond := range ast.Select.Conditions {
		fmt.Println(cond.ColName + "=" + cond.ValName)
		t.Log(cond)
	}
}

func TestCreateTable(t *testing.T) {
	ast := &SqlStmt{}
	_ = SQLParser.ParseString("CREATE TABLE name (col1 int, col2 float, col3 string)", ast)
	assert.Equal(t, "name", ast.CreateTable.TableName, "Tablename name doesn't match")
	assert.Equal(t, "col1", ast.CreateTable.ColInfos[0].ColName)
	assert.Equal(t, "int", ast.CreateTable.ColInfos[0].ColType)
}

func TestInsert(t *testing.T) {
	ast := &SqlStmt{}
	_ = SQLParser.ParseString("INSERT INTO tablename (col1, col2, col3,) VALUES (v1, v2, v3,)", ast)
	assert.Equal(t, "tablename", ast.Insert.TableName)
	assert.Equal(t, "col1", ast.Insert.ColNames[0])
	assert.Equal(t, "v1", ast.Insert.ValNames[0])
}
