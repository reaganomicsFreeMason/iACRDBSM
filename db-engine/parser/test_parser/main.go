package main

import (
	"fmt"
	"iACRDBSM/db-engine/parser"
)

func main() {
	parser.InitParser()
	ast := &parser.SelectStmt{}
	err := parser.SQLParser.ParseString("SELECT col1, col2, col3, FROM t1, t2, t3, WHERE col1 = v1, col2 = v2, col3 = v3,", ast)
	if err != nil {
		fmt.Println("Parse Error:" + err.Error())
	}

	// Print column names
	for _, col := range ast.ColNames {
		fmt.Println(col)
	}

	// Print table names
	for _, tbl := range ast.TableNames {
		fmt.Println(tbl)
	}

	// Print where conditions
	for _, cond := range ast.Conditions {
		fmt.Println(cond.ColName + "=" + cond.ValName)
	}
}
