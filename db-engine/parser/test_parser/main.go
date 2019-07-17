package main

import (
	"fmt"
	"iACRDBSM/db-engine/parser"
)

func main() {
	parser.InitParser()
	ast := &parser.SelectStmt{}
	err := parser.SQLParser.ParseString("SELECT col1, col2, col3, FROM t1, t2, t3, WHERE col1 = val", ast)
	if err != nil {
		fmt.Println("Parse Error:" + err.Error())
	}
}
