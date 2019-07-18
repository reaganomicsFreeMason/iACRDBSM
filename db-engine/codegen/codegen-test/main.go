package main

import (
	"fmt"
	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/core"
	"iACRDBSM/db-engine/parser"
	"os"
)

func main() {
	testQuery := "SELECT a, FROM TestTable,"
	parser.InitParser()
	ast, err := core.ParseInput(testQuery)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	bytecode, err := codegen.GenByteCode(ast)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	for _, insn := range bytecode {
		fmt.Println(insn.GetOpName())
	}

}
