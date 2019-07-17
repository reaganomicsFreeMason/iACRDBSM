package cores

import (
	"iACRDBSM/db-engine/codegen"
	"iACRDBSM/db-engine/parser"
)

/*ProcessSQLString :
This function defines the pipeline a SQL command string goes through
to interact with iACRDBSM. A rough overview is as follows:
1.) The command string is parsed into an AST (Done)
2.) An execution plan for the command is generated in the form of bytecode from the AST (TODO)
3.) The bytecode is executed on the bytecode virtual machine (TODO)
*/
func ProcessSQLString(sqlstr string) (string, error) {
	//Parse input string into an AST
	ast, parseErr := parseInput(sqlstr)
	if parseErr != nil {
		return "", parseErr
	}

	//Generate execution plan in bytecode from AST (TODO)
	insns, codeGenErr := codegen.GenByteCode(ast)
	if codeGenErr != nil {
		return "", codeGenErr
	}

	//Exectue bytecode on virtual machine and return results (TODO)
	results, execErr := execByteCode(insns)
	if execErr != nil {
		return "", execErr
	}

	return results, nil
}

// TODO: Probably move this to a parse file with any other parse stuff we need in the future
func parseInput(sqlString string) (*parser.SelectStmt, error) {
	ast := &parser.SelectStmt{}
	parseErr := parser.SQLParser.ParseString(sqlString, ast)
	if parseErr != nil {
		return nil, parseErr
	}
	return ast, nil
}
