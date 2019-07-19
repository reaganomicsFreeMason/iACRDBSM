package parser

//Relavant parser definitions and functions needed to define a simple SQL parser

import (
	"errors"
	"iACRDBSM/db-engine/ast"

	"github.com/alecthomas/participle"
)

//SQLParser -
var SQLParser *participle.Parser

/*InitParser -
Creates a parser with the simple SQL grammar defined above
*/
func InitParser() error {

	parser, parseErr := participle.Build(&ast.SqlStmt{})

	if parseErr != nil {
		parseErr := errors.New("Error creating parser:" + parseErr.Error())
		return parseErr
	}

	SQLParser = parser
	return nil
}

//ParseInput -
func ParseInput(sqlString string) (*ast.SqlStmt, error) {
	ast := &ast.SqlStmt{}
	parseErr := SQLParser.ParseString(sqlString, ast)
	if parseErr != nil {
		return nil, parseErr
	}
	return ast, nil
}
