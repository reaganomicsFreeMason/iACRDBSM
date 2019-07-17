package parser

//Relavant parser definitions and functions needed to define a simple SQL parser

import (
	"errors"

	"github.com/alecthomas/participle"
)

//////////////////////////////BEGINNING OF GRAMMAR/////////////////////////////

/*SelectStmt -
 */
type SelectStmt struct {
	ColNames   []*string      `"SELECT" (@Ident",")+`
	TableNames []*string      `"FROM" (@Ident",")+`
	Conditions []*EqCondition `("WHERE" (@@)+)?`
}

/*InCondition -
 */
type EqCondition struct {
	ColName string `@Ident "="`
	ValName string `@Ident`
}

//////////////////////////////END OF GRAMMAR/////////////////////////////

//SQLParser -
var SQLParser *participle.Parser

/*InitParser -
Creates a parser with the simple SQL grammar defined above
*/
func InitParser() error {

	parser, parseErr := participle.Build(&SelectStmt{})

	if parseErr != nil {
		parseErr := errors.New("Error creating parser:" + parseErr.Error())
		return parseErr
	}

	SQLParser = parser
	return nil
}
