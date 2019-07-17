package parser

//Relavant parser definitions and functions needed to define a simple SQL parser

import (
	"errors"

	"github.com/alecthomas/participle"
)

/*SelectStmt -
 */
type SelectStmt struct {
	ColNames   []*ColName   `"SELECT" (@@)+ "FROM"`
	TableNames []*TableName `(@@)+`
	// TODO: Handle WHERE clause
}

/*ColName -
 */
type ColName struct {
	ColName string `@Ident","`
}

/*TableName -
 */
type TableName struct {
	TableName string `@Ident","`
}

var SqlParser *participle.Parser

/*InitParser -
Creates a parser with the simple SQL grammar defined above
*/
func InitParser() error {

	parser, parseErr := participle.Build(&SelectStmt{})

	if parseErr != nil {
		parseErr := errors.New("Error creating parser:" + parseErr.Error())
		return parseErr
	}

	SqlParser = parser
	return nil
}
