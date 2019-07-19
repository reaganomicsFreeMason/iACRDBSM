package parser

//Relavant parser definitions and functions needed to define a simple SQL parser

import (
	"errors"

	"github.com/alecthomas/participle"
)

//////////////////////////////BEGINNING OF GRAMMAR/////////////////////////////

type SqlStmt struct {
	CreateTable *CreateTableStmt `"CREATE" @@`
	Select      *SelectStmt      `| "SELECT" @@`
	Insert      *InsertStmt      `| "INSERT" @@`
	Update      *UpdateStmt      `| "UPDATE" @@`
	Delete      *DeleteStmt      `| "DELETE" @@`
	AlterTable  *AlterTableStmt  `| "ALTER" @@`
}

/*CreateTableStmt -
 */
type CreateTableStmt struct {
	TableName    string         `"TABLE" @Ident`
	ColTypeInfos []*ColTypeInfo `"(" (@@",")+ ")"`
}

/*ColTypeInfo -
 */
type ColTypeInfo struct {
	ColName string `@Ident`
	ColType string `@Ident`
}

type ColValue struct {
	String *string  `  @String`
	Int    *int     `| @Int`
	Float  *float64 `| @Float`
}

/*SelectStmt -
 */
type SelectStmt struct {
	ColNames   []string       `(@Ident",")+`
	TableNames []string       `"FROM" (@Ident",")+`
	Conditions []*EqCondition `("WHERE" (@@",")+)?`
}

/*EqCondition -
 */
type EqCondition struct {
	ColName  string    `@Ident "="`
	ColValue *ColValue `@@`
}

/*InsertStmt -
 */
type InsertStmt struct {
	TableName string   `"INTO" @Ident`
	ColNames  []string `"(" (@Ident",")+ ")"`
	ValNames  []string `"VALUES" "(" (@Ident",")+ ")"`
}

type UpdateStmt struct {
	TableName  string         `@Ident`
	ColSetVals []*ColSetVal   `"SET" (@@",")+`
	Conditions []*EqCondition `("WHERE" (@@",")+ )?`
}

type ColSetVal struct {
	ColName string    `@Ident "="`
	ColVal  *ColValue `@@`
}

type DeleteStmt struct {
	TableName  string         `"FROM" @Ident`
	Conditions []*EqCondition `"WHERE" (@@",")+`
}

type AlterTableStmt struct {
	TableName string `"TABLE" @Ident`
	ColName   string `"ADD" @Ident`
	ColType   string `@Ident`
}

//////////////////////////////END OF GRAMMAR/////////////////////////////

//SQLParser -
var SQLParser *participle.Parser

/*InitParser -
Creates a parser with the simple SQL grammar defined above
*/
func InitParser() error {

	parser, parseErr := participle.Build(&SqlStmt{})

	if parseErr != nil {
		parseErr := errors.New("Error creating parser:" + parseErr.Error())
		return parseErr
	}

	SQLParser = parser
	return nil
}

//ParseInput -
func ParseInput(sqlString string) (*SqlStmt, error) {
	ast := &SqlStmt{}
	parseErr := SQLParser.ParseString(sqlString, ast)
	if parseErr != nil {
		return nil, parseErr
	}
	return ast, nil
}
