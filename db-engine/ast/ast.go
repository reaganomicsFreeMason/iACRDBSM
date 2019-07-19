package ast

// This file defines how an AST should be generated from a grammar
// via the parser generator library we are using (participle)

type SqlStmt struct {
	CreateTable   *CreateTableStmt   `"CREATE" @@`
	Select        *SelectStmt        `| "SELECT" @@`
	Insert        *InsertStmt        `| "INSERT" @@`
	Update        *UpdateStmt        `| "UPDATE" @@`
	Delete        *DeleteStmt        `| "DELETE" @@`
	AlterTable    *AlterTableStmt    `| "ALTER" @@`
	DropTable     *DropTableStmt     `| "DROP" @@`
	TruncateTable *TruncateTableStmt `| "TRUNCATE" @@`
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
	TableName  string         `"FROM" @Ident`
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
	TableName string     `"TABLE" @Ident`
	AlterExpr *AlterExpr `@@`
}

type AlterExpr struct {
	DropColumnStmt *DropColumnStmt `  "DROP"  @@`
	AddColumnStmt  *AddColumnStmt  `| "ADD" @@`
}

type DropColumnStmt struct {
	ColumnName string `"COLUMN" @Ident`
}

type AddColumnStmt struct {
	ColTypeInfo *ColTypeInfo `@@`
}

type DropTableStmt struct {
	TableName string `"TABLE" @Ident`
}

type TruncateTableStmt struct {
	TableName string `"TABLE" @Ident`
}
