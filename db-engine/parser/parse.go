package parse

import (
	"strings"
)

type Stmt interface {
}

type SelectStmt struct {
	Stmt
	tablenames []string
	colnames   []string
	// TODO: Handle WHERE clause
}

func sqlCommand(sqlString string) SelectStmt {
	// Assumption: the command is a SELECT commands
	strings.SplitAfterN(sqlString, "SELECT", 2)
}
