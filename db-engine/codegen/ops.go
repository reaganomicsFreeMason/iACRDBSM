package codegen

import (
	"iACRDBSM/db-engine/datastore/key_value"
)

/* Defines a small bytecode language whose execution platform
is the virtual machine. The opcodes operate on registers.
*/

//Register - Used to store values
type Register *interface{}

const (
	R1 = 1
	R2 = 2
	R3 = 3
	R4 = 4
	R5 = 5 // Pointer to table
	R6 = 6 // List of column names we want in query result table
	R7 = 7 // List of row indexes we want in query result table
	// have a special value in the row registers that starts with all rows
)

/*ByteCodeOp -
Empty base type for anything that is considered an instruction to
be executed on the virtual machine.
*/
type ByteCodeOp interface {
}

/*GetTableOp -
Loads a pointer to a DataTable with tablename into R5
*/
type GetTableOp struct {
	// Name of table to be retrieved
	tablename string
}

/*AddColumnOp -
 */
type AddColumnOp struct {
	// Column name to add to the query table result
	colname string
}

/*AddRowOp -
 */
type AddRowOp struct {
	//Index of row to add to the query table result
	idx uint32
}

type WhereOp struct {
	colname string
	value   key_value.SupportedValueType
}
