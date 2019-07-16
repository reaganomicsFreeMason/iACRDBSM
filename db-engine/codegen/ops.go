package codegen

/* Defines a small bytecode language whose execution platform
is the virtual machine. The opcodes operate on registers.
*/

//Register - Used to store values
type Register struct {
	name string
}

//Value - Types that are of type Value can be stored in registers
type Value interface {
}

//IntVal - A value of type int
type IntVal struct {
	Value
	v int
}

//StrVal - A value of type str
type StrVal struct {
	Value
	v string
}

//TableVal - A DataTable from datastore
type TableVal struct {
	Value
	v DataTable
}

/*ByteCodeOp -
Empty base type for anything that is considered an instruction to
be executed on the virtual machine.
*/
type ByteCodeOp struct {
}

/*GetTableOp -
Retrieves a pointer to a value of type DataTable from datastore
*/
type GetTableOp struct {
	ByteCodeOp
	tablename string
	// retval: table, TableVal
}

/*FilterOp -
Filters out
*/
type FilterOp struct {
	ByteCodeOp
	table TableVal
}
