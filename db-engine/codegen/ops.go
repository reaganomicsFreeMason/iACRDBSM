package codegen

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
	GetOpName() string
}

/*GetTableOp -
Loads a pointer to a DataTable with tablename into R5
*/
type GetTableOp struct {
	// Name of table to be retrieved
	Tablename string
}

func (o GetTableOp) GetOpName() string {
	return "GetTableOp"
}

/*AddColumnOp -
 */
type AddColumnOp struct {
	// Column name to add to the query table result
	Colname string
}

func (o AddColumnOp) GetOpName() string {
	return "AddColumnOp"
}

/*AddRowOp -
 */
type AddRowOp struct {
	//Index of row to add to the query table result
	Idx uint32
}

func (o AddRowOp) GetOpName() string {
	return "AddRowOp"
}

/*FilterOp -
 */
type FilterOp struct {
	ColName string
	ValName string
}

func (o FilterOp) GetOpName() string {
	return "FilterOp"
}

type InsertOp struct {
	ColNames []string
	ValNames []string // corresponding values to the columns as strings
}

func (o InsertOp) GetOpName() string {
	return "InsertOp"
}

type MakeTableOp struct {
	TableName string
	ColNames  []string
	ColTypes  []string // corresponding values to the columns as strings
}

func (o MakeTableOp) GetOpName() string {
	return "MakeTableOp"
}

type DeleteTableOp struct {
	TableName string
}

func (o DeleteTableOp) GetOpName() string {
	return "DeleteTableOp"
}

// UPDATE: look at rows and cols in the regs currently
// replace all the cols with the values given as inputs
type UpdateTableOp struct {
	ColName string
	NewVal  string // new vals given as strings
}

func (o UpdateTableOp) GetOpName() string {
	return "UpdateTableOp"
}

type DeleteRowsOp struct{}

func (o DeleteRowsOp) GetOpName() string {
	return "DeleteRowsOp"
}

type DeleteColsOp struct{}

func (o DeleteColsOp) GetOpName() string {
	return "DeleteColsOp"
}

type DisplayOp struct{}

func (o DisplayOp) GetOpName() string {
	return "DisplayOp"
}

type InsertColumnOp struct {
	ColName string
	ColType string //int, float, or string
}

func (o InsertColumnOp) GetOpName() string {
	return "InsertColumnOp"
}

// ALTER TABLE TO DO
