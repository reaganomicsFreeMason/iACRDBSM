package key_value

var (
	initialNumTables = 10
)

type IntegerSet map[uint64]bool
type ValueToRowMap IntegerSet

type Row []interface{}

type ColumnInfoMap struct {
	Index  int
	Type   string                               // how we designate types; is one of the possible names
	Values map[SupportedValueType]ValueToRowMap // instantiate and does type checking for us
}

type DataTable struct {
	ColumnsMap  map[string]ColumnInfoMap // set type data structure(maps item to True since set not in go)
	ColumnNames []string
	Rows        []Row
	NumCol      int
}

// DataBase data type
type DataBase map[string]*DataTable // map from name of the table to the table itself

// NewDataBase creates a new data base.
func NewDataBase() *DataBase {
	res := make(DataBase, initialNumTables)
	return &res
}
