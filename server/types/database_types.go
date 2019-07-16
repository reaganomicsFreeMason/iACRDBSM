package types

type valueToRowMap integerSet

type row []interface{}

type columnInfoMap struct {
	index  int
	values map[supportedValueType]valueToRowMap // instantiate and does type checking for us
}

type dataTable struct {
	columnsMap  map[string]columnInfoMap // set type data structure(maps item to True since set not in go)
	columnNames []string
	rows        []row
	numCol      int
}

// DataBase data type
type DataBase map[string]*dataTable // map from name of the table to the table itself

// NewDataBase creates a new data base.
func NewDataBase() *DataBase {
	return &DataBase{}
}
