package key_value

import (
	"sync"
)

var (
	initialNumTables = 10
)

// IntegerSet is a set of integers
type IntegerSet map[uint64]bool

// ValueToRowMap maps column values to the rows that hold them for a single column
type ValueToRowMap IntegerSet

// Row represents rows
type Row []interface{}

// ColumnInfoMap is a struct that holds information about a single column.
type ColumnInfoMap struct {
	Index  int
	Type   string                               // how we designate types; is one of the possible names
	Values map[SupportedValueType]ValueToRowMap // instantiate and does type checking for us
}

// DataTable is the basic struct that represents a single data table
type DataTable struct {
	ColumnsMap  map[string]ColumnInfoMap // set type data structure(maps item to True since set not in go)
	ColumnNames []string                 // deleted column is signified by empty string
	Rows        []Row
	DeletedRows IntegerSet
	NumCol      int
	l           sync.RWMutex
}

// DataBase data type
type DataBase struct {
	db map[string]*DataTable // map from name of the table to the table itself
	l  sync.RWMutex
}

// NewDataBase creates a new data base.
func NewDataBase() *DataBase {
	res := DataBase{}
	res.db = make(map[string]*DataTable, initialNumTables)
	res.l = sync.RWMutex{}
	return &res
}
