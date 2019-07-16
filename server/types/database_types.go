package types

type valueToRowMap integerSet

type row []interface{}

type columnInfoMap {
	index int
	values map[supportedValueType]valueToRowMap // instantiate and does type checking for us 
}

type dataTable {
	columnsMap map[string]columnInfoMap // set type data structure(maps item to True since set not in go) 
	columnNames []string
	rows []row
	numCol int
}

type DataBase map[string]*dataTable // map from name of the table to the table itself 


func NewDataBase() *DataBase {
	return &DataBase{}
}

func (db *NewDataBase) newTable(tableName string) error {
	if _, found = (*db)[tableName]; found {
		return errors.New("Db already exists, dumbass")
	} else {
		dataTable, err := makeDataTable()
		if err != nil {
			return errors.New("Table not made successfully")
		} else {
			(*db)[tableName] = dataTable
			return nil
		}
	}
}

func makeDataTable() *dataTable {
	columnMap = make(map[string]columnInfoMap, initNumCols)
	columnNames= make([]string, initNumCols)
	rows = make([]row, initNumRows)
	return &dataTable {
		columnMap,
		columnNames,
		rows,
	}
}

func (db *dataTable) deleteTable(tableName string) error {
	if _, err = db.getTable(tableName); err != nil {
		return errors.New("Table not here, dumbass.")
	} else {
		delete(*db, tableName)
		return nil
	}
}  

func (db *dataTable) getTable(tableName string) (dataTable, error) {
	if table, found = (*db)[tableName]; !found {
		return nil, errors.New("Table not here, dumbass.")
	} else {
		return table, nil
	}
}