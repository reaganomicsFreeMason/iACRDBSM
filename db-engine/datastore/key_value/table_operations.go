package key_value

import (
	"errors"
	"sync"
)

var (
	initialInfoValueSize = 10
	initNumCols          = 10
	initNumRows          = 10
	initDefaultSize      = 10
)

// bug: numcol not being edited

// TODO(lgong): condition variables on the locks to decrease busy waiting

// NewTable creates a new DataTable of name tableName in database if it does not exist already.
// The new DataTable will have columns columnNames of types columnTypes. There must be as many columnTypes as there are column names.
func (database *DataBase) NewTable(
	tableName string,
	columnNames,
	columnTypes []string,
) error {
	if len(columnNames) < len(columnTypes) {
		return errors.New("less column names specified than column types")
	} else if len(columnNames) > len(columnTypes) {
		return errors.New("less column types specified than column names")
	}

	database.l.Lock() //writer lock on database
	defer database.l.Unlock()

	if _, found := (database.db)[tableName]; found {
		return errors.New("db already exists")
	}
	dataTable, err := makeDataTable(columnNames, columnTypes)
	if err != nil {
		return errors.New("Table not made successfully")
	}
	(database.db)[tableName] = dataTable
	return nil

}

func makeDataTable(columnNames, columnTypes []string) (*DataTable, error) {
	columnMap := make(map[string]ColumnInfoMap, initNumCols)

	// initialize map
	for i, name := range columnNames {
		typeName := columnTypes[i]
		if name == "" || typeName == "" {
			return nil, errors.New("user put in an empty column")
		}
		columnMap[name] = ColumnInfoMap{
			i,
			typeName,
			make(map[SupportedValueType]ValueToRowMap, initialInfoValueSize),
		}
	}
	rows := make([]Row, 0, initNumRows)

	return &DataTable{
		columnMap,
		columnNames,
		rows,
		make(IntegerSet, initDefaultSize),
		len(columnNames),
		sync.RWMutex{},
	}, nil
}

// DeleteTable deletes a table from the database if it exists.
func (database *DataBase) DeleteTable(tableName string) error {
	database.l.Lock() //writer lock on database
	defer database.l.Unlock()

	table, found := (database.db)[tableName]

	if !found {
		return errors.New("table not here")
	}

	table.l.Lock() //writer lock on datatable
	defer table.l.Unlock()

	delete(database.db, tableName)
	return nil
}

// GetTable gets a table from the database
func (database *DataBase) GetTable(tableName string) (*DataTable, error) {
	database.l.RLock() //reader lock on database
	defer database.l.RUnlock()

	table, found := (database.db)[tableName]

	if !found {
		return nil, errors.New("table does not exist")
	}
	return table, nil

}

// PutColumn adds a column to datatable dt
func (dt *DataTable) PutColumn(columnName string, columnType string) error {
	if columnName == "" {
		return errors.New("you can't put a column without a columname")
	}

	dt.l.Lock() //datatable writer lock
	defer dt.l.Unlock()

	dt.columnsMap[columnName] = ColumnInfoMap{
		len(dt.columnNames),
		columnType,
		make(map[SupportedValueType]ValueToRowMap, initialInfoValueSize),
	}
	dt.columnNames = append(dt.columnNames, columnName)
	return nil
}

// UpdateRow updates the entry at row at index rowIndex and column colName with a new value
func (dt *DataTable) UpdateRow(rowIndex uint64, colName string, newValue SupportedValueType) error {
	if colName == "" {
		return errors.New("can't update entry. column name is empty string")
	}

	dt.l.Lock() //writer lock on database
	defer dt.l.Unlock()

	if _, found := dt.columnsMap[colName]; !found {
		return errors.New("No column there;  ")
	}
	if _, found := dt.deletedRows[rowIndex]; found {
		return errors.New("Row was deleted  ")
	}
	colIndex := dt.columnsMap[colName].Index
	rowLen := len(dt.rows[rowIndex])
	var prevValue interface{}
	for i := rowLen; i < colIndex; i++ {
		dt.rows[rowIndex] = append(dt.rows[rowIndex], nil)
	}
	if colIndex >= len(dt.rows[rowIndex]) {
		dt.rows[rowIndex] = append(dt.rows[rowIndex], nil)
	} else {
		// get and remove the old value in the case that it exists
		prevValue = dt.rows[rowIndex][colIndex]
		if prevValue != nil {
			prevValue2 := prevValue.(SupportedValueType)
			delete(dt.columnsMap[colName].Values[prevValue2], rowIndex)
			if len(dt.columnsMap[colName].Values[prevValue2]) == 0 {
				delete(dt.columnsMap[colName].Values, prevValue2)
			}
		}
	}

	// then update the actual column; TODO type checking elsewhere or here?
	dt.rows[rowIndex][colIndex] = newValue

	// now update corresponding maps
	_, found := dt.columnsMap[colName].Values[newValue]
	if !found {
		dt.columnsMap[colName].Values[newValue] = ValueToRowMap{}
	}
	dt.columnsMap[colName].Values[newValue][rowIndex] = true
	return nil
}

// DeleteColumn deletes column columnName
func (dt *DataTable) DeleteColumn(columnName string) error { // we should make sure this is not empty
	if columnName == "" {
		return errors.New("cannot delete column. provided column name is empty string")
	}

	dt.l.Lock() //writer lock on datatable
	defer dt.l.Unlock()

	if _, found := dt.columnsMap[columnName]; !found {
		return errors.New("No column there ")
	}
	columnInd := dt.columnsMap[columnName].Index
	delete(dt.columnsMap, columnName)
	dt.columnNames[columnInd] = "" // signifies that has been deleted
	return nil
}

//DeleteRow deletes row at index rowIndex
func (dt *DataTable) DeleteRow(rowIndex uint64) error {
	dt.l.Lock() //writer lock on datatable
	defer dt.l.Unlock()

	if int(rowIndex) >= len(dt.rows) {
		return errors.New("Row doesn't exist")
	} else if _, found := dt.deletedRows[rowIndex]; found {
		return errors.New("Row already deleted")
	}
	dt.deletedRows[rowIndex] = true
	for ind, colValue := range dt.rows[rowIndex] {
		colName := dt.columnNames[ind]
		if colValue == nil {
			continue
		}
		delete(dt.columnsMap[colName].Values[colValue.(SupportedValueType)], rowIndex)
		if len(dt.columnsMap[colName].Values[colValue.(SupportedValueType)]) == 0 {
			delete(dt.columnsMap[colName].Values, colValue.(SupportedValueType))
		}
	}
	return nil
}

// PutRow adds a new row to the datatable dt
func (dt *DataTable) PutRow(row Row) error {
	dt.l.Lock() //writer lock on datatable
	defer dt.l.Unlock()

	// should prob do some sort of error checking
	// assume for now this row is valid at input
	// just assume that deleted columns are silly

	// fmt.Println(row, "gonna, put in ")
	rowIndex := len(dt.rows)
	dt.rows = append(dt.rows, row)
	dt.rows[rowIndex] = row
	for ind, colVal := range row {
		if colVal == nil {
			continue
		}
		colName := dt.columnNames[ind]
		if colName == "" {
			continue
		}
		// TODO: break up and make temp variables.
		if _, found := dt.columnsMap[colName].Values[colVal.(SupportedValueType)]; !found {
			dt.columnsMap[colName].Values[colVal.(SupportedValueType)] = ValueToRowMap{}
		}
		dt.columnsMap[colName].Values[colVal.(SupportedValueType)][uint64(rowIndex)] = true
	}
	// fmt.Println(dt)
	return nil
}

// GetColumn gets a column from datatable dt
func (dt *DataTable) GetColumn(colName string) (*ColumnInfoMap, error) {

	dt.l.RLock() // reader lock on datatable
	defer dt.l.RUnlock()

	column, found := dt.columnsMap[colName]

	if !found {
		return nil, errors.New("no column there")
	}
	return &column, nil

}

// GetRow gets a row from datatable dt
func (dt *DataTable) GetRow(rowIndex uint64) (Row, error) {
	dt.l.RLock() // reader lock on datatable
	defer dt.l.RUnlock()

	if int(rowIndex) >= len(dt.rows) {
		return nil, errors.New("no row")
	} else if _, found := dt.deletedRows[rowIndex]; found {
		return nil, errors.New("row already deleted")
	}
	res := make(Row, 0, len(dt.rows[rowIndex]))
	for i, colName := range dt.columnNames {
		if colName == "" {
			continue
		} else {
			row := dt.rows[rowIndex][i]
			//TODO(lgong) typing here
			rowcopy := SupportedValueTypeImpl{row.GetName(), row.GetValue()}
			res = append(res, rowcopy)
		}
	}
	return res, nil
}

// GetAllColumnNames reeturns an array string of all column names in the datatable
func (dt *DataTable) GetAllColumnNames() []string {
	dt.l.RLock()
	defer dt.l.RUnlock()

	res := []string{}
	for _, colName := range dt.columnNames {
		if colName == "" {
			continue
		}
		res = append(res, colName)
	}
	return res
}

// TODO: this doesn't work because of weird typing things
// GetColumnType returns the type of the column specified
// func (dt DataTable) GetColumnType(colName string) (SupportedValueType, error) {
// 	dt.l.RLock() // reader lock on datatable
// 	defer dt.l.RUnlock()

// 	column, found := dt.columnsMap[colName]

// 	if !found {
// 		return nil, errors.New("no column there")
// 	}
// 	return column.Type, nil
// }
