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

	for _, row := range dt.rows {
		if columnInd == 0 {
			row = row[columnInd+1:]
		} else if columnInd == len(row) {
			row = row[:columnInd]
		} else {
			row = append(row[:columnInd], row[columnInd+1:]...)
		}
	}

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

	//TODO(lgong): bug need to deep copy
	copy := ColumnInfoMap{}
	copy.Index = column.Index
	copy.Type = column.Type
	copy.Values = make(map[SupportedValueType]ValueToRowMap)

	for key, value := range column.Values {
		valuecopy := ValueToRowMap{}
		for row := range value {
			valuecopy[row] = true
		}
		copy.Values[key] = valuecopy
	}

	return &copy, nil

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

//GetAllRowNames reeturns a 2D array of strings of all column names in the datatable
func (dt *DataTable) GetAllRowNames(goodIndices map[uint32]bool) string {
	dt.l.RLock()
	defer dt.l.RUnlock()

	ret := ""
	for i := range dt.rows {
		rowIndPointer := &i
		row, _ := dt.GetRow(uint64(*rowIndPointer))
		for i, elem := range row {
			if _, found := goodIndices[uint32(i)]; found {
				asValue := elem.(SupportedValueType)
				ret += " " + SupValToString(asValue) + " "
			}
		}
		ret += "\n" // new row
	}
	return ret
}

// GetNumCols Returns the number of rows
func (dt *DataTable) GetNumCols() int {
	dt.l.RLock()
	defer dt.l.RUnlock()

	return len(dt.columnsMap)
}

// GetNumRows Returns the number of rows
func (dt *DataTable) GetNumRows() int {
	dt.l.RLock()
	defer dt.l.RUnlock()

	return (len(dt.rows) - len(dt.deletedRows))
}

// GetNumRows Returns the number of rows
func (dt *DataTable) GetRows() []Row {

	return dt.rows
}

// GetOrderedColumn returns an array of supported value types of the column in order
func (dt *DataTable) GetOrderedColumn(colName string) ([]SupportedValueType, error) {

	dt.l.RLock() // reader lock on datatable
	defer dt.l.RUnlock()

	column, found := dt.columnsMap[colName]

	if !found {
		return nil, errors.New("no column there")
	}

	colIndex := column.Index

	returnCol := []SupportedValueType{}

	for _, row := range dt.rows {
		val := row[colIndex]
		copyVal := SupportedValueTypeImpl{Name: val.GetName(), Value: val.GetValue()}
		returnCol = append(returnCol, copyVal)
	}

	return returnCol, nil
}

// TODO: this doesn't work because of weird typing things
// GetColumnType returns the type of the column specified
func (dt *DataTable) GetColumnType(colName string) (string, error) {
	dt.l.RLock() // reader lock on datatable
	defer dt.l.RUnlock()

	column, found := dt.columnsMap[colName]

	if !found {
		return "", errors.New("no column there")
	}
	return column.Type, nil
}

// SetEmptyTable deletes all user made entries to the table, but maintains table name
func (database *DataBase) SetEmptyTable(tableName string) error {
	database.l.RLock() //reader lock database
	defer database.l.RLock()
	dt, exist := database.db[tableName]

	if !exist {
		return errors.New("database does not exist")
	}

	dt.l.Lock() // reader lock on datatable
	defer dt.l.Unlock()

	dt.deletedRows = IntegerSet{}
	dt.numCol = 0
	dt.rows = []Row{}

	for key, val := range dt.columnsMap {
		i := val.Index
		typeName := val.Type
		dt.columnsMap[key] = ColumnInfoMap{
			i,
			typeName,
			make(map[SupportedValueType]ValueToRowMap, initialInfoValueSize),
		}
	}

	return nil
}
