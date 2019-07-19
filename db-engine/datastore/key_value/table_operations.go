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

// TODO(lgong): condition variables on the locks to decrease busy waiting

// NewTable creates a new DataTable of name tableName in database if it does not exist already.
// The new DataTable will have columns columnNames of types columnTypes. There must be as many columnTypes as there are column names.
func (database *DataBase) NewTable(
	tableName string,
	columnNames,
	columnTypes []string,
) error {
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

	if found {
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

	dt.ColumnsMap[columnName] = ColumnInfoMap{
		len(dt.ColumnNames),
		columnType,
		make(map[SupportedValueType]ValueToRowMap, initialInfoValueSize),
	}
	dt.ColumnNames = append(dt.ColumnNames, columnName)
	return nil
}

// UpdateRow updates the entry at row at index rowIndex and column colName with a new value
func (dt *DataTable) UpdateRow(rowIndex uint64, colName string, newValue SupportedValueType) error {
	if colName == "" {
		return errors.New("can't update entry. column name is empty string")
	}

	dt.l.Lock() //writer lock on database
	defer dt.l.Unlock()

	if _, found := dt.ColumnsMap[colName]; !found {
		return errors.New("No column there;  ")
	}
	if _, found := dt.DeletedRows[rowIndex]; found {
		return errors.New("Row was deleted  ")
	}
	colIndex := dt.ColumnsMap[colName].Index
	rowLen := len(dt.Rows[rowIndex])
	var prevValue interface{}
	for i := rowLen; i < colIndex; i++ {
		dt.Rows[rowIndex] = append(dt.Rows[rowIndex], nil)
	}
	if colIndex >= len(dt.Rows[rowIndex]) {
		dt.Rows[rowIndex] = append(dt.Rows[rowIndex], nil)
	} else {
		// get and remove the old value in the case that it exists
		prevValue = dt.Rows[rowIndex][colIndex]
		if prevValue != nil {
			prevValue2 := prevValue.(SupportedValueType)
			delete(dt.ColumnsMap[colName].Values[prevValue2], rowIndex)
			if len(dt.ColumnsMap[colName].Values[prevValue2]) == 0 {
				delete(dt.ColumnsMap[colName].Values, prevValue2)
			}
		}
	}

	// then update the actual column; TODO type checking elsewhere or here?
	dt.Rows[rowIndex][colIndex] = newValue

	// now update corresponding maps
	_, found := dt.ColumnsMap[colName].Values[newValue]
	if !found {
		dt.ColumnsMap[colName].Values[newValue] = ValueToRowMap{}
	}
	dt.ColumnsMap[colName].Values[newValue][rowIndex] = true
	return nil
}

// DeleteColumn deletes column columnName
func (dt *DataTable) DeleteColumn(columnName string) error { // we should make sure this is not empty
	if columnName == "" {
		return errors.New("cannot delete column. provided column name is empty string")
	}

	dt.l.Lock() //writer lock on datatable
	defer dt.l.Unlock()

	if _, found := dt.ColumnsMap[columnName]; !found {
		return errors.New("No column there ")
	}
	columnInd := dt.ColumnsMap[columnName].Index
	delete(dt.ColumnsMap, columnName)
	dt.ColumnNames[columnInd] = "" // signifies that has been deleted
	return nil
}

//DeleteRow deletes row at index rowIndex
func (dt *DataTable) DeleteRow(rowIndex uint64) error {
	dt.l.Lock() //writer lock on datatable
	defer dt.l.Unlock()

	if int(rowIndex) >= len(dt.Rows) {
		return errors.New("Row doesn't exist")
	} else if _, found := dt.DeletedRows[rowIndex]; found {
		return errors.New("Row already deleted")
	}
	dt.DeletedRows[rowIndex] = true
	for ind, colValue := range dt.Rows[rowIndex] {
		colName := dt.ColumnNames[ind]
		if colValue == nil {
			continue
		}
		delete(dt.ColumnsMap[colName].Values[colValue.(SupportedValueType)], rowIndex)
		if len(dt.ColumnsMap[colName].Values[colValue.(SupportedValueType)]) == 0 {
			delete(dt.ColumnsMap[colName].Values, colValue.(SupportedValueType))
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
	rowIndex := len(dt.Rows)
	dt.Rows = append(dt.Rows, row)
	dt.Rows[rowIndex] = row
	for ind, colVal := range row {
		if colVal == nil {
			continue
		}
		colName := dt.ColumnNames[ind]
		if colName == "" {
			continue
		}
		// TODO: break up and make temp variables.
		if _, found := dt.ColumnsMap[colName].Values[colVal.(SupportedValueType)]; !found {
			dt.ColumnsMap[colName].Values[colVal.(SupportedValueType)] = ValueToRowMap{}
		}
		dt.ColumnsMap[colName].Values[colVal.(SupportedValueType)][uint64(rowIndex)] = true
	}
	// fmt.Println(dt)
	return nil
}

// GetColumn gets a column from datatable dt
func (dt *DataTable) GetColumn(colName string) (*ColumnInfoMap, error) {

	dt.l.RLock() // reader lock on datatable
	defer dt.l.RUnlock()

	if column, found := dt.ColumnsMap[colName]; !found {
		return nil, errors.New("no column there")
	} else {
		return &column, nil
	}
}

// GetRow gets a column from datatable dt
func (dt *DataTable) GetRow(rowIndex uint64) (Row, error) {
	dt.l.RLock() // reader lock on datatable
	defer dt.l.RUnlock()

	if int(rowIndex) >= len(dt.Rows) {
		return nil, errors.New("no row")
	} else if _, found := dt.DeletedRows[rowIndex]; found {
		return nil, errors.New("row already deleted")
	}
	res := make(Row, 0, len(dt.Rows[rowIndex]))
	for i, colName := range dt.ColumnNames {
		if colName == "" {
			continue
		} else {
			res = append(res, dt.Rows[rowIndex][i])
		}
	}
	return res, nil
}
