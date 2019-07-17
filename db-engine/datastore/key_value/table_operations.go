package key_value

import (
	"errors"
)

var (
	initialInfoValueSize = 10
	initNumCols          = 10
	initNumRows          = 10
	initDefaultSize      = 10
)

func (db *DataBase) NewTable(
	tableName string,
	columnNames,
	columnTypes []string,
) error {
	if _, found := (*db)[tableName]; found {
		return errors.New("Db already exists, dumbass")
	} else {
		dataTable, err := makeDataTable(columnNames, columnTypes)
		if err != nil {
			return errors.New("Table not made successfully")
		} else {
			(*db)[tableName] = dataTable
			return nil
		}
	}
}

func makeDataTable(columnNames, columnTypes []string) (*DataTable, error) {
	columnMap := make(map[string]ColumnInfoMap, initNumCols)

	// initialize map
	for i, name := range columnNames {
		typeName := columnTypes[i]
		if name == "" || typeName == "" {
			return nil, errors.New("Fucktard user put in an empty column. SAD!")
		}
		// var columnInfoValue map[SupportedValueType]ValueToRowMap
		// // makeNewColumnInfoMap(typeName, &columnInfoValue)
		// switch typeName {
		// case "Supported-Value-Type.int":
		// 	// var columnInfoValue map[SupportedInt]ValueToRowMap
		// 	// columnInfoValue = make(map[SupportedInt]ValueToRowMap, initialInfoValueSize)
		// 	columnMap[name] = ColumnInfoMap{
		// 		i,
		// 		make(map[SupportedInt]ValueToRowMap, initialInfoValueSize),
		// 	}
		// case "Supported-Value-Type.float":
		// 	var columnInfoValue map[SupportedFloat]ValueToRowMap
		// 	columnInfoValue = make(map[SupportedFloat]ValueToRowMap, initialInfoValueSize)
		// case "Supported-Value-Type.string":
		// 	var columnInfoValue map[SupportedString]ValueToRowMap
		// 	columnInfoValue = make(map[SupportedString]ValueToRowMap, initialInfoValueSize)
		// default:
		// 	columnInfoValue = make(map[SupportedValueType]ValueToRowMap, initialInfoValueSize)
		// }
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
	}, nil // TODO error messages and shit
}

// func makeNewColumnInfoMap(
// 	typeName string,
// 	columnInfoValue *map[SupportedValueType]ValueToRowMap,
// ) {
// 	switch typeName {
// 	case "Supported-Value-Type.int":
// 		*columnInfoValue = make(map[SupportedInt]ValueToRowMap, initialInfoValueSize)
// 	case "Supported-Value-Type.float":
// 		*columnInfoValue = make(map[SupportedFloat]ValueToRowMap, initialInfoValueSize)
// 	case "Supported-Value-Type.string":
// 		*columnInfoValue = make(map[SupportedString]ValueToRowMap, initialInfoValueSize)
// 	default:
// 		*columnInfoValue = make(map[SupportedValueType]ValueToRowMap, initialInfoValueSize)
// 	}
// }

func (db *DataBase) DeleteTable(tableName string) error {
	if _, err := db.GetTable(tableName); err != nil {
		return errors.New("Table not here, dumbass.")
	}
	delete(*db, tableName)
	return nil
}

func (db *DataBase) GetTable(tableName string) (*DataTable, error) {
	if table, found := (*db)[tableName]; !found {
		return nil, errors.New("Table not here, dumbass.")
	} else {
		return table, nil
	}
}

// TODO:L the batching question
func (dt *DataTable) PutColumn(columnName string, columnType string) error {
	// var columnInfoValue map[SupportedValueType]ValueToRowMap
	// switch columnType {
	// case "Supported-Value-Type.int":
	// 	var columnInfoValue map[SupportedInt]ValueToRowMap
	// 	columnInfoValue = make(map[SupportedInt]ValueToRowMap, initialInfoValueSize)
	// case "Supported-Value-Type.float":
	// 	var columnInfoValue map[SupportedFloat]ValueToRowMap
	// 	columnInfoValue = make(map[SupportedFloat]ValueToRowMap, initialInfoValueSize)
	// case "Supported-Value-Type.string":
	// 	var columnInfoValue map[SupportedString]ValueToRowMap
	// 	columnInfoValue = make(map[SupportedString]ValueToRowMap, initialInfoValueSize)
	// default:
	// 	columnInfoValue = make(map[SupportedValueType]ValueToRowMap, initialInfoValueSize)
	// }
	// dt.ColumnsMap[columnName] = ColumnInfoMap{
	// 	len(dt.ColumnNames),
	// 	columnInfoValue,
	// }
	if columnName == "" {
		return errors.New("Oops! This column has been deleted")
	}
	dt.ColumnsMap[columnName] = ColumnInfoMap{
		len(dt.ColumnNames),
		columnType,
		make(map[SupportedValueType]ValueToRowMap, initialInfoValueSize),
	}
	dt.ColumnNames = append(dt.ColumnNames, columnName)
	return nil
}

func (dt *DataTable) UpdateRow(rowIndex uint64, colName string, newValue SupportedValueType) error {
	if colName == "" {
		return errors.New("Oops! This column has been deleted")
	}
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

func (dt *DataTable) DeleteColumn(columnName string) error { // we should make sure this is not empty
	if columnName == "" {
		return errors.New("Oops! This column was already deleted")
	}
	if _, found := dt.ColumnsMap[columnName]; !found {
		return errors.New("No column there ")
	}
	columnInd := dt.ColumnsMap[columnName].Index
	delete(dt.ColumnsMap, columnName)
	dt.ColumnNames[columnInd] = "" // signifies that has been deleted
	return nil
}

func (dt *DataTable) DeleteRow(rowIndex uint64) error {
	if int(rowIndex) >= len(dt.Rows) {
		return errors.New("Row doesn't exist, libtard")
	} else if _, found := dt.DeletedRows[rowIndex]; found {
		return errors.New("Row already deleted, are you seriously this stupid?")
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

// assume we are appending to the end of the database
func (dt *DataTable) PutRow(row Row) error {
	// should prob do some sort of error checking
	// assume for now this row is valid at input
	// just assume that deleted columns are silly
	rowIndex := len(dt.Rows)
	dt.Rows = append(dt.Rows, row)
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
	return nil
}

// getColumn , get Row TODO
func (dt *DataTable) GetColumn(colName string) (*ColumnInfoMap, error) {
	if column, found := dt.ColumnsMap[colName]; !found {
		return nil, errors.New("No column there")
	} else {
		return &column, nil
	}
}

func (dt *DataTable) GetRow(rowIndex uint64) (Row, error) {
	if int(rowIndex) >= len(dt.Rows) {
		return nil, errors.New("No row there")
	} else if _, found := dt.DeletedRows[rowIndex]; found {
		return nil, errors.New("Boi y'all deleted this row ages ago.")
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

// TODO Delete column, test, add row, delete row
// TODO :: unmake the switch thing from function to inline; fucking annoying
