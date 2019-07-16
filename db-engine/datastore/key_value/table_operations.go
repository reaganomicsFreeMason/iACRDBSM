package key_value

import (
	"errors"
)

var (
	initialInfoValueSize = 10
	initNumCols          = 10
	initNumRows          = 10
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
			map[SupportedValueType]ValueToRowMap{},
		}
	}
	rows := make([]Row, 0, initNumRows)
	return &DataTable{
		columnMap,
		columnNames,
		rows,
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
	} else {
		delete(*db, tableName)
		return nil
	}
}

func (db *DataBase) GetTable(tableName string) (*DataTable, error) {
	if table, found := (*db)[tableName]; !found {
		return nil, errors.New("Table not here, dumbass.")
	} else {
		return table, nil
	}
}

// TODO:L the batching question
func (dt *DataTable) PutColumn(columnName string, columnType string) {
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
	dt.ColumnsMap[columnName] = ColumnInfoMap{
		len(dt.ColumnNames),
		columnType,
		map[SupportedValueType]ValueToRowMap{},
	}
	dt.ColumnNames = append(dt.ColumnNames, columnName)
	dt.NumCol++
}

func (dt *DataTable) UpdateRow(rowIndex uint64, colName string, newValue SupportedValueType) error {
	if _, found := dt.ColumnsMap[colName]; !found {
		return errors.New("No column there ")
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
			dt.ColumnsMap[colName].Values[prevValue2][rowIndex] = false
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

// TODO Delete column, test, add row, delete row
// TODO :: unmake the switch thing from function to inline; fucking annoying
