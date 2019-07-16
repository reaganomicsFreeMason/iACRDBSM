package key_value_operations

import (
	"errors"

	. "./types"
)

var(
	initialInfoValueSize = 10
)

func (db *DataBase) newTable(
	tableName string,
	columnNames,
	columnTypes []string,
) error {
	if _, found = (*db)[tableName]; found {
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

func makeDataTable(columnNames, columnTypes []string) *dataTable {
	columnMap = make(map[string]columnInfoMap, initNumCols)

	// initialize map
	for i, name := range columnNames {
		typeName = columnTypes[i]
		var columnInfoValueConst map[supportedValueType]valueToRowMap
		switch typeName {
		case "Supported-Value-Type.int":
			columnInfoValueConst = make(map[SupportedInt]valueToRowMap, initialInfoValueSize)
		case "Supported-Value-Type.float":
			columnInfoValueConst = make(map[SupportedFloat]valueToRowMap, initialInfoValueSize)
		case "Supported-Value-Type.string":
			columnInfoValueConst = make(map[SupportedString]valueToRowMap, , initialInfoValueSize)
		default:
			columnInfoValueConst = make(map[supportedValueType]valueToRowMap, , initialInfoValueSize)
		}
		columnMap[name] = ColumnInfoMap{
			i,
			columnInfoValue,
		}
	}
	rows = make([]row, 0, initNumRows)
	return &dataTable{
		columnMap,
		columnNames,
		rows,
	}
}

func (db *DataBase) deleteTable(tableName string) error {
	if _, err = db.getTable(tableName); err != nil {
		return errors.New("Table not here, dumbass.")
	} else {
		delete(*db, tableName)
		return nil
	}
}

func (db *DataBase) getTable(tableName string) (dataTable, error) {
	if table, found = (*db)[tableName]; !found {
		return nil, errors.New("Table not here, dumbass.")
	} else {
		return table, nil
	}
}
