package key_value_operations

import (
	"errors"

	"./types"
)

func (db *types.DataBase) newTable(tableName string) error {
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
	columnNames = make([]string, initNumCols)
	rows = make([]row, initNumRows)
	return &dataTable{
		columnMap,
		columnNames,
		rows,
	}
}

func (db *types.DataBase) deleteTable(tableName string) error {
	if _, err = db.getTable(tableName); err != nil {
		return errors.New("Table not here, dumbass.")
	} else {
		delete(*db, tableName)
		return nil
	}
}

func (db *types.DataBase) getTable(tableName string) (dataTable, error) {
	if table, found = (*db)[tableName]; !found {
		return nil, errors.New("Table not here, dumbass.")
	} else {
		return table, nil
	}
}
