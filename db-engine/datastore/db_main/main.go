package main

// Main for testing datastore operations

import (
	types "iACRDBSM/db-engine/datastore/types"
)

func main() {
	testDB := types.NewDataBase()
	newTableColNames := []string{"name", "quant"}
	newTableColTypes := []string{"Supported-Value-Type.string", "Supported-Value-Type.int"}
	testDB.newTable
}
