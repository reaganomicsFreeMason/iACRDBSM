package main

import (
	"iACRDBSM/server/types"
)

func main() {
	testDB = types.NewDataBase()
	newTableColNames := []string{"name", "quant"}
	newTableColTypes := []string{"Supported-Value-Type.string", "Supported-Value-Type.int"}
	testDB.newTable
}
