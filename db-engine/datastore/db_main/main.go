package main

// Main for testing datastore operations

import (
	"fmt"
	"iACRDBSM/db-engine/datastore/key_value"
)

func main() {
	testDB := key_value.NewDataBase()
	fmt.Println(testDB)
	testDB.NewTable(
		"LongLiveSanjit",
		[]string{"Sanjit1", "Sanjit2", "Sanjit3"}, 
		[]string{"Supported-Value-Type.int", "Supported-Value-Type.float", "Supported-Value-Type.string",} 
	)
}
