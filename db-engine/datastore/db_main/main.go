package main

// Main for testing datastore operations

import (
	"fmt"
	"iACRDBSM/db-engine/datastore/key_value"
)

func main() {
	testDB := key_value.NewDataBase()
	fmt.Println(testDB)
}
